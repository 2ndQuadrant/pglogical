#include "postgres.h"

#include "access/xact.h"

#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"

#ifdef HAVE_REPLICATION_ORIGINS
#include "replication/origin.h"
#endif

#include "parser/parse_func.h"

#include "utils/acl.h"
#include "utils/lsyscache.h"

#include "miscadmin.h"

#include "pglogical_hooks.h"
#include "pglogical_output.h"

/*
 * Returns Oid of the hooks function specified in funcname.
 *
 * Error is thrown if function doesn't exist or doen't return correct datatype
 * or is volatile.
 */
static Oid
get_hooks_function_oid(List *funcname)
{
	Oid			funcid;
	Oid			funcargtypes[1];

	funcargtypes[0] = INTERNALOID;

	/* find the the function */
	funcid = LookupFuncName(funcname, 1, funcargtypes, false);

	/* Validate that the function returns void */
	if (get_func_rettype(funcid) != VOIDOID)
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("function %s must return void",
						NameListToString(funcname))));
	}

	if (func_volatile(funcid) == PROVOLATILE_VOLATILE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("function %s must not be VOLATILE",
						NameListToString(funcname))));
	}

	if (pg_proc_aclcheck(funcid, GetUserId(), ACL_EXECUTE) != ACLCHECK_OK)
	{
		const char * username;
#if PG_VERSION_NUM >= 90500
		username = GetUserNameFromId(GetUserId(), false);
#else
		username = GetUserNameFromId(GetUserId());
#endif
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("current user %s does not have permission to call function %s",
					 username, NameListToString(funcname))));
	}

	return funcid;
}

/*
 * If a hook setup function was specified in the startup parameters, look it up
 * in the catalogs, check permissions, call it, and store the resulting hook
 * info struct.
 */
void
load_hooks(PGLogicalOutputData *data)
{
	Oid hooks_func;
	MemoryContext old_ctxt;
	bool txn_started = false;

	if (!IsTransactionState())
	{
		txn_started = true;
		StartTransactionCommand();
	}

	if (data->hooks_setup_funcname != NIL)
	{
		hooks_func = get_hooks_function_oid(data->hooks_setup_funcname);

		old_ctxt = MemoryContextSwitchTo(data->hooks_mctxt);
		(void) OidFunctionCall1(hooks_func, PointerGetDatum(&data->hooks));
		MemoryContextSwitchTo(old_ctxt);

		elog(DEBUG3, "pglogical_output: Loaded hooks from function %u. Hooks are: \n"
				"\tstartup_hook: %p\n"
				"\tshutdown_hook: %p\n"
				"\trow_filter_hook: %p\n"
				"\ttxn_filter_hook: %p\n"
				"\thooks_private_data: %p\n",
				hooks_func,
				data->hooks.startup_hook,
				data->hooks.shutdown_hook,
				data->hooks.row_filter_hook,
				data->hooks.txn_filter_hook,
				data->hooks.hooks_private_data);
	}

	if (txn_started)
		CommitTransactionCommand();
}

void
call_startup_hook(PGLogicalOutputData *data, List *plugin_params)
{
	struct PGLogicalStartupHookArgs args;
	MemoryContext old_ctxt;

	if (data->hooks.startup_hook != NULL)
	{
		bool tx_started = false;

		args.private_data = data->hooks.hooks_private_data;
		args.in_params = plugin_params;
		args.out_params = NIL;

		elog(DEBUG3, "calling pglogical startup hook");

		if (!IsTransactionState())
		{
			tx_started = true;
			StartTransactionCommand();
		}

		old_ctxt = MemoryContextSwitchTo(data->hooks_mctxt);
		(void) (*data->hooks.startup_hook)(&args);
		MemoryContextSwitchTo(old_ctxt);

		if (tx_started)
			CommitTransactionCommand();

		data->extra_startup_params = args.out_params;
		/* The startup hook might change the private data seg */
		data->hooks.hooks_private_data = args.private_data;

		elog(DEBUG3, "called pglogical startup hook");
	}
}

void
call_shutdown_hook(PGLogicalOutputData *data)
{
	struct PGLogicalShutdownHookArgs args;
	MemoryContext old_ctxt;

	if (data->hooks.shutdown_hook != NULL)
	{
		args.private_data = data->hooks.hooks_private_data;

		elog(DEBUG3, "calling pglogical shutdown hook");

		old_ctxt = MemoryContextSwitchTo(data->hooks_mctxt);
		(void) (*data->hooks.shutdown_hook)(&args);
		MemoryContextSwitchTo(old_ctxt);

		data->hooks.hooks_private_data = args.private_data;

		elog(DEBUG3, "called pglogical shutdown hook");
	}
}

/*
 * Decide if the individual change should be filtered out by
 * calling a client-provided hook.
 */
bool
call_row_filter_hook(PGLogicalOutputData *data, ReorderBufferTXN *txn,
		Relation rel, ReorderBufferChange *change)
{
	struct  PGLogicalRowFilterArgs hook_args;
	MemoryContext old_ctxt;
	bool ret = true;

	if (data->hooks.row_filter_hook != NULL)
	{
		hook_args.change_type = change->action;
		hook_args.private_data = data->hooks.hooks_private_data;
		hook_args.changed_rel = rel;

		elog(DEBUG3, "calling pglogical row filter hook");

		old_ctxt = MemoryContextSwitchTo(data->hooks_mctxt);
		ret = (*data->hooks.row_filter_hook)(&hook_args);
		MemoryContextSwitchTo(old_ctxt);

		/* Filter hooks shouldn't change the private data ptr */
		Assert(data->hooks.hooks_private_data == hook_args.private_data);

		elog(DEBUG3, "called pglogical row filter hook, returned %d", (int)ret);
	}

	return ret;
}

bool
call_txn_filter_hook(PGLogicalOutputData *data, RepOriginId txn_origin)
{
	struct PGLogicalTxnFilterArgs hook_args;
	bool ret = true;
	MemoryContext old_ctxt;

	if (data->hooks.txn_filter_hook != NULL)
	{
		hook_args.private_data = data->hooks.hooks_private_data;
		hook_args.origin_id = txn_origin;

		elog(DEBUG3, "calling pglogical txn filter hook");

		old_ctxt = MemoryContextSwitchTo(data->hooks_mctxt);
		ret = (*data->hooks.txn_filter_hook)(&hook_args);
		MemoryContextSwitchTo(old_ctxt);

		/* Filter hooks shouldn't change the private data ptr */
		Assert(data->hooks.hooks_private_data == hook_args.private_data);

		elog(DEBUG3, "called pglogical txn filter hook, returned %d", (int)ret);
	}

	return ret;
}
