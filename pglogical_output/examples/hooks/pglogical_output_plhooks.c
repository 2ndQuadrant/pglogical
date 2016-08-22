#include "postgres.h"

#include "pglogical_output/hooks.h"

#include "access/xact.h"

#include "catalog/pg_type.h"

#include "nodes/makefuncs.h"

#include "parser/parse_func.h"

#include "replication/reorderbuffer.h"

#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "fmgr.h"
#include "miscadmin.h"

PG_MODULE_MAGIC;

PGDLLEXPORT extern Datum pglo_plhooks_setup_fn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pglo_plhooks_setup_fn);

void pglo_plhooks_startup(struct PGLogicalStartupHookArgs *startup_args);
void pglo_plhooks_shutdown(struct PGLogicalShutdownHookArgs *shutdown_args);
bool pglo_plhooks_row_filter(struct PGLogicalRowFilterArgs *rowfilter_args);
bool pglo_plhooks_txn_filter(struct PGLogicalTxnFilterArgs *txnfilter_args);

typedef struct PLHPrivate
{
	const char *client_arg;
	Oid startup_hook;
	Oid shutdown_hook;
	Oid row_filter_hook;
	Oid txn_filter_hook;
	MemoryContext hook_call_context;
} PLHPrivate;

static void read_parameters(PLHPrivate *private, List *in_params);
static Oid find_startup_hook(const char *proname);
static Oid find_shutdown_hook(const char *proname);
static Oid find_row_filter_hook(const char *proname);
static Oid find_txn_filter_hook(const char *proname);
static void exec_user_startup_hook(PLHPrivate *private, List *in_params, List **out_params);

void
pglo_plhooks_startup(struct PGLogicalStartupHookArgs *startup_args)
{
	PLHPrivate *private;

	/* pglogical_output promises to call us in a tx */
	Assert(IsTransactionState());

	/* Allocated in hook memory context, scoped to the logical decoding session: */
	startup_args->private_data = private = (PLHPrivate*)palloc(sizeof(PLHPrivate));

	private->startup_hook = InvalidOid;
	private->shutdown_hook = InvalidOid;
	private->row_filter_hook = InvalidOid;
	private->txn_filter_hook = InvalidOid;
	/* client_arg is the empty string when not specified to simplify function calls */
	private->client_arg = "";

	read_parameters(private, startup_args->in_params);

	private->hook_call_context = AllocSetContextCreate(CurrentMemoryContext,
			                    "pglogical_output plhooks hook call context",
			                    ALLOCSET_SMALL_MINSIZE,
			                    ALLOCSET_SMALL_INITSIZE,
			                    ALLOCSET_SMALL_MAXSIZE);


	if (private->startup_hook != InvalidOid)
		exec_user_startup_hook(private, startup_args->in_params, &startup_args->out_params);
}

void
pglo_plhooks_shutdown(struct PGLogicalShutdownHookArgs *shutdown_args)
{
	PLHPrivate *private = (PLHPrivate*)shutdown_args->private_data;
	MemoryContext old_ctx;

	Assert(private != NULL);

	if (OidIsValid(private->shutdown_hook))
	{
		old_ctx = MemoryContextSwitchTo(private->hook_call_context);
		elog(DEBUG3, "calling pglo shutdown hook with %s", private->client_arg);
		(void) OidFunctionCall1(
				private->shutdown_hook,
				CStringGetTextDatum(private->client_arg));
		elog(DEBUG3, "called pglo shutdown hook");
		MemoryContextSwitchTo(old_ctx);
		MemoryContextReset(private->hook_call_context);
	}
}

bool
pglo_plhooks_row_filter(struct PGLogicalRowFilterArgs *rowfilter_args)
{
	PLHPrivate *private = (PLHPrivate*)rowfilter_args->private_data;
	bool ret = true;
	MemoryContext old_ctx;

	Assert(private != NULL);

	if (OidIsValid(private->row_filter_hook))
	{
		char change_type;
		switch (rowfilter_args->change_type)
		{
			case REORDER_BUFFER_CHANGE_INSERT:
				change_type = 'I';
				break;
			case REORDER_BUFFER_CHANGE_UPDATE:
				change_type = 'U';
				break;
			case REORDER_BUFFER_CHANGE_DELETE:
				change_type = 'D';
				break;
			default:
				elog(ERROR, "unknown change type %d", rowfilter_args->change_type);
				change_type = '0';	/* silence compiler */
		}

		old_ctx = MemoryContextSwitchTo(private->hook_call_context);
		elog(DEBUG3, "calling pglo row filter hook with (%u,%c,%s)",
				rowfilter_args->changed_rel->rd_id, change_type,
				private->client_arg);
		ret = DatumGetBool(OidFunctionCall3(
				private->row_filter_hook,
				ObjectIdGetDatum(rowfilter_args->changed_rel->rd_id),
				CharGetDatum(change_type),
				CStringGetTextDatum(private->client_arg)));
		elog(DEBUG3, "called pglo row filter hook, returns %d", (int)ret);
		MemoryContextSwitchTo(old_ctx);
		MemoryContextReset(private->hook_call_context);
	}

	return ret;
}

bool
pglo_plhooks_txn_filter(struct PGLogicalTxnFilterArgs *txnfilter_args)
{
	PLHPrivate *private = (PLHPrivate*)txnfilter_args->private_data;
	bool ret = true;
	MemoryContext old_ctx;

	Assert(private != NULL);


	if (OidIsValid(private->txn_filter_hook))
	{
		old_ctx = MemoryContextSwitchTo(private->hook_call_context);

		elog(DEBUG3, "calling pglo txn filter hook with (%hu,%s)",
				txnfilter_args->origin_id, private->client_arg);
		ret = DatumGetBool(OidFunctionCall2(
					private->txn_filter_hook,
					UInt16GetDatum(txnfilter_args->origin_id),
					CStringGetTextDatum(private->client_arg)));
		elog(DEBUG3, "calling pglo txn filter hook, returns %d", (int)ret);

		MemoryContextSwitchTo(old_ctx);
		MemoryContextReset(private->hook_call_context);
	}

	return ret;
}

Datum
pglo_plhooks_setup_fn(PG_FUNCTION_ARGS)
{
	struct PGLogicalHooks *hooks = (struct PGLogicalHooks*) PG_GETARG_POINTER(0);

	/* Your code doesn't need this, it's just for the tests: */
	Assert(hooks != NULL);
	Assert(hooks->hooks_private_data == NULL);
	Assert(hooks->startup_hook == NULL);
	Assert(hooks->shutdown_hook == NULL);
	Assert(hooks->row_filter_hook == NULL);
	Assert(hooks->txn_filter_hook == NULL);

	/*
	 * Just assign the hook pointers. We're not meant to do much
	 * work here.
	 *
	 * Note that private_data is left untouched, to be set up by the
	 * startup hook.
	 */
	hooks->startup_hook = pglo_plhooks_startup;
	hooks->shutdown_hook = pglo_plhooks_shutdown;
	hooks->row_filter_hook = pglo_plhooks_row_filter;
	hooks->txn_filter_hook = pglo_plhooks_txn_filter;
	elog(DEBUG3, "configured pglo hooks");

	PG_RETURN_VOID();
}

static void
exec_user_startup_hook(PLHPrivate *private, List *in_params, List **out_params)
{
		ArrayType *startup_params;
		Datum ret;
		ListCell *lc;
		Datum *startup_params_elems;
		bool  *startup_params_isnulls;
		int   n_startup_params;
		int   i;
		MemoryContext old_ctx;


		old_ctx = MemoryContextSwitchTo(private->hook_call_context);

		/*
		 * Build the input parameter array. NULL parameters are passed as the
		 * empty string for the sake of convenience. Each param is two
		 * elements, a key then a value element.
		 */
		n_startup_params = list_length(in_params) * 2;
		startup_params_elems = (Datum*)palloc0(sizeof(Datum)*n_startup_params);

		i = 0;
		foreach (lc, in_params)
		{
			DefElem * elem = (DefElem*)lfirst(lc);
			const char *val;

			if (elem->arg == NULL || strVal(elem->arg) == NULL)
				val = "";
			else
				val = strVal(elem->arg);

			startup_params_elems[i++] = CStringGetTextDatum(elem->defname);
			startup_params_elems[i++] = CStringGetTextDatum(val);
		}
		Assert(i == n_startup_params);

		startup_params = construct_array(startup_params_elems, n_startup_params,
				TEXTOID, -1, false, 'i');

		ret = OidFunctionCall2(
				private->startup_hook,
				PointerGetDatum(startup_params),
				CStringGetTextDatum(private->client_arg));

		/*
		 * deconstruct return array and add pairs of results to a DefElem list.
		 */
		deconstruct_array(DatumGetArrayTypeP(ret), TEXTARRAYOID,
				-1, false, 'i', &startup_params_elems, &startup_params_isnulls,
				&n_startup_params);


		*out_params = NIL;
		for (i = 0; i < n_startup_params; i = i + 2)
		{
			char *value;
			DefElem *elem;

			if (startup_params_isnulls[i])
				elog(ERROR, "Array entry corresponding to a key was null at idx=%d", i);

			if (startup_params_isnulls[i+1])
				value = "";
			else
				value = TextDatumGetCString(startup_params_elems[i+1]);

			elem = makeDefElem(
					TextDatumGetCString(startup_params_elems[i]),
					(Node*)makeString(value));

			*out_params = lcons(elem, *out_params);
		}

		MemoryContextSwitchTo(old_ctx);
		MemoryContextReset(private->hook_call_context);
}

static void
read_parameters(PLHPrivate *private, List *in_params)
{
	ListCell *option;

	foreach(option, in_params)
	{
		DefElem    *elem = lfirst(option);

		if (pg_strcasecmp("pglo_plhooks.client_hook_arg", elem->defname) == 0)
		{
			if (elem->arg == NULL || strVal(elem->arg) == NULL)
				elog(ERROR, "pglo_plhooks.client_hook_arg may not be NULL");
			private->client_arg = pstrdup(strVal(elem->arg));
		}

		if (pg_strcasecmp("pglo_plhooks.startup_hook", elem->defname) == 0)
		{
			if (elem->arg == NULL || strVal(elem->arg) == NULL)
				elog(ERROR, "pglo_plhooks.startup_hook may not be NULL");
			private->startup_hook = find_startup_hook(strVal(elem->arg));
		}

		if (pg_strcasecmp("pglo_plhooks.shutdown_hook", elem->defname) == 0)
		{
			if (elem->arg == NULL || strVal(elem->arg) == NULL)
				elog(ERROR, "pglo_plhooks.shutdown_hook may not be NULL");
			private->shutdown_hook = find_shutdown_hook(strVal(elem->arg));
		}

		if (pg_strcasecmp("pglo_plhooks.txn_filter_hook", elem->defname) == 0)
		{
			if (elem->arg == NULL || strVal(elem->arg) == NULL)
				elog(ERROR, "pglo_plhooks.txn_filter_hook may not be NULL");
			private->txn_filter_hook = find_txn_filter_hook(strVal(elem->arg));
		}

		if (pg_strcasecmp("pglo_plhooks.row_filter_hook", elem->defname) == 0)
		{
			if (elem->arg == NULL || strVal(elem->arg) == NULL)
				elog(ERROR, "pglo_plhooks.row_filter_hook may not be NULL");
			private->row_filter_hook = find_row_filter_hook(strVal(elem->arg));
		}
	}
}

static Oid
find_hook_fn(const char *funcname, Oid funcargtypes[], int nfuncargtypes, Oid returntype)
{
	Oid			funcid;
	List	   *qname;

	qname = stringToQualifiedNameList(funcname);

	/* find the the function */
	funcid = LookupFuncName(qname, nfuncargtypes, funcargtypes, false);

	/* Check expected return type */
	if (get_func_rettype(funcid) != returntype)
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("function %s doesn't return expected type %d",
						NameListToString(qname), returntype)));
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
					 username, NameListToString(qname))));
	}

	list_free_deep(qname);

	return funcid;
}

static Oid
find_startup_hook(const char *proname)
{
	Oid argtypes[2];

	argtypes[0] = TEXTARRAYOID;
	argtypes[1] = TEXTOID;

	return find_hook_fn(proname, argtypes, 2, VOIDOID);
}

static Oid
find_shutdown_hook(const char *proname)
{
	Oid argtypes[1];

	argtypes[0] = TEXTOID;

	return find_hook_fn(proname, argtypes, 1, VOIDOID);
}

static Oid
find_row_filter_hook(const char *proname)
{
	Oid argtypes[3];

	argtypes[0] = REGCLASSOID;
	argtypes[1] = CHAROID;
	argtypes[2] = TEXTOID;

	return find_hook_fn(proname, argtypes, 3, BOOLOID);
}

static Oid
find_txn_filter_hook(const char *proname)
{
	Oid argtypes[2];

	argtypes[0] = INT4OID;
	argtypes[1] = TEXTOID;

	return find_hook_fn(proname, argtypes, 2, BOOLOID);
}
