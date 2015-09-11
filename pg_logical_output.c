/*-------------------------------------------------------------------------
 *
 * pg_logical_output.c
 *		  Logical Replication output plugin
 *
 * Copyright (c) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pg_logical_output.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_logical_compat.h"
#include "pg_logical_output.h"
#include "pg_logical_proto.h"

#include "access/sysattr.h"

#include "catalog/pg_class.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"

#include "mb/pg_wchar.h"

#include "nodes/parsenodes.h"

#include "parser/parse_func.h"

#include "replication/output_plugin.h"
#include "replication/logical.h"
#ifdef HAVE_REPLICATION_ORIGINS
#include "replication/origin.h"
#endif

#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

PG_MODULE_MAGIC;

extern void		_PG_output_plugin_init(OutputPluginCallbacks *cb);

typedef enum PGLogicalOutputParamType
{
	OUTPUT_PARAM_TYPE_BOOL,
	OUTPUT_PARAM_TYPE_UINT32,
	OUTPUT_PARAM_TYPE_STRING,
	OUTPUT_PARAM_TYPE_QUALIFIED_NAME
} PGLogicalOutputParamType;

/* These must be available to pg_dlsym() */
static void pg_decode_startup(LogicalDecodingContext * ctx,
							  OutputPluginOptions *opt, bool is_init);
//static void pg_decode_shutdown(LogicalDecodingContext * ctx);
static void pg_decode_begin_txn(LogicalDecodingContext *ctx,
					ReorderBufferTXN *txn);
static void pg_decode_commit_txn(LogicalDecodingContext *ctx,
					 ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void pg_decode_change(LogicalDecodingContext *ctx,
				 ReorderBufferTXN *txn, Relation rel,
				 ReorderBufferChange *change);

#ifdef HAVE_REPLICATION_ORIGINS
static bool pg_decode_origin_filter(LogicalDecodingContext *ctx,
						RepOriginId origin_id);
#endif

/* hooks */
static Oid get_table_filter_function_id(List *funcname, bool validate);
static void hook_cache_callback(Datum arg, int cacheid, uint32 hashvalue);
static inline bool pg_decode_change_filter(PGLogicalOutputData *data,
						Relation rel,
						enum ReorderBufferChangeType change);

/* param parsing */
static Datum get_param(List *options, const char *name, bool missing_ok,
					   bool null_ok, PGLogicalOutputParamType type,
					   bool *found);
static bool parse_param_bool(DefElem *elem);
static uint32 parse_param_uint32(DefElem *elem);
static bool server_float4_byval(void);
static bool server_float8_byval(void);
static bool server_integer_datetimes(void);
static bool server_bigendian(void);


/* specify output plugin callbacks */
void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	cb->startup_cb = pg_decode_startup;
	cb->begin_cb = pg_decode_begin_txn;
	cb->change_cb = pg_decode_change;
	cb->commit_cb = pg_decode_commit_txn;
#ifdef HAVE_REPLICATION_ORIGINS
	cb->filter_by_origin_cb = pg_decode_origin_filter;
#endif
	cb->shutdown_cb = NULL;
}

static bool
check_binary_compatibility(List *options)
{
	bool	found;
	Datum	val;

	val = get_param(options, "binary.bigendian", false, false,
					OUTPUT_PARAM_TYPE_BOOL, &found);
	if (DatumGetBool(val) != server_bigendian())
		return false;

	val = get_param(options, "binary.sizeof_datum", false, false,
					OUTPUT_PARAM_TYPE_UINT32, &found);
	if (DatumGetUInt32(val) != sizeof(Datum))
		return false;

	val = get_param(options, "binary.sizeof_int", false, false,
					OUTPUT_PARAM_TYPE_UINT32, &found);
	if (DatumGetUInt32(val) != sizeof(int))
		return false;

	val = get_param(options, "binary.sizeof_long", false, false,
					OUTPUT_PARAM_TYPE_UINT32, &found);
	if (DatumGetUInt32(val) != sizeof(long))
		return false;

	val = get_param(options, "binary.float4_byval", false, false,
					OUTPUT_PARAM_TYPE_BOOL, &found);
	if (DatumGetBool(val) != server_float4_byval())
		return false;

	val = get_param(options, "binary.float8_byval", false, false,
					OUTPUT_PARAM_TYPE_BOOL, &found);
	if (DatumGetBool(val) != server_float8_byval())
		return false;

	val = get_param(options, "binary.integer_datetimes", false, false,
					OUTPUT_PARAM_TYPE_BOOL, &found);
	if (DatumGetBool(val) != server_integer_datetimes())
		return false;

	return true;
}

/* initialize this plugin */
static void
pg_decode_startup(LogicalDecodingContext * ctx, OutputPluginOptions *opt,
				  bool is_init)
{
	PGLogicalOutputData  *data;

	data = palloc0(sizeof(PGLogicalOutputData));
	data->context = AllocSetContextCreate(TopMemoryContext,
										  "pg_logical conversion context",
										  ALLOCSET_DEFAULT_MINSIZE,
										  ALLOCSET_DEFAULT_INITSIZE,
										  ALLOCSET_DEFAULT_MAXSIZE);

	ctx->output_plugin_private = data;

	opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

	/*
	 * This is replication start and not slot initialization.
	 *
	 * Parse and validate options passed by the client.
	 */
	if (!is_init)
	{
		bool	found;
		Datum	val;
		bool	requirenodeid = false;

		/* check for encoding match */
		val = get_param(ctx->output_plugin_options, "client_encoding", false,
						false, OUTPUT_PARAM_TYPE_STRING, &found);
		data->client_encoding = DatumGetCString(val);
		if (strcmp(data->client_encoding, GetDatabaseEncodingName()) != 0)
			elog(ERROR, "only \"%s\" encoding is supported by this server",
				 GetDatabaseEncodingName());

		/*
		 * Check PostgreSQL version, this can be omitted to support clients
		 * other than PostgreSQL.
		 */
		val = get_param(ctx->output_plugin_options, "pg_version", true, false,
						OUTPUT_PARAM_TYPE_UINT32, &found);
		data->client_pg_version = found ? DatumGetUInt32(val) : 0;

		val = get_param(ctx->output_plugin_options, "pg_catversion", true,
						false, OUTPUT_PARAM_TYPE_UINT32, &found);
		data->client_pg_catversion = found ? DatumGetUInt32(val) : 0;

		/*
		 * Check to see if the client asked for changeset forwarding
		 *
		 * Note that we cannot support this on 9.4. We'll tell the client
		 * in the startup reply message.
		 */
		val = get_param(ctx->output_plugin_options, "Forward_changesets", true,
						false, OUTPUT_PARAM_TYPE_BOOL, &found);

		data->forward_changesets = found ? DatumGetBool(val) : false;

		/* check if we want to use binary data representation */
		val = get_param(ctx->output_plugin_options, "want_binary", true,
						false, OUTPUT_PARAM_TYPE_BOOL, &found);

		if (found && DatumGetBool(val) &&
			data->client_pg_version / 100 == PG_VERSION_NUM / 100)
			data->allow_binary_protocol =
				check_binary_compatibility(ctx->output_plugin_options);
		else
			data->allow_binary_protocol = false;

		/* check if we want to use sendrecv data representation */
		val = get_param(ctx->output_plugin_options, "want_sendrecv", true,
						false, OUTPUT_PARAM_TYPE_BOOL, &found);

		if (found && DatumGetBool(val) &&
			data->client_pg_version / 100 == PG_VERSION_NUM / 100)
			data->allow_sendrecv_protocol = true;
		else
			data->allow_sendrecv_protocol = false;

		/* Hooks */
		val = get_param(ctx->output_plugin_options,
						"hooks.table_change_filter", true, false,
						OUTPUT_PARAM_TYPE_QUALIFIED_NAME, &found);

		if (found)
		{
			List   *funcname = (List *) PointerGetDatum(val);

			data->table_change_filter = funcname;
			/* Validate the function but don't store the Oid */
			(void) get_table_filter_function_id(funcname, true);

			/* We need to properly invalidate the hooks */
			CacheRegisterSyscacheCallback(PROCOID, hook_cache_callback, PointerGetDatum(data));

			/* Node id is required parameter if there is hook which needs it */
			requirenodeid = true;
		}

		/* Node id */
		val = get_param(ctx->output_plugin_options, "node_id", !requirenodeid,
						false, OUTPUT_PARAM_TYPE_STRING, &found);

		if (found)
			data->node_id = DatumGetCString(val);
	}
}

/*
 * BEGIN callback
 */
void
pg_decode_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	bool send_replication_origin = false;

#ifdef HAVE_REPLICATION_ORIGINS
	send_replication_origin = txn->origin_id != InvalidRepOriginId;

#endif
	OutputPluginPrepareWrite(ctx, !send_replication_origin);
	pg_logical_write_begin(ctx->out, txn);

#ifdef HAVE_REPLICATION_ORIGINS
	if (send_replication_origin)
	{
		char *origin;

		/* Message boundary */
		OutputPluginWrite(ctx, false);
		OutputPluginPrepareWrite(ctx, true);

		/*
		 * XXX: which behaviour we want here?
		 *
		 * Alternatives:
		 *  - don't send origin message if origin name not found
		 *    (that's what we do now)
		 *  - throw error - that will break replication, not good
		 *  - send some special "unknown" origin
		 */
		if (replorigin_by_oid(txn->origin_id, true, &origin))
			pg_logical_write_origin(ctx->out, origin, txn->origin_lsn);
	}
#endif

	OutputPluginWrite(ctx, true);
}

/*
 * COMMIT callback
 */
void
pg_decode_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
					 XLogRecPtr commit_lsn)
{
	OutputPluginPrepareWrite(ctx, true);
	pg_logical_write_commit(ctx->out, txn, commit_lsn);
	OutputPluginWrite(ctx, true);
}

void
pg_decode_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				 Relation relation, ReorderBufferChange *change)
{
	PGLogicalOutputData *data;
	MemoryContext old;

	data = ctx->output_plugin_private;

	/* First checkk filter */
	if (pg_decode_change_filter(data, relation, change->action))
		return;

	/* Avoid leaking memory by using and resetting our own context */
	old = MemoryContextSwitchTo(data->context);

	/* TODO: add caching (send only if changed) */
	OutputPluginPrepareWrite(ctx, false);
	pg_logical_write_rel(ctx->out, relation);
	OutputPluginWrite(ctx, false);

	/* Send the data */
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			OutputPluginPrepareWrite(ctx, true);
			pg_logical_write_insert(ctx->out, data, relation,
									&change->data.tp.newtuple->tuple);
			OutputPluginWrite(ctx, true);
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			{
				HeapTuple oldtuple = change->data.tp.oldtuple ?
					&change->data.tp.oldtuple->tuple : NULL;

				OutputPluginPrepareWrite(ctx, true);
				pg_logical_write_update(ctx->out, data, relation, oldtuple,
										&change->data.tp.newtuple->tuple);
				OutputPluginWrite(ctx, true);
				break;
			}
		case REORDER_BUFFER_CHANGE_DELETE:
			if (change->data.tp.oldtuple)
			{
				OutputPluginPrepareWrite(ctx, true);
				pg_logical_write_delete(ctx->out, data, relation,
										&change->data.tp.oldtuple->tuple);
				OutputPluginWrite(ctx, true);
			}
			else
				elog(DEBUG1, "didn't send DELETE change because of missing oldtuple");
			break;
		default:
			Assert(false);
	}

	/* Cleanup */
	MemoryContextSwitchTo(old);
	MemoryContextReset(data->context);
}


#ifdef HAVE_REPLICATION_ORIGINS
/*
 * Decide if the whole transaction with specific origin should be filtered out.
 */
static bool
pg_decode_origin_filter(LogicalDecodingContext *ctx,
						RepOriginId origin_id)
{
	PGLogicalOutputData *data = ctx->output_plugin_private;

	if (!data->forward_changesets && origin_id != InvalidRepOriginId)
		return true;

	return false;

}
#endif

/*
 * Decide if the invidual change should be filtered out.
 */
static inline bool
pg_decode_change_filter(PGLogicalOutputData *data, Relation rel,
						enum ReorderBufferChangeType change)
{
	/* Call hook if provided. */
	if (data->table_change_filter)
	{
		Datum	res;
		char	change_type;

		if (!OidIsValid(data->table_change_filter_oid))
		{
			Oid		funcoid =
				get_table_filter_function_id(data->table_change_filter, false);

			/*
			 * Not found, this can be historical snapshot and we can't do
			 * validation one way or the other so we act as if the hook
			 * was not provided.
			 */
			if (!OidIsValid(funcoid))
				return false;

			/* Update the cache and continue */
			data->table_change_filter_oid = funcoid;
			data->table_change_filter_hash =
				GetSysCacheHashValue1(PROCOID, funcoid);
		}

		switch (change)
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
				elog(ERROR, "unknown change type %d", change);
				change_type = '0';	/* silence compiler */
		}

		res = OidFunctionCall3(data->table_change_filter_oid,
							   CStringGetTextDatum(data->node_id),
							   ObjectIdGetDatum(RelationGetRelid(rel)),
							   CharGetDatum(change_type));

		return DatumGetBool(res);
	}

	/* Default action is to always replicate the change, so don't filter. */
	return false;
}

/*
 * Hook oid cache invalidation.
 */
static void
hook_cache_callback(Datum arg, int cacheid, uint32 hashvalue)
{
	PGLogicalOutputData *data;

	Assert(cacheid == PROCOID);

	data = (PGLogicalOutputData *) DatumGetPointer(arg);

	if (hashvalue == data->table_change_filter_hash &&
		OidIsValid(data->table_change_filter_oid))
		data->table_change_filter_oid = InvalidOid;
}

/*
 * Returns Oid of the function specified in funcname.
 *
 * Error is thrown if validate is true and function doesn't exist or doen't
 * return correct datatype or is volatile. When validate is false InvalidOid
 * will be returned instead of error.
 *
 * TODO check ACL
 */
static Oid
get_table_filter_function_id(List *funcname, bool validate)
{
	Oid			funcid;
	Oid			funcargtypes[3];

	funcargtypes[0] = TEXTOID;	/* identifier of this node */
	funcargtypes[1] = OIDOID;	/* relation */
	funcargtypes[2] = CHAROID;	/* change type */

	/* find the the function */
	funcid = LookupFuncName(funcname, 3, funcargtypes, !validate);

	if (!OidIsValid(funcid))
	{
		if (validate)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("function %s not found",
							NameListToString(funcname))));
		else
			return InvalidOid;
	}

	/* Validate that the function returns boolean */
	if (get_func_rettype(funcid) != BOOLOID)
	{
		if (validate)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("function %s must return type \"boolean\"",
							NameListToString(funcname))));
		else
			return InvalidOid;
	}

	if (func_volatile(funcid) == PROVOLATILE_VOLATILE)
	{
		if (validate)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("function %s must not be VOLATILE",
							NameListToString(funcname))));
		else
			return InvalidOid;
	}

	return funcid;
}

/*
 * Param parsing
 *
 * This is not exactly fast but since it's only called on replication start
 * we'll leave it for now.
 */
static Datum
get_param(List *options, const char *name, bool missing_ok, bool null_ok,
		  PGLogicalOutputParamType type, bool *found)
{
	ListCell	   *option;

	*found = false;

	foreach(option, options)
	{
		DefElem    *elem = lfirst(option);

		Assert(elem->arg == NULL || IsA(elem->arg, String));

		/* Search until matching parameter found */
		if (pg_strcasecmp(name, elem->defname))
			continue;

		/* Check for NULL value */
		if (elem->arg == NULL || strVal(elem->arg) == NULL)
		{
			if (null_ok)
				return (Datum) 0;
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("parameter \"%s\" cannot be NULL", name)));
		}

		*found = true;

		switch (type)
		{
			case OUTPUT_PARAM_TYPE_UINT32:
				return UInt32GetDatum(parse_param_uint32(elem));
			case OUTPUT_PARAM_TYPE_BOOL:
				return BoolGetDatum(parse_param_bool(elem));
			case OUTPUT_PARAM_TYPE_STRING:
				return PointerGetDatum(pstrdup(strVal(elem->arg)));
			case OUTPUT_PARAM_TYPE_QUALIFIED_NAME:
				return PointerGetDatum(textToQualifiedNameList(cstring_to_text(pstrdup(strVal(elem->arg)))));
			default:
				elog(ERROR, "unknown parameter type %d", type);
		}
	}

	if (!missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("missing required parameter \"%s\"", name)));

	return (Datum) 0;
}

static bool
parse_param_bool(DefElem *elem)
{
	bool		res;

	if (!parse_bool(strVal(elem->arg), &res))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not parse boolean value \"%s\" for parameter \"%s\": %m",
						strVal(elem->arg), elem->defname)));

	return res;
}

static uint32
parse_param_uint32(DefElem *elem)
{
	int64		res;

	if (!scanint8(strVal(elem->arg), true, &res))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not parse integer value \"%s\" for parameter \"%s\": %m",
						strVal(elem->arg), elem->defname)));

	if (res > PG_UINT32_MAX || res < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("value \"%s\" out of range for parameter \"%s\": %m",
						strVal(elem->arg), elem->defname)));

	return (uint32) res;
}


static bool
server_float4_byval(void)
{
#ifdef USE_FLOAT4_BYVAL
	return true;
#else
	return false;
#endif
}

static bool
server_float8_byval(void)
{
#ifdef USE_FLOAT8_BYVAL
	return true;
#else
	return false;
#endif
}

static bool
server_integer_datetimes(void)
{
#ifdef USE_INTEGER_DATETIMES
	return true;
#else
	return false;
#endif
}

static bool
server_bigendian(void)
{
#ifdef WORDS_BIGENDIAN
	return true;
#else
	return false;
#endif
}
