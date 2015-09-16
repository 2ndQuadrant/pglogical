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
check_binary_compatibility(PGLogicalOutputData *data)
{
	if (data->client_binary_basetypes_major_version != PG_VERSION_NUM / 100)
		return false;

	if (data->client_binary_bigendian_set
			&& data->client_binary_bigendian != server_bigendian())
	{
		elog(DEBUG1, "Binary mode rejected: Server and client endian mis-match");
		return false;
	}

	if (data->client_binary_sizeofdatum_set
			&& data->client_binary_sizeofdatum != sizeof(Datum))
	{
		elog(DEBUG1, "Binary mode rejected: Server and client endian sizeof(Datum) mismatch");
		return false;
	}

	if (data->client_binary_sizeofint_set
			&& data->client_binary_sizeofint != sizeof(int))
	{
		elog(DEBUG1, "Binary mode rejected: Server and client endian sizeof(int) mismatch");
		return false;
	}

	if (data->client_binary_sizeoflong_set
			&& data->client_binary_sizeoflong != sizeof(long))
	{
		elog(DEBUG1, "Binary mode rejected: Server and client endian sizeof(long) mismatch");
		return false;
	}

	if (data->client_binary_float4byval_set
			&& data->client_binary_float4byval != server_float4_byval())
	{
		elog(DEBUG1, "Binary mode rejected: Server and client endian float4byval mismatch");
		return false;
	}

	if (data->client_binary_float8byval_set
			&& data->client_binary_float8byval != server_float8_byval())
	{
		elog(DEBUG1, "Binary mode rejected: Server and client endian float8byval mismatch");
		return false;
	}

	if (data->client_binary_intdatetimes_set
			&& data->client_binary_intdatetimes != server_integer_datetimes())
	{
		elog(DEBUG1, "Binary mode rejected: Server and client endian integer datetimes mismatch");
		return false;
	}

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
	data->allow_binary_protocol = false;
	data->allow_sendrecv_protocol = false;

	ctx->output_plugin_private = data;

	/*
	 * Tell logical decoding that we will be doing binary output. This is
	 * not the same thing as the selection of binary or text format for
	 * output of individual fields.
	 */
	opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

	/*
	 * This is replication start and not slot initialization.
	 *
	 * Parse and validate options passed by the client.
	 */
	if (!is_init)
	{
		bool	requirenodeid = false;
		int		params_format;

		/* Now parse the rest of the params and ERROR if we see any we don't recognise */
		params_format = process_parameters(ctx->output_plugin_options, data);

		/* TODO: delay until after sending startup reply */
		if (params_format != 1)
			ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("client sent startup parameters in format %d but we only support format 1",
					params_format)));

		/* TODO: Should delay our ERROR until sending startup reply */
		if (data->client_min_proto_version > PG_LOGICAL_PROTO_VERSION_NUM)
			ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("client sent min_proto_version=%d but we only support protocol %d or lower",
					 data->client_min_proto_version, PG_LOGICAL_PROTO_VERSION_NUM)));

		/* TODO: Should delay our ERROR until sending startup reply */
		if (data->client_max_proto_version < PG_LOGICAL_PROTO_MIN_VERSION_NUM)
			ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("client sent max_proto_version=%d but we only support protocol %d or higher",
				 	data->client_max_proto_version, PG_LOGICAL_PROTO_MIN_VERSION_NUM)));

		/* check for encoding match if specific encoding demanded by client */
		if (strlen(data->client_expected_encoding) != 0
				&& strcmp(data->client_expected_encoding, GetDatabaseEncodingName()) != 0)
		{
			elog(ERROR, "only \"%s\" encoding is supported by this server, client requested %s",
				 GetDatabaseEncodingName(), data->client_expected_encoding);
		}

		if (data->client_want_binary_basetypes)
		{
			data->allow_binary_protocol =
				check_binary_compatibility(data);
		}

		if (data->client_want_sendrecv_basetypes &&
			data->client_binary_basetypes_major_version == PG_VERSION_NUM / 100)
		{
			data->allow_sendrecv_protocol = true;
		}

		/* Hooks */
		if (data->table_change_filter != NULL)
		{
			/* Validate the function but don't store the Oid */
			(void) get_table_filter_function_id(data->table_change_filter, true);

			/* We need to properly invalidate the hooks */
			CacheRegisterSyscacheCallback(PROCOID, hook_cache_callback, PointerGetDatum(data));

			/* Node id is required parameter if there is hook which needs it */
			requirenodeid = true;
		}

		if (requirenodeid && data->node_id == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("node_id param must be specified if hook Hooks.Table_change_filter is specified")));
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
