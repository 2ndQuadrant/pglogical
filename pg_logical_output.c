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

#include "access/hash.h"
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
#include "utils/catcache.h"
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
static Oid get_table_filter_function_id(HookFuncName *funcname, bool validate);
static void hook_cache_callback(Datum arg, int cacheid, uint32 hashvalue);
static inline bool pg_decode_change_filter(PGLogicalOutputData *data,
						Relation rel,
						enum ReorderBufferChangeType change);

static bool server_float4_byval(void);
static bool server_float8_byval(void);
static bool server_integer_datetimes(void);
static bool server_bigendian(void);

typedef struct HookCacheEntry {
	HookFuncName	name;	/* schema-qualified name of hook function. Hash key. */
	Oid				funcoid; /* oid in pg_proc of function */
	FmgrInfo		flinfo;  /* fmgrinfo for direct calls to the function */
} HookCacheEntry;

/*
 * Per issue #2, we need a hash table that invalidation callbacks can
 * access because they survive past the logical decoding context and
 * therefore past our local PGLogicalOutputData's lifetime.
 */
static HTAB *HookCache = NULL;

static MemoryContext HookCacheMemoryContext;


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

static void
init_hook_cache(void)
{
	HASHCTL	ctl;
	int hash_flags = HASH_ELEM | HASH_CONTEXT;

	if (HookCache != NULL)
		return;

	/* Make sure we've initialized CacheMemoryContext. */
	if (CacheMemoryContext == NULL)
		CreateCacheMemoryContext();

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(HookFuncName);
	ctl.entrysize = sizeof(HookCacheEntry);
	/* safe to allocate to CacheMemoryContext since it's never reset */
	ctl.hcxt = CacheMemoryContext;

#if PG_VERSION_NUM/100 == 904
	ctl.hash = tag_hash;
	hash_flags |= HASH_FUNCTION;
#else
	hash_flags |= HASH_BLOBS;
#endif
	HookCache = hash_create("pg_logical hook cache", 32, &ctl, hash_flags);

	Assert(HookCache != NULL);

	/*
	 * The hook cache allocation set contains the FmgrInfo entries. It does
	 * *not* contain the hash its self, which must survive cache invalidations.
	 */
	HookCacheMemoryContext = AllocSetContextCreate(CacheMemoryContext,
												   "pglogical output hook cache",
												   ALLOCSET_DEFAULT_MINSIZE,
												   ALLOCSET_DEFAULT_INITSIZE,
												   ALLOCSET_DEFAULT_MAXSIZE);

	/* Watch for invalidation events. */
	CacheRegisterSyscacheCallback(PROCOID, hook_cache_callback, (Datum)0);
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
	PGLogicalOutputData  *data = palloc0(sizeof(PGLogicalOutputData));

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

	init_hook_cache();

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
		if (data->client_expected_encoding != NULL
				&& strlen(data->client_expected_encoding) != 0
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
			/*
			 * Validate the function but don't store the FmgrInfo
			 *
			 * The function might not be valid once we
			 * catalog-timetravel to the point we start replaying the
			 * slot at, and the relcache is going to get invalidated
			 * anyway. So we need to look it up on first use.
			 */
			(void) get_table_filter_function_id(data->table_change_filter, true);

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
 * Decide if the individual change should be filtered out.
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
		HookCacheEntry *hook;

		/* Find cached function info */
		hook = (HookCacheEntry*) hash_search(HookCache,
											 (void *) data->table_change_filter,
											 HASH_FIND, NULL);

		if (hook == NULL)
		{
			Oid		funcoid =
				get_table_filter_function_id(data->table_change_filter, false);

			/*
			 * Not found, this can be historical snapshot and we can't do
			 * validation one way or the other so we act as if the hook
			 * was not provided. We know the hook exists in up-to-date
			 * snapshots since we found it during init, it just doesn't
			 * exist in this timetraveled snapshot yet. We'll keep
			 * looking until we find it.
			 */
			if (!OidIsValid(funcoid))
				return false;

			/* Update the cache and continue */
			hook = (HookCacheEntry*) hash_search(HookCache,
												 (void *) data->table_change_filter,
											     HASH_ENTER, NULL);

			hook->funcoid = funcoid;

			/*
			 * Prepare a FmgrInfo to cache so we can call the function
			 * more efficiently, avoiding the need to OidFunctionCall3
			 * each time.
			 *
			 * Must be allocated in the cache context so that it doesn't
			 * get purged before the cache entry containing the struct
			 * its self.
			 *
			 * We create a sub-context under CacheMemoryContext so that
			 * we can flush that context when we get a cache invalidation.
			 */
			fmgr_info_cxt(funcoid, &hook->flinfo, HookCacheMemoryContext);
		}

		Assert(hook != NULL);
		Assert(OidIsValid(hook->funcoid));

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

		res = FunctionCall3(&hook->flinfo,
							CStringGetTextDatum(data->node_id),
							ObjectIdGetDatum(RelationGetRelid(rel)),
							CharGetDatum(change_type));

		return DatumGetBool(res);
	}

	/* Default action is to always replicate the change, so don't filter. */
	return false;
}

/*
 * Hook oid cache invalidation, for when a hook function
 * gets replaced.
 *
 * Currently we flush everything, even if the hashvalue is
 * nonzero so only one entry is being invalidated. Hook
 * function lookups are cheap enough and pg_proc isn't
 * going to be constantly churning.
 */
static void
hook_cache_callback(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS status;
	HookCacheEntry *hentry;

	Assert(cacheid == PROCOID);

	hash_seq_init(&status, HookCache);

	/*
	 * Remove every entry from the hash. This won't free heap-allocated memory
	 * in the cache entries, such as the FmgrInfo entries.
	 */
	while ((hentry = (HookCacheEntry*) hash_seq_search(&status)) != NULL)
	{
		if (hash_search(HookCache,
						(void *) &hentry->name,
						HASH_REMOVE, NULL) == NULL)
			elog(ERROR, "pglogical HookCacheEntry hash table corrupted");
	}

	/*
	 * The cache hash is now flushed and we no longer have references to
	 * the FmgrInfo entries. We can flush the memory context that contains
	 * them.
	 *
	 * There's no function to free a FmgrInfo's individual parts so we have
	 * to reset the context.
	 */
	MemoryContextReset(HookCacheMemoryContext);
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
get_table_filter_function_id(HookFuncName *funcname, bool validate)
{
	Oid			funcid;
	Oid			funcargtypes[3];
	List		*key;

	funcargtypes[0] = TEXTOID;	/* identifier of this node */
	funcargtypes[1] = OIDOID;	/* relation */
	funcargtypes[2] = CHAROID;	/* change type */

	/*
	 * We require that filter function names be schema-qualified
	 * so this is always a 2-list. No catalog name is permitted.
	 */
	key = list_make2(makeString(funcname->schema),
					 makeString(funcname->function));

	/* find the the function */
	funcid = LookupFuncName(key, 3, funcargtypes, !validate);

	if (!OidIsValid(funcid))
	{
		if (validate)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("function %s not found",
							NameListToString(key))));
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
							NameListToString(key))));
		else
			return InvalidOid;
	}

	if (func_volatile(funcid) == PROVOLATILE_VOLATILE)
	{
		if (validate)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("function %s must not be VOLATILE",
							NameListToString(key))));
		else
			return InvalidOid;
	}

	list_free(key);

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
