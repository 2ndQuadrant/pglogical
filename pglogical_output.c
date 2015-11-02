/*-------------------------------------------------------------------------
 *
 * pglogical_output.c
 *		  Logical Replication output plugin
 *
 * Copyright (c) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_output.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pglogical_output/compat.h"
#include "pglogical_config.h"
#include "pglogical_output.h"
#include "pglogical_proto.h"
#include "pglogical_hooks.h"

#include "access/hash.h"
#include "access/sysattr.h"
#include "access/xact.h"

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
static void pg_decode_shutdown(LogicalDecodingContext * ctx);
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

static void send_startup_message(LogicalDecodingContext *ctx,
		PGLogicalOutputData *data, bool last_message);

static bool startup_message_sent = false;

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
	cb->shutdown_cb = pg_decode_shutdown;
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

	if (data->client_binary_sizeofdatum != 0
		&& data->client_binary_sizeofdatum != sizeof(Datum))
	{
		elog(DEBUG1, "Binary mode rejected: Server and client endian sizeof(Datum) mismatch");
		return false;
	}

	if (data->client_binary_sizeofint != 0
		&& data->client_binary_sizeofint != sizeof(int))
	{
		elog(DEBUG1, "Binary mode rejected: Server and client endian sizeof(int) mismatch");
		return false;
	}

	if (data->client_binary_sizeoflong != 0
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
										  "pglogical conversion context",
										  ALLOCSET_DEFAULT_MINSIZE,
										  ALLOCSET_DEFAULT_INITSIZE,
										  ALLOCSET_DEFAULT_MAXSIZE);
	data->allow_internal_basetypes = false;
	data->allow_binary_basetypes = false;

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
		int		params_format;

		startup_message_sent = false;

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
		/* TODO: Should parse encoding name and compare properly */
		if (data->client_expected_encoding != NULL
				&& strlen(data->client_expected_encoding) != 0
				&& strcmp(data->client_expected_encoding, GetDatabaseEncodingName()) != 0)
		{
			ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only \"%s\" encoding is supported by this server, client requested %s",
				 	GetDatabaseEncodingName(), data->client_expected_encoding)));
		}

		if (data->client_want_internal_basetypes)
		{
			data->allow_internal_basetypes =
				check_binary_compatibility(data);
		}

		if (data->client_want_binary_basetypes &&
			data->client_binary_basetypes_major_version == PG_VERSION_NUM / 100)
		{
			data->allow_binary_basetypes = true;
		}

		/*
		 * Will we forward changesets? We have to if we're on 9.4;
		 * otherwise honour the client's request.
		 */
		if (PG_VERSION_NUM/100 == 904)
		{
			/*
			 * 9.4 unconditionally forwards changesets due to lack of
			 * replication origins, and it can't ever send origin info
			 * for the same reason.
			 */
			data->forward_changesets = true;
			data->forward_changeset_origins = false;

			if (data->client_forward_changesets_set
				&& !data->client_forward_changesets)
			{
				ereport(DEBUG1,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("Cannot disable changeset forwarding on PostgreSQL 9.4")));
			}
		}
		else if (data->client_forward_changesets_set
				 && data->client_forward_changesets)
		{
			/* Client explicitly asked for forwarding; forward csets and origins */
			data->forward_changesets = true;
			data->forward_changeset_origins = true;
		}
		else
		{
			/* Default to not forwarding or honour client's request not to fwd */
			data->forward_changesets = false;
			data->forward_changeset_origins = false;
		}

		if (data->hooks_setup_funcname != NIL)
		{

			data->hooks_mctxt = AllocSetContextCreate(ctx->context,
					"pglogical_output hooks context",
					ALLOCSET_SMALL_MINSIZE,
					ALLOCSET_SMALL_INITSIZE,
					ALLOCSET_SMALL_MAXSIZE);

			load_hooks(data);
			call_startup_hook(data, ctx->output_plugin_options);
		}
	}
}

/*
 * BEGIN callback
 */
void
pg_decode_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	PGLogicalOutputData* data = (PGLogicalOutputData*)ctx->output_plugin_private;
	bool send_replication_origin = data->forward_changeset_origins;

	if (!startup_message_sent)
		send_startup_message( ctx, data, false /* can't be last message */);

#ifdef HAVE_REPLICATION_ORIGINS
	/* If the record didn't originate locally, send origin info */
	send_replication_origin &= txn->origin_id != InvalidRepOriginId;
#endif

	OutputPluginPrepareWrite(ctx, !send_replication_origin);
	pglogical_write_begin(ctx->out, txn);

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
			pglogical_write_origin(ctx->out, origin, txn->origin_lsn);
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
	pglogical_write_commit(ctx->out, txn, commit_lsn);
	OutputPluginWrite(ctx, true);
}

void
pg_decode_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				 Relation relation, ReorderBufferChange *change)
{
	PGLogicalOutputData *data;
	MemoryContext old;

	data = ctx->output_plugin_private;

	/* First check the table filter */
	if (!call_row_filter_hook(data, txn, relation, change))
		return;

	/* Avoid leaking memory by using and resetting our own context */
	old = MemoryContextSwitchTo(data->context);

	/* TODO: add caching (send only if changed) */
	OutputPluginPrepareWrite(ctx, false);
	pglogical_write_rel(ctx->out, relation);
	OutputPluginWrite(ctx, false);

	/* Send the data */
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			OutputPluginPrepareWrite(ctx, true);
			pglogical_write_insert(ctx->out, data, relation,
									&change->data.tp.newtuple->tuple);
			OutputPluginWrite(ctx, true);
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			{
				HeapTuple oldtuple = change->data.tp.oldtuple ?
					&change->data.tp.oldtuple->tuple : NULL;

				OutputPluginPrepareWrite(ctx, true);
				pglogical_write_update(ctx->out, data, relation, oldtuple,
										&change->data.tp.newtuple->tuple);
				OutputPluginWrite(ctx, true);
				break;
			}
		case REORDER_BUFFER_CHANGE_DELETE:
			if (change->data.tp.oldtuple)
			{
				OutputPluginPrepareWrite(ctx, true);
				pglogical_write_delete(ctx->out, data, relation,
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

	if (!call_txn_filter_hook(data, origin_id))
		return true;

	if (!data->forward_changesets && origin_id != InvalidRepOriginId)
		return true;

	return false;
}
#endif

static void
send_startup_message(LogicalDecodingContext *ctx,
		PGLogicalOutputData *data, bool last_message)
{
	char *msg;
	int len;

	Assert(!startup_message_sent);

	prepare_startup_message(data, &msg, &len);

	/*
	 * We could free the extra_startup_params DefElem list here, but it's
	 * pretty harmless to just ignore it, since it's in the decoding memory
	 * context anyway, and we don't know if it's safe to free the defnames or
	 * not.
	 */

	OutputPluginPrepareWrite(ctx, last_message);
	write_startup_message(ctx->out, msg, len);
	OutputPluginWrite(ctx, last_message);

	pfree(msg);

	startup_message_sent = true;
}

static void pg_decode_shutdown(LogicalDecodingContext * ctx)
{
	PGLogicalOutputData* data = (PGLogicalOutputData*)ctx->output_plugin_private;

	call_shutdown_hook(data);

	if (data->hooks_mctxt != NULL)
	{
		MemoryContextDelete(data->hooks_mctxt);
		data->hooks_mctxt = NULL;
	}
}
