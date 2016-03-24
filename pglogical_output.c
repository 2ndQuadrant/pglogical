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
#include "pglogical_output.h"

#include "mb/pg_wchar.h"
#include "replication/logical.h"
#ifdef HAVE_REPLICATION_ORIGINS
#include "replication/origin.h"
#endif

#include "pglogical_config.h"
#include "pglogical_output_internal.h"
#include "pglogical_proto.h"
#include "pglogical_hooks.h"
#include "pglogical_relmetacache.h"

PG_MODULE_MAGIC;

extern void		_PG_output_plugin_init(OutputPluginCallbacks *cb);

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
		elog(DEBUG1, "Binary mode rejected: Server and client endian mismatch");
		return false;
	}

	if (data->client_binary_sizeofdatum != 0
		&& data->client_binary_sizeofdatum != sizeof(Datum))
	{
		elog(DEBUG1, "Binary mode rejected: Server and client sizeof(Datum) mismatch");
		return false;
	}

	if (data->client_binary_sizeofint != 0
		&& data->client_binary_sizeofint != sizeof(int))
	{
		elog(DEBUG1, "Binary mode rejected: Server and client sizeof(int) mismatch");
		return false;
	}

	if (data->client_binary_sizeoflong != 0
		&& data->client_binary_sizeoflong != sizeof(long))
	{
		elog(DEBUG1, "Binary mode rejected: Server and client sizeof(long) mismatch");
		return false;
	}

	if (data->client_binary_float4byval_set
		&& data->client_binary_float4byval != server_float4_byval())
	{
		elog(DEBUG1, "Binary mode rejected: Server and client float4byval mismatch");
		return false;
	}

	if (data->client_binary_float8byval_set
		&& data->client_binary_float8byval != server_float8_byval())
	{
		elog(DEBUG1, "Binary mode rejected: Server and client float8byval mismatch");
		return false;
	}

	if (data->client_binary_intdatetimes_set
		&& data->client_binary_intdatetimes != server_integer_datetimes())
	{
		elog(DEBUG1, "Binary mode rejected: Server and client integer datetimes mismatch");
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

	data->context = AllocSetContextCreate(ctx->context,
										  "pglogical conversion context",
										  ALLOCSET_DEFAULT_MINSIZE,
										  ALLOCSET_DEFAULT_INITSIZE,
										  ALLOCSET_DEFAULT_MAXSIZE);
	data->allow_internal_basetypes = false;
	data->allow_binary_basetypes = false;


	ctx->output_plugin_private = data;

	/*
	 * This is replication start and not slot initialization.
	 *
	 * Parse and validate options passed by the client.
	 */
	if (!is_init)
	{
		int		params_format;

		 /*
		 * Ideally we'd send the startup message immediately. That way
		 * it'd arrive before any error we emit if we see incompatible
		 * options sent by the client here. That way the client could
		 * possibly adjust its options and reconnect. It'd also make
		 * sure the client gets the startup message in a timely way if
		 * the server is idle, since otherwise it could be a while
		 * before the next callback.
		 *
		 * The decoding plugin API doesn't let us write to the stream
		 * from here, though, so we have to delay the startup message
		 * until the first change processed on the stream, in a begin
		 * callback.
		 *
		 * If we ERROR there, the startup message is buffered but not
		 * sent since the callback didn't finish. So we'd have to send
		 * the startup message, finish the callback and check in the
		 * next callback if we need to ERROR.
		 *
		 * That's a bit much hoop jumping, so for now ERRORs are
		 * immediate. A way to emit a message from the startup callback
		 * is really needed to change that.
		 */
		startup_message_sent = false;

		/* Now parse the rest of the params and ERROR if we see any we don't recognise */
		params_format = process_parameters(ctx->output_plugin_options, data);

		if (params_format != 1)
			ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("client sent startup parameters in format %d but we only support format 1",
					params_format)));

		if (data->client_min_proto_version > PGLOGICAL_PROTO_VERSION_NUM)
			ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("client sent min_proto_version=%d but we only support protocol %d or lower",
					 data->client_min_proto_version, PGLOGICAL_PROTO_VERSION_NUM)));

		if (data->client_max_proto_version < PGLOGICAL_PROTO_MIN_VERSION_NUM)
			ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("client sent max_proto_version=%d but we only support protocol %d or higher",
				 	data->client_max_proto_version, PGLOGICAL_PROTO_MIN_VERSION_NUM)));

		/*
		 * Set correct protocol format.
		 *
		 * This is the output plugin protocol format, this is different
		 * from the individual fields binary vs textual format.
		 */
		if (data->client_protocol_format != NULL
				&& strcmp(data->client_protocol_format, "json") == 0)
		{
			data->api = pglogical_init_api(PGLogicalProtoJson);
			opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
		}
		else if ((data->client_protocol_format != NULL
			     && strcmp(data->client_protocol_format, "native") == 0)
				 || data->client_protocol_format == NULL)
		{
			data->api = pglogical_init_api(PGLogicalProtoNative);
			opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

			if (data->client_no_txinfo)
			{
				elog(WARNING, "no_txinfo option ignored for protocols other than json");
				data->client_no_txinfo = false;
			}
		}
		else
		{
			ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("client requested protocol %s but only \"json\" or \"native\" are supported",
				 	data->client_protocol_format)));
		}

		/* check for encoding match if specific encoding demanded by client */
		if (data->client_expected_encoding != NULL
				&& strlen(data->client_expected_encoding) != 0)
		{
			int wanted_encoding = pg_char_to_encoding(data->client_expected_encoding);

			if (wanted_encoding == -1)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("unrecognised encoding name %s passed to expected_encoding",
								data->client_expected_encoding)));

			if (opt->output_type == OUTPUT_PLUGIN_TEXTUAL_OUTPUT)
			{
				/*
				 * datum encoding must match assigned client_encoding in text
				 * proto, since everything is subject to client_encoding
				 * conversion.
				 */
				if (wanted_encoding != pg_get_client_encoding())
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("expected_encoding must be unset or match client_encoding in text protocols")));
			}
			else
			{
				/*
				 * currently in the binary protocol we can only emit encoded
				 * datums in the server encoding. There's no support for encoding
				 * conversion.
				 */
				if (wanted_encoding != GetDatabaseEncoding())
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("encoding conversion for binary datum not supported yet"),
							 errdetail("expected_encoding %s must be unset or match server_encoding %s",
								 data->client_expected_encoding, GetDatabaseEncodingName())));
			}

			data->field_datum_encoding = wanted_encoding;
		}

		/*
		 * It's obviously not possible to send binary representation of data
		 * unless we use the binary output.
		 */
		if (opt->output_type == OUTPUT_PLUGIN_BINARY_OUTPUT &&
			data->client_want_internal_basetypes)
		{
			data->allow_internal_basetypes =
				check_binary_compatibility(data);
		}

		if (opt->output_type == OUTPUT_PLUGIN_BINARY_OUTPUT &&
			data->client_want_binary_basetypes &&
			data->client_binary_basetypes_major_version == PG_VERSION_NUM / 100)
		{
			data->allow_binary_basetypes = true;
		}

		/*
		 * 9.4 lacks origins info so don't forward it.
		 *
		 * There's currently no knob for clients to use to suppress
		 * this info and it's sent if it's supported and available.
		 */
		if (PG_VERSION_NUM/100 == 904)
			data->forward_changeset_origins = false;
		else
			data->forward_changeset_origins = true;

		if (data->hooks_setup_funcname != NIL)
		{

			data->hooks_session_mctxt =
				AllocSetContextCreate(ctx->context,
					"pglogical_output hooks context",
					ALLOCSET_SMALL_MINSIZE,
					ALLOCSET_SMALL_INITSIZE,
					ALLOCSET_SMALL_MAXSIZE);

			load_hooks(data);
			call_startup_hook(data, ctx->output_plugin_options);
		}

		pglogical_init_relmetacache(ctx->context);
	}
}

/*
 * BEGIN callback
 */
static void
pg_decode_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	PGLogicalOutputData* data = (PGLogicalOutputData*)ctx->output_plugin_private;
	bool send_replication_origin = data->forward_changeset_origins;

	if (!startup_message_sent)
		send_startup_message(ctx, data, false /* can't be last message */);

#ifdef HAVE_REPLICATION_ORIGINS
	/* If the record didn't originate locally, send origin info */
	send_replication_origin &= txn->origin_id != InvalidRepOriginId;
#endif

	OutputPluginPrepareWrite(ctx, !send_replication_origin);
	data->api->write_begin(ctx->out, data, txn);

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
		if (data->api->write_origin &&
			replorigin_by_oid(txn->origin_id, true, &origin))
			data->api->write_origin(ctx->out, origin, txn->origin_lsn);
	}
#endif

	OutputPluginWrite(ctx, true);
}

/*
 * COMMIT callback
 */
static void
pg_decode_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
					 XLogRecPtr commit_lsn)
{
	PGLogicalOutputData* data = (PGLogicalOutputData*)ctx->output_plugin_private;

	OutputPluginPrepareWrite(ctx, true);
	data->api->write_commit(ctx->out, data, txn, commit_lsn);
	OutputPluginWrite(ctx, true);

	/*
	 * Now is a good time to get rid of invalidated relation
	 * metadata entries since nothing will be referencing them
	 * at the moment.
	 */
	pglogical_prune_relmetacache();
}

static void
pg_decode_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				 Relation relation, ReorderBufferChange *change)
{
	PGLogicalOutputData *data = ctx->output_plugin_private;
	MemoryContext old;
	struct PGLRelMetaCacheEntry *cached_relmeta = NULL;


	/* First check the table filter */
	if (!call_row_filter_hook(data, txn, relation, change))
		return;

	/* Avoid leaking memory by using and resetting our own context */
	old = MemoryContextSwitchTo(data->context);

	/*
	 * If the protocol wants to write relation information and the client
	 * isn't known to have metadata cached for this relation already,
	 * send relation metadata.
	 *
	 * TODO: track hit/miss stats
	 */
	if (data->api->write_rel != NULL &&
			!pglogical_cache_relmeta(data, relation, &cached_relmeta))
	{
		OutputPluginPrepareWrite(ctx, false);
		data->api->write_rel(ctx->out, data, relation, cached_relmeta);
		OutputPluginWrite(ctx, false);
	}

	/* Send the data */
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			OutputPluginPrepareWrite(ctx, true);
			data->api->write_insert(ctx->out, data, relation,
									&change->data.tp.newtuple->tuple);
			OutputPluginWrite(ctx, true);
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			{
				HeapTuple oldtuple = change->data.tp.oldtuple ?
					&change->data.tp.oldtuple->tuple : NULL;

				OutputPluginPrepareWrite(ctx, true);
				data->api->write_update(ctx->out, data, relation, oldtuple,
										&change->data.tp.newtuple->tuple);
				OutputPluginWrite(ctx, true);
				break;
			}
		case REORDER_BUFFER_CHANGE_DELETE:
			if (change->data.tp.oldtuple)
			{
				OutputPluginPrepareWrite(ctx, true);
				data->api->write_delete(ctx->out, data, relation,
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

	return false;
}
#endif

static void
send_startup_message(LogicalDecodingContext *ctx,
		PGLogicalOutputData *data, bool last_message)
{
	List *msg;

	Assert(!startup_message_sent);

	msg = prepare_startup_message(data);

	/*
	 * We could free the extra_startup_params DefElem list here, but it's
	 * pretty harmless to just ignore it, since it's in the decoding memory
	 * context anyway, and we don't know if it's safe to free the defnames or
	 * not.
	 */

	OutputPluginPrepareWrite(ctx, last_message);
	data->api->write_startup_message(ctx->out, msg);
	OutputPluginWrite(ctx, last_message);

	list_free_deep(msg);

	startup_message_sent = true;
}

static void pg_decode_shutdown(LogicalDecodingContext * ctx)
{
	PGLogicalOutputData* data = (PGLogicalOutputData*)ctx->output_plugin_private;

	call_shutdown_hook(data);

	pglogical_destroy_relmetacache();

	/*
	 * no need to delete data->context or data->hooks_session_mctxt as they're
	 * children of ctx->context which will expire on return.
	 */
}
