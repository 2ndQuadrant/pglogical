/*-------------------------------------------------------------------------
 *
 * pglogical_config.c
 *		  Logical Replication output plugin
 *
 * Copyright (c) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_config.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "pglogical_output.h"

#include "catalog/catversion.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/int8.h"

#include "pglogical_config.h"
#include "pglogical_output_internal.h"

typedef enum PGLogicalOutputParamType
{
	OUTPUT_PARAM_TYPE_BOOL,
	OUTPUT_PARAM_TYPE_UINT32,
	OUTPUT_PARAM_TYPE_INT32,
	OUTPUT_PARAM_TYPE_STRING,
	OUTPUT_PARAM_TYPE_QUALIFIED_NAME
} PGLogicalOutputParamType;

/* param parsing */
static Datum get_param_value(DefElem *elem, bool null_ok,
		PGLogicalOutputParamType type);

static Datum get_param(List *options, const char *name, bool missing_ok,
					   bool null_ok, PGLogicalOutputParamType type,
					   bool *found);
static bool parse_param_bool(DefElem *elem);
static uint32 parse_param_uint32(DefElem *elem);
static int32 parse_param_int32(DefElem *elem);

static void
process_parameters_v1(List *options, PGLogicalOutputData *data);

enum {
	PARAM_UNRECOGNISED,
	PARAM_MAX_PROTOCOL_VERSION,
	PARAM_MIN_PROTOCOL_VERSION,
	PARAM_PROTOCOL_FORMAT,
	PARAM_EXPECTED_ENCODING,
	PARAM_BINARY_BIGENDIAN,
	PARAM_BINARY_SIZEOF_DATUM,
	PARAM_BINARY_SIZEOF_INT,
	PARAM_BINARY_SIZEOF_LONG,
	PARAM_BINARY_FLOAT4BYVAL,
	PARAM_BINARY_FLOAT8BYVAL,
	PARAM_BINARY_INTEGER_DATETIMES,
	PARAM_BINARY_WANT_INTERNAL_BASETYPES,
	PARAM_BINARY_WANT_BINARY_BASETYPES,
	PARAM_BINARY_BASETYPES_MAJOR_VERSION,
	PARAM_PG_VERSION,
	PARAM_HOOKS_SETUP_FUNCTION,
	PARAM_NO_TXINFO
} OutputPluginParamKey;

typedef struct {
	const char * const paramname;
	int paramkey;
} OutputPluginParam;

/* Oh, if only C had switch on strings */
static OutputPluginParam param_lookup[] = {
	{"max_proto_version", PARAM_MAX_PROTOCOL_VERSION},
	{"min_proto_version", PARAM_MIN_PROTOCOL_VERSION},
	{"proto_format", PARAM_PROTOCOL_FORMAT},
	{"expected_encoding", PARAM_EXPECTED_ENCODING},
	{"binary.bigendian", PARAM_BINARY_BIGENDIAN},
	{"binary.sizeof_datum", PARAM_BINARY_SIZEOF_DATUM},
	{"binary.sizeof_int", PARAM_BINARY_SIZEOF_INT},
	{"binary.sizeof_long", PARAM_BINARY_SIZEOF_LONG},
	{"binary.float4_byval", PARAM_BINARY_FLOAT4BYVAL},
	{"binary.float8_byval", PARAM_BINARY_FLOAT8BYVAL},
	{"binary.integer_datetimes", PARAM_BINARY_INTEGER_DATETIMES},
	{"binary.want_internal_basetypes", PARAM_BINARY_WANT_INTERNAL_BASETYPES},
	{"binary.want_binary_basetypes", PARAM_BINARY_WANT_BINARY_BASETYPES},
	{"binary.basetypes_major_version", PARAM_BINARY_BASETYPES_MAJOR_VERSION},
	{"pg_version", PARAM_PG_VERSION},
	{"hooks.setup_function", PARAM_HOOKS_SETUP_FUNCTION},
	{"no_txinfo", PARAM_NO_TXINFO},
	{NULL, PARAM_UNRECOGNISED}
};

/*
 * Look up a param name to find the enum value for the
 * param, or PARAM_UNRECOGNISED if not found.
 */
static int
get_param_key(const char * const param_name)
{
	OutputPluginParam *param = &param_lookup[0];

	do {
		if (strcmp(param->paramname, param_name) == 0)
			return param->paramkey;
		param++;
	} while (param->paramname != NULL);

	return PARAM_UNRECOGNISED;
}


void
process_parameters_v1(List *options, PGLogicalOutputData *data)
{
	Datum		val;
	ListCell	*lc;

	/*
	 * max_proto_version and min_proto_version are specified
	 * as required, and must be parsed before anything else.
	 *
	 * TODO: We should still parse them as optional and
	 * delay the ERROR until after the startup reply.
	 */
	val = get_param(options, "max_proto_version", false, false,
					OUTPUT_PARAM_TYPE_UINT32, NULL);
	data->client_max_proto_version = DatumGetUInt32(val);

	val = get_param(options, "min_proto_version", false, false,
					OUTPUT_PARAM_TYPE_UINT32, NULL);
	data->client_min_proto_version = DatumGetUInt32(val);

	/* Examine all the other params in the v1 message. */
	foreach(lc, options)
	{
		DefElem    *elem = lfirst(lc);

		Assert(elem->arg == NULL || IsA(elem->arg, String));

		/* Check each param, whether or not we recognise it */
		switch(get_param_key(elem->defname))
		{
			case PARAM_BINARY_BIGENDIAN:
				val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_BOOL);
				data->client_binary_bigendian_set = true;
				data->client_binary_bigendian = DatumGetBool(val);
				break;

			case PARAM_BINARY_SIZEOF_DATUM:
				val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_UINT32);
				data->client_binary_sizeofdatum = DatumGetUInt32(val);
				break;

			case PARAM_BINARY_SIZEOF_INT:
				val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_UINT32);
				data->client_binary_sizeofint = DatumGetUInt32(val);
				break;

			case PARAM_BINARY_SIZEOF_LONG:
				val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_UINT32);
				data->client_binary_sizeoflong = DatumGetUInt32(val);
				break;

			case PARAM_BINARY_FLOAT4BYVAL:
				val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_BOOL);
				data->client_binary_float4byval_set = true;
				data->client_binary_float4byval = DatumGetBool(val);
				break;

			case PARAM_BINARY_FLOAT8BYVAL:
				val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_BOOL);
				data->client_binary_float4byval_set = true;
				data->client_binary_float4byval = DatumGetBool(val);
				break;

			case PARAM_BINARY_INTEGER_DATETIMES:
				val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_BOOL);
				data->client_binary_intdatetimes_set = true;
				data->client_binary_intdatetimes = DatumGetBool(val);
				break;

			case PARAM_PROTOCOL_FORMAT:
				val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_STRING);
				data->client_protocol_format = DatumGetCString(val);
				break;

			case PARAM_EXPECTED_ENCODING:
				val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_STRING);
				data->client_expected_encoding = DatumGetCString(val);
				break;

			case PARAM_PG_VERSION:
				val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_UINT32);
				data->client_pg_version = DatumGetUInt32(val);
				break;

			case PARAM_BINARY_WANT_INTERNAL_BASETYPES:
				/* check if we want to use internal data representation */
				val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_BOOL);
				data->client_want_internal_basetypes_set = true;
				data->client_want_internal_basetypes = DatumGetBool(val);
				break;

			case PARAM_BINARY_WANT_BINARY_BASETYPES:
				/* check if we want to use binary data representation */
				val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_BOOL);
				data->client_want_binary_basetypes_set = true;
				data->client_want_binary_basetypes = DatumGetBool(val);
				break;

			case PARAM_BINARY_BASETYPES_MAJOR_VERSION:
				val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_UINT32);
				data->client_binary_basetypes_major_version = DatumGetUInt32(val);
				break;

			case PARAM_HOOKS_SETUP_FUNCTION:
				val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_QUALIFIED_NAME);
				data->hooks_setup_funcname = (List*) PointerGetDatum(val);
				break;

			case PARAM_NO_TXINFO:
				val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_BOOL);
				data->client_no_txinfo = DatumGetBool(val);
				break;

			case PARAM_UNRECOGNISED:
				ereport(DEBUG1,
						(errmsg("Unrecognised pglogical parameter %s ignored", elem->defname)));
				break;
		}
	}
}

/*
 * Read parameters sent by client at startup and store recognised
 * ones in the parameters PGLogicalOutputData.
 *
 * The PGLogicalOutputData must have all client-supplied parameter fields
 * zeroed, such as by memset or palloc0, since values not supplied
 * by the client are not set.
 */
int
process_parameters(List *options, PGLogicalOutputData *data)
{
	Datum	val;
	int		params_format;

	val = get_param(options, "startup_params_format", false, false,
					OUTPUT_PARAM_TYPE_UINT32, NULL);

	params_format = DatumGetUInt32(val);

	if (params_format == PGLOGICAL_STARTUP_PARAM_FORMAT_FLAT)
		process_parameters_v1(options, data);
	else
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("startup_params_format %d not supported, only version %d supported",
					 params_format, PGLOGICAL_STARTUP_PARAM_FORMAT_FLAT)));

	return params_format;
}

static Datum
get_param_value(DefElem *elem, bool null_ok, PGLogicalOutputParamType type)
{
	/* Check for NULL value */
	if (elem->arg == NULL || strVal(elem->arg) == NULL)
	{
		if (null_ok)
			return (Datum) 0;
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("parameter \"%s\" cannot be NULL", elem->defname)));
	}

	switch (type)
	{
		case OUTPUT_PARAM_TYPE_UINT32:
			return UInt32GetDatum(parse_param_uint32(elem));
		case OUTPUT_PARAM_TYPE_INT32:
			return Int32GetDatum(parse_param_int32(elem));
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

	if (found != NULL)
		*found = false;
	else
		Assert(!missing_ok);

	foreach(option, options)
	{
		DefElem    *elem = lfirst(option);

		Assert(elem->arg == NULL || IsA(elem->arg, String));

		/* Search until matching parameter found */
		if (pg_strcasecmp(name, elem->defname))
			continue;

		if (found != NULL)
			*found = true;

		return get_param_value(elem, null_ok, type);
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
				 errmsg("could not parse boolean value \"%s\" for parameter \"%s\"",
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
				 errmsg("could not parse integer value \"%s\" for parameter \"%s\"",
						strVal(elem->arg), elem->defname)));

	if (res > PG_UINT32_MAX || res < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("value \"%s\" out of range for parameter \"%s\"",
						strVal(elem->arg), elem->defname)));

	return (uint32) res;
}

static int32
parse_param_int32(DefElem *elem)
{
	int64		res;

	if (!scanint8(strVal(elem->arg), true, &res))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not parse integer value \"%s\" for parameter \"%s\"",
						strVal(elem->arg), elem->defname)));

	if (res > PG_INT32_MAX || res < PG_INT32_MIN)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("value \"%s\" out of range for parameter \"%s\"",
						strVal(elem->arg), elem->defname)));

	return (int32) res;
}

static List*
add_startup_msg_s(List *l, char *key, char *val)
{
	return lappend(l, makeDefElem(key, (Node*)makeString(val)));
}

static List*
add_startup_msg_i(List *l, char *key, int val)
{
	return lappend(l, makeDefElem(key, (Node*)makeString(psprintf("%d", val))));
}

static List*
add_startup_msg_b(List *l, char *key, bool val)
{
	return lappend(l, makeDefElem(key, (Node*)makeString(val ? "t" : "f")));
}

/*
 * This builds the protocol startup message, which is always the first
 * message on the wire after the client sends START_REPLICATION.
 *
 * It confirms to the client that we could apply requested options, and
 * tells the client our capabilities.
 *
 * Any additional parameters provided by the startup hook are also output
 * now.
 *
 * The output param 'msg' is a null-terminated char* palloc'd in the current
 * memory context and the length 'len' of that string that is valid. The caller
 * should pfree the result after use.
 *
 * This is a bit less efficient than direct pq_sendblah calls, but
 * separates config handling from the protocol implementation, and
 * it's not like startup msg performance matters much.
 */
List *
prepare_startup_message(PGLogicalOutputData *data)
{
	ListCell *lc;
	List *l = NIL;

	l = add_startup_msg_s(l, "max_proto_version", "1");
	l = add_startup_msg_s(l, "min_proto_version", "1");

	/* We don't support understand column types yet */
	l = add_startup_msg_b(l, "coltypes", false);

	/* Info about our Pg host */
	l = add_startup_msg_i(l, "pg_version_num", PG_VERSION_NUM);
	l = add_startup_msg_s(l, "pg_version", PG_VERSION);
	l = add_startup_msg_i(l, "pg_catversion", CATALOG_VERSION_NO);

	l = add_startup_msg_s(l, "database_encoding", (char*)GetDatabaseEncodingName());

	l = add_startup_msg_s(l, "encoding", (char*)pg_encoding_to_char(data->field_datum_encoding));

	l = add_startup_msg_b(l, "forward_changeset_origins",
			data->forward_changeset_origins);

	/* and ourselves */
	l = add_startup_msg_s(l, "pglogical_output_version",
			PGLOGICAL_OUTPUT_VERSION);
	l = add_startup_msg_i(l, "pglogical_output_version",
			PGLOGICAL_OUTPUT_VERSION_NUM);

	/* binary options enabled */
	l = add_startup_msg_b(l, "binary.internal_basetypes",
			data->allow_internal_basetypes);
	l = add_startup_msg_b(l, "binary.binary_basetypes",
			data->allow_binary_basetypes);

	/* Binary format characteristics of server */
	l = add_startup_msg_i(l, "binary.basetypes_major_version", PG_VERSION_NUM/100);
	l = add_startup_msg_i(l, "binary.sizeof_int", sizeof(int));
	l = add_startup_msg_i(l, "binary.sizeof_long", sizeof(long));
	l = add_startup_msg_i(l, "binary.sizeof_datum", sizeof(Datum));
	l = add_startup_msg_i(l, "binary.maxalign", MAXIMUM_ALIGNOF);
	l = add_startup_msg_b(l, "binary.bigendian", server_bigendian());
	l = add_startup_msg_b(l, "binary.float4_byval", server_float4_byval());
	l = add_startup_msg_b(l, "binary.float8_byval", server_float8_byval());
	l = add_startup_msg_b(l, "binary.integer_datetimes", server_integer_datetimes());
	/* We don't know how to send in anything except our host's format */
	l = add_startup_msg_i(l, "binary.binary_pg_version",
			PG_VERSION_NUM/100);

	l = add_startup_msg_b(l, "no_txinfo", data->client_no_txinfo);


	/*
	 * Confirm that we've enabled any requested hook functions.
	 */
	l = add_startup_msg_b(l, "hooks.startup_hook_enabled",
			data->hooks.startup_hook != NULL);
	l = add_startup_msg_b(l, "hooks.shutdown_hook_enabled",
			data->hooks.shutdown_hook != NULL);
	l = add_startup_msg_b(l, "hooks.row_filter_enabled",
			data->hooks.row_filter_hook != NULL);
	l = add_startup_msg_b(l, "hooks.transaction_filter_enabled",
			data->hooks.txn_filter_hook != NULL);

	/*
	 * Output any extra params supplied by a startup hook by appending
	 * them verbatim to the params list.
	 */
	foreach(lc, data->extra_startup_params)
	{
		DefElem *param = (DefElem*)lfirst(lc);
		Assert(IsA(param->arg, String) && strVal(param->arg) != NULL);
		l = lappend(l, param);
	}

	return l;
}
