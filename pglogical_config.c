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

#include "pglogical_config.h"
#include "pglogical_compat.h"
#include "pglogical_output.h"

#include "catalog/catversion.h"
#include "catalog/namespace.h"

#include "mb/pg_wchar.h"

#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

typedef enum PGLogicalOutputParamType
{
	OUTPUT_PARAM_TYPE_BOOL,
	OUTPUT_PARAM_TYPE_UINT32,
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

static HookFuncName* qname_to_hookfunc(List *qname);

static void
process_parameters_v1(List *options, PGLogicalOutputData *data);

enum {
	PARAM_UNRECOGNISED,
	PARAM_MAX_PROTOCOL_VERSION,
	PARAM_MIN_PROTOCOL_VERSION,
	PARAM_EXPECTED_ENCODING,
	PARAM_BINARY_BIGENDIAN,
	PARAM_BINARY_SIZEOF_DATUM,
	PARAM_BINARY_SIZEOF_INT,
	PARAM_BINARY_SIZEOF_LONG,
	PARAM_BINARY_FLOAT4BYVAL,
	PARAM_BINARY_FLOAT8BYVAL,
	PARAM_BINARY_INTEGER_DATETIMES,
	PARAM_BINARY_WANT_BINARY_BASETYPES,
	PARAM_BINARY_WANT_SENDRECV_BASETYPES,
	PARAM_BINARY_BASETYPES_MAJOR_VERSION,
	PARAM_PG_VERSION,
	PARAM_FORWARD_CHANGESETS,
	PARAM_HOOKS_TABLE_FILTER,
	PARAM_HOOKS_TABLE_FILTER_ARG
} OutputPluginParamKey;

typedef struct {
	const char * const paramname;
	int paramkey;
} OutputPluginParam;

/* Oh, if only C had switch on strings */
static OutputPluginParam param_lookup[] = {
	{"max_proto_version", PARAM_MAX_PROTOCOL_VERSION},
	{"min_proto_version", PARAM_MIN_PROTOCOL_VERSION},
	{"expected_encoding", PARAM_EXPECTED_ENCODING},
	{"binary.bigendian", PARAM_BINARY_BIGENDIAN},
	{"binary.sizeof_datum", PARAM_BINARY_SIZEOF_DATUM},
	{"binary.sizeof_int", PARAM_BINARY_SIZEOF_INT},
	{"binary.sizeof_long", PARAM_BINARY_SIZEOF_LONG},
	{"binary.float4_byval", PARAM_BINARY_FLOAT4BYVAL},
	{"binary.float8_byval", PARAM_BINARY_FLOAT8BYVAL},
	{"binary.integer_datetimes", PARAM_BINARY_INTEGER_DATETIMES},
	{"binary.want_binary_basetypes", PARAM_BINARY_WANT_BINARY_BASETYPES},
	{"binary.want_sendrecv_basetypes", PARAM_BINARY_WANT_SENDRECV_BASETYPES},
	{"binary.basetypes_major_version", PARAM_BINARY_BASETYPES_MAJOR_VERSION},
	{"pg_version", PARAM_PG_VERSION},
	{"forward_changesets", PARAM_FORWARD_CHANGESETS},
	{"hooks.table_filter", PARAM_HOOKS_TABLE_FILTER},
	{"hooks.table_filter_arg", PARAM_HOOKS_TABLE_FILTER_ARG},
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
	bool    	found;
	ListCell	*lc;

	/*
	 * max_proto_version and min_proto_version are specified
	 * as required, and must be parsed before anything else.
	 *
	 * TODO: We should still parse them as optional and
	 * delay the ERROR until after the startup reply.
	 */
	val = get_param(options, "max_proto_version", false, false,
					OUTPUT_PARAM_TYPE_UINT32, &found);
	data->client_max_proto_version = DatumGetUInt32(val);

	val = get_param(options, "min_proto_version", false, false,
					OUTPUT_PARAM_TYPE_UINT32, &found);
	data->client_min_proto_version = DatumGetUInt32(val);

	/* Examine all the other params in the v1 message. */
	foreach(lc, options)
	{
		DefElem    *elem = lfirst(lc);

		Assert(elem->arg == NULL || IsA(elem->arg, String));

		/* Check each param, whether or not we recognise it */
		switch(get_param_key(elem->defname))
		{
			val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_UINT32);

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

			case PARAM_EXPECTED_ENCODING:
				val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_STRING);
				data->client_expected_encoding = DatumGetCString(val);
				break;

			case PARAM_PG_VERSION:
				val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_UINT32);
				data->client_pg_version = DatumGetUInt32(val);
				break;

			case PARAM_FORWARD_CHANGESETS:
				/*
				 * Check to see if the client asked for changeset forwarding
				 *
				 * Note that we cannot support this on 9.4. We'll tell the client
				 * in the startup reply message.
				 */
				val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_BOOL);
				data->client_forward_changesets_set = true;
				data->client_forward_changesets = DatumGetBool(val);
				break;

			case PARAM_BINARY_WANT_BINARY_BASETYPES:
				/* check if we want to use binary data representation */
				val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_BOOL);
				data->client_want_binary_basetypes_set = true;
				data->client_want_binary_basetypes = DatumGetBool(val);
				break;

			case PARAM_BINARY_WANT_SENDRECV_BASETYPES:
				/* check if we want to use sendrecv data representation */
				val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_BOOL);
				data->client_want_sendrecv_basetypes_set = true;
				data->client_want_sendrecv_basetypes = DatumGetBool(val);
				break;

			case PARAM_BINARY_BASETYPES_MAJOR_VERSION:
				val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_UINT32);
				data->client_binary_basetypes_major_version = DatumGetUInt32(val);
				break;

			case PARAM_HOOKS_TABLE_FILTER:
				{
					List *qname;
					val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_QUALIFIED_NAME);
					qname = (List*) PointerGetDatum(val);
					data->table_filter = qname_to_hookfunc(qname);
					list_free_deep(qname);
				}
				break;

			case PARAM_HOOKS_TABLE_FILTER_ARG:
				val = get_param_value(elem, false, OUTPUT_PARAM_TYPE_STRING);
				data->table_filter_arg = DatumGetCString(val);
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
 * The PGLogicalOutputData must have all client-surprised parameter fields
 * zeroed, such as by memset or palloc0, since values not supplied
 * by the client are not set.
 */
int
process_parameters(List *options, PGLogicalOutputData *data)
{
	Datum	val;
	bool    found;
	int		params_format;

	val = get_param(options, "startup_params_format", false, false,
					OUTPUT_PARAM_TYPE_UINT32, &found);

	params_format = DatumGetUInt32(val);

	if (params_format == 1)
	{
		process_parameters_v1(options, data);
	}

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

	*found = false;

	foreach(option, options)
	{
		DefElem    *elem = lfirst(option);

		Assert(elem->arg == NULL || IsA(elem->arg, String));

		/* Search until matching parameter found */
		if (pg_strcasecmp(name, elem->defname))
			continue;

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

/*
 * Turn a qualified name into a HookFuncName, ensuring padding
 * is zeroed and that the function is fully schema-qualified.
 *
 * Doesn't ensure the function exists.
 */
static HookFuncName*
qname_to_hookfunc(List *qname)
{
	char		   *schemaname,
				   *funcname;
	HookFuncName   *hookfunc;

	if (qname == NULL)
		return NULL;

	DeconstructQualifiedName(qname, &schemaname, &funcname);

	if (schemaname == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("Hook function names must be fully schema qualified"),
				 errdetail("hooks.table_filter was not schema-qualified")));
	}

	Assert(strlen(schemaname) != 0);

	hookfunc = palloc0(sizeof(HookFuncName));

	strcpy(hookfunc->schema, schemaname);
	strcpy(hookfunc->function, funcname);

	return hookfunc;
}

static void
append_startup_msg_key(StringInfo si, const char *key)
{
	appendStringInfoString(si, key);
	appendStringInfoChar(si, '\0');
}

static void
append_startup_msg_s(StringInfo si, const char *key, const char *val)
{
	append_startup_msg_key(si, key);
	appendStringInfoString(si, val);
	appendStringInfoChar(si, '\0');
}

static void
append_startup_msg_i(StringInfo si, const char *key, int val)
{
	append_startup_msg_key(si, key);
	appendStringInfo(si, "%d", val);
	appendStringInfoChar(si, '\0');
}

static void
append_startup_msg_b(StringInfo si, const char *key, bool val)
{
	append_startup_msg_s(si, key, val ? "t" : "f");
}

/*
 * This builds the protocol startup message, which is always the first
 * message on the wire after the client sends START_REPLICATION.
 *
 * It confirms to the client that we could apply requested options, and
 * tells the client our capabilities.
 *
 * The message is a series of null-terminated strings, alternating keys
 * and values.
 *
 * See the protocol docs for details.
 *
 * The return value is a null-terminated char* palloc'd in the current
 * memory context and the length of that string that is valid. The
 * caller should pfree the result after use.
 *
 * This is a bit less efficient than direct pq_sendblah calls, but
 * separates config handling from the protocol implementation, and
 * it's not like startup msg performance matters much.
 */
void
prepare_startup_message(PGLogicalOutputData *data, char **msg, int *len)
{
	StringInfoData si;

	initStringInfo(&si);

	append_startup_msg_s(&si, "max_proto_version", "1");
	append_startup_msg_s(&si, "min_proto_version", "1");

	/* We don't support understand column types yet */
	append_startup_msg_b(&si, "coltypes", false);

	/* Info about our Pg host */
	append_startup_msg_i(&si, "pg_version_num", PG_VERSION_NUM);
	append_startup_msg_s(&si, "pg_version", PG_VERSION);
	append_startup_msg_i(&si, "pg_catversion", CATALOG_VERSION_NO);

	append_startup_msg_s(&si, "encoding", GetDatabaseEncodingName());

	append_startup_msg_b(&si, "forward_changesets",
			data->forward_changesets);
	append_startup_msg_b(&si, "forward_changeset_origins",
			data->forward_changeset_origins);

	/* binary options enabled */
	append_startup_msg_b(&si, "binary.binary_basetypes",
			data->allow_binary_protocol);
	append_startup_msg_b(&si, "binary.sendrecv_basetypes",
			data->allow_sendrecv_protocol);

	/* Binary format characteristics of server */
	append_startup_msg_i(&si, "binary.sizeof_int", sizeof(int));
	append_startup_msg_i(&si, "binary.sizeof_long", sizeof(long));
	append_startup_msg_i(&si, "binary.sizeof_datum", sizeof(Datum));
	append_startup_msg_i(&si, "binary.maxalign", MAXIMUM_ALIGNOF);
	append_startup_msg_b(&si, "binary.bigendian", server_bigendian());
	append_startup_msg_b(&si, "binary.float4_byval", server_float4_byval());
	append_startup_msg_b(&si, "binary.float8_byval", server_float8_byval());
	append_startup_msg_b(&si, "binary.integer_datetimes", server_integer_datetimes());
	/* We don't know how to send in anything except our host's format */
	append_startup_msg_i(&si, "binary.sendrecv_pg_version",
			PG_VERSION_NUM/100);


	/*
	 * Confirm that we've enabled the requested hook function.
	 */
	append_startup_msg_b(&si, "hooks.table_filter_enabled",
			data->table_filter != NULL);

	/*
	 * TODO: Should provide a hook to emit startup parameters.
	 */

	*msg = si.data;
	*len = si.len;
}
