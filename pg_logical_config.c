/*-------------------------------------------------------------------------
 *
 * pg_logical_config.c
 *		  Logical Replication output plugin
 *
 * Copyright (c) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pg_logical_config.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_logical_output.h"

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
static Datum get_param(List *options, const char *name, bool missing_ok,
					   bool null_ok, PGLogicalOutputParamType type,
					   bool *found);
static bool parse_param_bool(DefElem *elem);
static uint32 parse_param_uint32(DefElem *elem);

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
		val = get_param(options, "max_proto_version", false, false,
						OUTPUT_PARAM_TYPE_UINT32, &found);
		data->client_max_proto_version = DatumGetUInt32(val);

		val = get_param(options, "min_proto_version", false, false,
						OUTPUT_PARAM_TYPE_UINT32, &found);
		data->client_min_proto_version = DatumGetUInt32(val);


		/* now process regular parameters */
		val = get_param(options, "binary.bigendian", true, false,
				OUTPUT_PARAM_TYPE_BOOL,
				&data->client_binary_bigendian_set);
		if (data->client_binary_bigendian_set)
			data->client_binary_bigendian = DatumGetBool(val);

		val = get_param(options, "binary.sizeof_datum", true, false,
				OUTPUT_PARAM_TYPE_UINT32,
				&data->client_binary_sizeofdatum_set);
		if (data->client_binary_sizeofdatum_set)
			data->client_binary_sizeofdatum = DatumGetUInt32(val);

		val = get_param(options, "binary.sizeof_int", true, false,
				OUTPUT_PARAM_TYPE_UINT32,
				&data->client_binary_sizeofint_set);
		if (data->client_binary_sizeofint_set)
			data->client_binary_sizeofint = DatumGetUInt32(val);

		val = get_param(options, "binary.sizeof_long", true, false,
				OUTPUT_PARAM_TYPE_UINT32,
				&data->client_binary_sizeoflong_set);
		if (data->client_binary_sizeoflong_set)
			data->client_binary_sizeoflong = DatumGetUInt32(val);

		val = get_param(options, "binary.float4_byval", true, false,
				OUTPUT_PARAM_TYPE_BOOL,
				&data->client_binary_float4byval_set);
		if (data->client_binary_float4byval_set)
			data->client_binary_float4byval = DatumGetBool(val);

		val = get_param(options, "binary.float8_byval", true, false,
				OUTPUT_PARAM_TYPE_BOOL,
				&data->client_binary_float8byval_set);
		if (data->client_binary_float8byval_set)
			data->client_binary_float8byval = DatumGetBool(val);

		val = get_param(options, "binary.integer_datetimes", true, false,
				OUTPUT_PARAM_TYPE_BOOL,
				&data->client_binary_intdatetimes_set);
		if (data->client_binary_intdatetimes_set)
			data->client_binary_intdatetimes = DatumGetBool(val);

		val = get_param(options, "expected_encoding", true,
				false, OUTPUT_PARAM_TYPE_STRING, &found);
		data->client_expected_encoding = found ? DatumGetCString(val) : "";

		/*
		 * Check PostgreSQL version, this can be omitted to support clients
		 * other than PostgreSQL.
		 *
		 * Must not be used for binary format compatibility tests, this is
		 * informational only.
		 */
		val = get_param(options, "pg_version", true, false,
						OUTPUT_PARAM_TYPE_UINT32, &found);
		data->client_pg_version = found ? DatumGetUInt32(val) : 0;

		/*
		 * Check to see if the client asked for changeset forwarding
		 *
		 * Note that we cannot support this on 9.4. We'll tell the client
		 * in the startup reply message.
		 */
		val = get_param(options, "forward_changesets", true,
						false, OUTPUT_PARAM_TYPE_BOOL, &found);

		data->forward_changesets = found ? DatumGetBool(val) : false;

		/* check if we want to use binary data representation */
		val = get_param(options, "binary.want_binary_basetypes", true,
						false, OUTPUT_PARAM_TYPE_BOOL, &found);
		data->client_want_binary_basetypes = found ? DatumGetBool(val) : false;

		/* check if we want to use sendrecv data representation */
		val = get_param(options, "binary.want_sendrecv_basetypes", true,
						false, OUTPUT_PARAM_TYPE_BOOL, &found);
		data->client_want_sendrecv_basetypes = found ? DatumGetBool(val) : false;

		val = get_param(options, "binary.basetypes_major_version", true, false,
						OUTPUT_PARAM_TYPE_UINT32, &found);
		data->client_binary_basetypes_major_version = found ? DatumGetUInt32(val) : 0;

		/* Hooks */
		val = get_param(options,
						"hooks.table_change_filter", true, false,
						OUTPUT_PARAM_TYPE_QUALIFIED_NAME, &found);

		data->table_change_filter = found ? (List *) PointerGetDatum(val) : NULL;

		/* Node id */
		val = get_param(options, "node_id", true,
						false, OUTPUT_PARAM_TYPE_STRING, &found);

		if (found)
			data->node_id = found ? DatumGetCString(val) : NULL;
	}

	return params_format;
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
