/*-------------------------------------------------------------------------
 *
 * pglogical_proto_json.c
 * 		pglogical protocol functions for json support
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_proto_json.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "replication/reorderbuffer.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"

#include "pglogical_output_plugin.h"
#include "pglogical_proto_json.h"

#ifdef HAVE_REPLICATION_ORIGINS
#include "replication/origin.h"
#endif

static void
json_write_tuple(StringInfo out, Relation rel, HeapTuple tuple,
				 Bitmapset *att_list);

/*
 * Write BEGIN to the output stream.
 */
void
pglogical_json_write_begin(StringInfo out, PGLogicalOutputData *data, ReorderBufferTXN *txn)
{
	appendStringInfoChar(out, '{');
	appendStringInfoString(out, "\"action\":\"B\"");
	appendStringInfo(out, ", \"has_catalog_changes\":\"%c\"",
		rbtxn_has_catalog_changes(txn) ? 't' : 'f');
#ifdef HAVE_REPLICATION_ORIGINS
	if (txn->origin_id != InvalidRepOriginId)
		appendStringInfo(out, ", \"origin_id\":\"%u\"", txn->origin_id);
#endif
	if (!data->client_no_txinfo)
	{
		appendStringInfo(out, ", \"xid\":\"%u\"", txn->xid);
		appendStringInfo(out, ", \"first_lsn\":\"%X/%X\"",
			(uint32)(txn->first_lsn >> 32), (uint32)(txn->first_lsn));
#ifdef HAVE_REPLICATION_ORIGINS
		appendStringInfo(out, ", \"origin_lsn\":\"%X/%X\"",
			(uint32)(txn->origin_lsn >> 32), (uint32)(txn->origin_lsn));
#endif
		if (txn->commit_time != 0)
		appendStringInfo(out, ", \"commit_time\":\"%s\"",
			timestamptz_to_str(txn->commit_time));
	}
	appendStringInfoChar(out, '}');
}

/*
 * Write COMMIT to the output stream.
 */
void
pglogical_json_write_commit(StringInfo out, PGLogicalOutputData *data, ReorderBufferTXN *txn,
						XLogRecPtr commit_lsn)
{
	appendStringInfoChar(out, '{');
	appendStringInfoString(out, "\"action\":\"C\"");
	if (!data->client_no_txinfo)
	{
		appendStringInfo(out, ", \"final_lsn\":\"%X/%X\"",
			(uint32)(txn->final_lsn >> 32), (uint32)(txn->final_lsn));
		appendStringInfo(out, ", \"end_lsn\":\"%X/%X\"",
			(uint32)(txn->end_lsn >> 32), (uint32)(txn->end_lsn));
	}
	appendStringInfoChar(out, '}');
}


/*
 * Write change.
 *
 * Generic function handling DML changes.
 */
static void
pglogical_json_write_change(StringInfo out, const char *change, Relation rel,
							HeapTuple oldtuple, HeapTuple newtuple,
							Bitmapset *att_list)
{
	appendStringInfoChar(out, '{');
	appendStringInfo(out, "\"action\":\"%s\",\"relation\":[\"%s\",\"%s\"]",
					 change,
					 get_namespace_name(RelationGetNamespace(rel)),
					 RelationGetRelationName(rel));

	if (oldtuple)
	{
		appendStringInfoString(out, ",\"oldtuple\":");
		json_write_tuple(out, rel, oldtuple, att_list);
	}
	if (newtuple)
	{
		appendStringInfoString(out, ",\"newtuple\":");
		json_write_tuple(out, rel, newtuple, att_list);
	}
	appendStringInfoChar(out, '}');
}

/*
 * Write INSERT to the output stream.
 */
void
pglogical_json_write_insert(StringInfo out, PGLogicalOutputData *data,
							Relation rel, HeapTuple newtuple,
							Bitmapset *att_list)
{
	pglogical_json_write_change(out, "I", rel, NULL, newtuple, att_list);
}

/*
 * Write UPDATE to the output stream.
 */
void
pglogical_json_write_update(StringInfo out, PGLogicalOutputData *data,
							Relation rel, HeapTuple oldtuple,
							HeapTuple newtuple, Bitmapset *att_list)
{
	pglogical_json_write_change(out, "U", rel, oldtuple, newtuple, att_list);
}

/*
 * Write DELETE to the output stream.
 */
void
pglogical_json_write_delete(StringInfo out, PGLogicalOutputData *data,
							Relation rel, HeapTuple oldtuple,
							Bitmapset *att_list)
{
	pglogical_json_write_change(out, "D", rel, oldtuple, NULL, att_list);
}

/*
 * The startup message should be constructed as a json object, one
 * key/value per DefElem list member.
 */
void
json_write_startup_message(StringInfo out, List *msg)
{
	ListCell *lc;
	bool first = true;

	appendStringInfoString(out, "{\"action\":\"S\", \"params\": {");
	foreach (lc, msg)
	{
		DefElem *param = (DefElem*)lfirst(lc);
		Assert(IsA(param->arg, String) && strVal(param->arg) != NULL);
		if (first)
			first = false;
		else
			appendStringInfoChar(out, ',');
		escape_json(out, param->defname);
		appendStringInfoChar(out, ':');
		escape_json(out, strVal(param->arg));
	}
	appendStringInfoString(out, "}}");
}


/*
 * Functions taken from json.c
 *
 * Current as of commit 272adf4f9cd67df323ae57ff3dee238b649d3b73
 */
#include "parser/parse_coerce.h"

#include "utils/date.h"
#include "utils/datetime.h"

#if PG_VERSION_NUM >= 130000
#include "common/jsonapi.h"
#else
#include "utils/jsonapi.h"
#endif

/*
 * Determine how we want to print values of a given type in datum_to_json.
 *
 * Given the datatype OID, return its JsonTypeCategory, as well as the type's
 * output function OID.  If the returned category is JSONTYPE_CAST, we
 * return the OID of the type->JSON cast function instead.
 */
typedef enum					/* type categories for datum_to_json */
{
	JSONTYPE_NULL,				/* null, so we didn't bother to identify */
	JSONTYPE_BOOL,				/* boolean (built-in types only) */
	JSONTYPE_NUMERIC,			/* numeric (ditto) */
	JSONTYPE_DATE,				/* we use special formatting for datetimes */
	JSONTYPE_TIMESTAMP,
	JSONTYPE_TIMESTAMPTZ,
	JSONTYPE_JSON,				/* JSON itself (and JSONB) */
	JSONTYPE_ARRAY,				/* array */
	JSONTYPE_COMPOSITE,			/* composite */
	JSONTYPE_CAST,				/* something with an explicit cast to JSON */
	JSONTYPE_OTHER				/* all else */
} JsonTypeCategory;

static void composite_to_json(Datum composite, StringInfo result,
				  bool use_line_feeds);
static void array_dim_to_json(StringInfo result, int dim, int ndims, int *dims,
				  Datum *vals, bool *nulls, int *valcount,
				  JsonTypeCategory tcategory, Oid outfuncoid,
				  bool use_line_feeds);
static void array_to_json_internal(Datum array, StringInfo result,
					   bool use_line_feeds);
static void json_categorize_type(Oid typoid,
					 JsonTypeCategory *tcategory,
					 Oid *outfuncoid);

static void
json_categorize_type(Oid typoid,
					 JsonTypeCategory *tcategory,
					 Oid *outfuncoid)
{
	bool		typisvarlena;

	/* Look through any domain */
	typoid = getBaseType(typoid);

	*outfuncoid = InvalidOid;

	/*
	 * We need to get the output function for everything except date and
	 * timestamp types, array and composite types, booleans, and non-builtin
	 * types where there's a cast to json.
	 */

	switch (typoid)
	{
		case BOOLOID:
			*tcategory = JSONTYPE_BOOL;
			break;

		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
			*tcategory = JSONTYPE_NUMERIC;
			break;

		case DATEOID:
			*tcategory = JSONTYPE_DATE;
			break;

		case TIMESTAMPOID:
			*tcategory = JSONTYPE_TIMESTAMP;
			break;

		case TIMESTAMPTZOID:
			*tcategory = JSONTYPE_TIMESTAMPTZ;
			break;

		case JSONOID:
		case JSONBOID:
			getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
			*tcategory = JSONTYPE_JSON;
			break;

		default:
			/* Check for arrays and composites */
			if (OidIsValid(get_element_type(typoid)) || typoid == ANYARRAYOID
				|| typoid == RECORDARRAYOID)
				*tcategory = JSONTYPE_ARRAY;
			else if (type_is_rowtype(typoid)) /* includes RECORDOID */
				*tcategory = JSONTYPE_COMPOSITE;
			else
			{
				/* It's probably the general case ... */
				*tcategory = JSONTYPE_OTHER;
				/* but let's look for a cast to json, if it's not built-in */
				if (typoid >= FirstNormalObjectId)
				{
					Oid			castfunc;
					CoercionPathType ctype;

					ctype = find_coercion_pathway(JSONOID, typoid,
												  COERCION_EXPLICIT,
												  &castfunc);
					if (ctype == COERCION_PATH_FUNC && OidIsValid(castfunc))
					{
						*tcategory = JSONTYPE_CAST;
						*outfuncoid = castfunc;
					}
					else
					{
						/* non builtin type with no cast */
						getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
					}
				}
				else
				{
					/* any other builtin type */
					getTypeOutputInfo(typoid, outfuncoid, &typisvarlena);
				}
			}
			break;
	}
}

/*
 * Turn a Datum into JSON text, appending the string to "result".
 *
 * tcategory and outfuncoid are from a previous call to json_categorize_type,
 * except that if is_null is true then they can be invalid.
 *
 * If key_scalar is true, the value is being printed as a key, so insist
 * it's of an acceptable type, and force it to be quoted.
 */
static void
datum_to_json(Datum val, bool is_null, StringInfo result,
			  JsonTypeCategory tcategory, Oid outfuncoid,
			  bool key_scalar)
{
	char	   *outputstr;
	text	   *jsontext;

	check_stack_depth();

	/* callers are expected to ensure that null keys are not passed in */
	Assert(!(key_scalar && is_null));

	if (is_null)
	{
		appendStringInfoString(result, "null");
		return;
	}

	if (key_scalar &&
		(tcategory == JSONTYPE_ARRAY ||
		 tcategory == JSONTYPE_COMPOSITE ||
		 tcategory == JSONTYPE_JSON ||
		 tcategory == JSONTYPE_CAST))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		 errmsg("key value must be scalar, not array, composite, or json")));

	switch (tcategory)
	{
		case JSONTYPE_ARRAY:
			array_to_json_internal(val, result, false);
			break;
		case JSONTYPE_COMPOSITE:
			composite_to_json(val, result, false);
			break;
		case JSONTYPE_BOOL:
			outputstr = DatumGetBool(val) ? "true" : "false";
			if (key_scalar)
				escape_json(result, outputstr);
			else
				appendStringInfoString(result, outputstr);
			break;
		case JSONTYPE_NUMERIC:
			outputstr = OidOutputFunctionCall(outfuncoid, val);

			/*
			 * Don't call escape_json for a non-key if it's a valid JSON
			 * number.
			 */
			if (!key_scalar && IsValidJsonNumber(outputstr, strlen(outputstr)))
				appendStringInfoString(result, outputstr);
			else
				escape_json(result, outputstr);
			pfree(outputstr);
			break;
		case JSONTYPE_DATE:
			{
				DateADT		date;
				struct pg_tm tm;
				char		buf[MAXDATELEN + 1];

				date = DatumGetDateADT(val);
				/* Same as date_out(), but forcing DateStyle */
				if (DATE_NOT_FINITE(date))
					EncodeSpecialDate(date, buf);
				else
				{
					j2date(date + POSTGRES_EPOCH_JDATE,
						   &(tm.tm_year), &(tm.tm_mon), &(tm.tm_mday));
					EncodeDateOnly(&tm, USE_XSD_DATES, buf);
				}
				appendStringInfo(result, "\"%s\"", buf);
			}
			break;
		case JSONTYPE_TIMESTAMP:
			{
				Timestamp	timestamp;
				struct pg_tm tm;
				fsec_t		fsec;
				char		buf[MAXDATELEN + 1];

				timestamp = DatumGetTimestamp(val);
				/* Same as timestamp_out(), but forcing DateStyle */
				if (TIMESTAMP_NOT_FINITE(timestamp))
					EncodeSpecialTimestamp(timestamp, buf);
				else if (timestamp2tm(timestamp, NULL, &tm, &fsec, NULL, NULL) == 0)
					EncodeDateTime(&tm, fsec, false, 0, NULL, USE_XSD_DATES, buf);
				else
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range")));
				appendStringInfo(result, "\"%s\"", buf);
			}
			break;
		case JSONTYPE_TIMESTAMPTZ:
			{
				TimestampTz timestamp;
				struct pg_tm tm;
				int			tz;
				fsec_t		fsec;
				const char *tzn = NULL;
				char		buf[MAXDATELEN + 1];

				timestamp = DatumGetTimestampTz(val);
				/* Same as timestamptz_out(), but forcing DateStyle */
				if (TIMESTAMP_NOT_FINITE(timestamp))
					EncodeSpecialTimestamp(timestamp, buf);
				else if (timestamp2tm(timestamp, &tz, &tm, &fsec, &tzn, NULL) == 0)
					EncodeDateTime(&tm, fsec, true, tz, tzn, USE_XSD_DATES, buf);
				else
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range")));
				appendStringInfo(result, "\"%s\"", buf);
			}
			break;
		case JSONTYPE_JSON:
			/* JSON and JSONB output will already be escaped */
			outputstr = OidOutputFunctionCall(outfuncoid, val);
			appendStringInfoString(result, outputstr);
			pfree(outputstr);
			break;
		case JSONTYPE_CAST:
			/* outfuncoid refers to a cast function, not an output function */
			jsontext = DatumGetTextP(OidFunctionCall1(outfuncoid, val));
			outputstr = text_to_cstring(jsontext);
			appendStringInfoString(result, outputstr);
			pfree(outputstr);
			pfree(jsontext);
			break;
		default:
			outputstr = OidOutputFunctionCall(outfuncoid, val);
			escape_json(result, outputstr);
			pfree(outputstr);
			break;
	}
}

/*
 * Process a single dimension of an array.
 * If it's the innermost dimension, output the values, otherwise call
 * ourselves recursively to process the next dimension.
 */
static void
array_dim_to_json(StringInfo result, int dim, int ndims, int *dims, Datum *vals,
				  bool *nulls, int *valcount, JsonTypeCategory tcategory,
				  Oid outfuncoid, bool use_line_feeds)
{
	int			i;
	const char *sep;

	Assert(dim < ndims);

	sep = use_line_feeds ? ",\n " : ",";

	appendStringInfoChar(result, '[');

	for (i = 1; i <= dims[dim]; i++)
	{
		if (i > 1)
			appendStringInfoString(result, sep);

		if (dim + 1 == ndims)
		{
			datum_to_json(vals[*valcount], nulls[*valcount], result, tcategory,
						  outfuncoid, false);
			(*valcount)++;
		}
		else
		{
			/*
			 * Do we want line feeds on inner dimensions of arrays? For now
			 * we'll say no.
			 */
			array_dim_to_json(result, dim + 1, ndims, dims, vals, nulls,
							  valcount, tcategory, outfuncoid, false);
		}
	}

	appendStringInfoChar(result, ']');
}

/*
 * Turn an array into JSON.
 */
static void
array_to_json_internal(Datum array, StringInfo result, bool use_line_feeds)
{
	ArrayType  *v = DatumGetArrayTypeP(array);
	Oid			element_type = ARR_ELEMTYPE(v);
	int		   *dim;
	int			ndim;
	int			nitems;
	int			count = 0;
	Datum	   *elements;
	bool	   *nulls;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	JsonTypeCategory tcategory;
	Oid			outfuncoid;

	ndim = ARR_NDIM(v);
	dim = ARR_DIMS(v);
	nitems = ArrayGetNItems(ndim, dim);

	if (nitems <= 0)
	{
		appendStringInfoString(result, "[]");
		return;
	}

	get_typlenbyvalalign(element_type,
						 &typlen, &typbyval, &typalign);

	json_categorize_type(element_type,
						 &tcategory, &outfuncoid);

	deconstruct_array(v, element_type, typlen, typbyval,
					  typalign, &elements, &nulls,
					  &nitems);

	array_dim_to_json(result, 0, ndim, dim, elements, nulls, &count, tcategory,
					  outfuncoid, use_line_feeds);

	pfree(elements);
	pfree(nulls);
}

/*
 * Turn a composite / record into JSON.
 */
static void
composite_to_json(Datum composite, StringInfo result, bool use_line_feeds)
{
	HeapTupleHeader td;
	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupdesc;
	HeapTupleData tmptup,
			   *tuple;
	int			i;
	bool		needsep = false;
	const char *sep;

	sep = use_line_feeds ? ",\n " : ",";

	td = DatumGetHeapTupleHeader(composite);

	/* Extract rowtype info and find a tupdesc */
	tupType = HeapTupleHeaderGetTypeId(td);
	tupTypmod = HeapTupleHeaderGetTypMod(td);
	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

	/* Build a temporary HeapTuple control structure */
	tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
	tmptup.t_data = td;
	tuple = &tmptup;

	appendStringInfoChar(result, '{');

	for (i = 0; i < tupdesc->natts; i++)
	{
		Datum		val;
		bool		isnull;
		char	   *attname;
		JsonTypeCategory tcategory;
		Oid			outfuncoid;

		if (TupleDescAttr(tupdesc,i)->attisdropped)
			continue;

		if (needsep)
			appendStringInfoString(result, sep);
		needsep = true;

		attname = NameStr(TupleDescAttr(tupdesc,i)->attname);
		escape_json(result, attname);
		appendStringInfoChar(result, ':');

		val = heap_getattr(tuple, i + 1, tupdesc, &isnull);

		if (isnull)
		{
			tcategory = JSONTYPE_NULL;
			outfuncoid = InvalidOid;
		}
		else
			json_categorize_type(TupleDescAttr(tupdesc,i)->atttypid,
								 &tcategory, &outfuncoid);

		datum_to_json(val, isnull, result, tcategory, outfuncoid, false);
	}

	appendStringInfoChar(result, '}');
	ReleaseTupleDesc(tupdesc);
}


/*
 * And finally the function that uses the above json functions.
 */

/*
 * Write a tuple to the outputstream, in the most efficient format possible.
 */
static void
json_write_tuple(StringInfo out, Relation rel, HeapTuple tuple,
				 Bitmapset *att_list)
{
	TupleDesc	tupdesc;
	int			i;
	bool		needsep = false;
	Datum		values[MaxTupleAttributeNumber];
	bool		isnull[MaxTupleAttributeNumber];

	tupdesc = RelationGetDescr(rel);
	appendStringInfoChar(out, '{');

	heap_deform_tuple(tuple, tupdesc, values, isnull);

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc,i);
		JsonTypeCategory tcategory;
		Oid			outfuncoid;

		/* skip dropped columns */
		if (att->attisdropped)
			continue;
		if (att_list &&
			!bms_is_member(att->attnum - FirstLowInvalidHeapAttributeNumber,
						   att_list))
			continue;

		/*
		 * Don't send unchanged toast column as we may not be able to fetch
		 * them.
		 */
		if (!isnull[i] && att->attlen == -1 &&
			VARATT_IS_EXTERNAL_ONDISK(values[i]))
			continue;

		if (needsep)
			appendStringInfoChar(out, ',');
		needsep = true;

		escape_json(out, NameStr(att->attname));
		appendStringInfoChar(out, ':');

		if (isnull[i])
		{
			tcategory = JSONTYPE_NULL;
			outfuncoid = InvalidOid;
		}
		else
			json_categorize_type(att->atttypid, &tcategory, &outfuncoid);

		datum_to_json(values[i], isnull[i], out, tcategory, outfuncoid, false);
	}

	appendStringInfoChar(out, '}');
}
