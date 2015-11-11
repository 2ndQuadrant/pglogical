/*-------------------------------------------------------------------------
 *
 * pglogical_proto.c
 * 		pglogical protocol functions
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_proto.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"

#include "pglogical_output.h"
#include "pglogical_proto_json.h"

#include "access/sysattr.h"
#include "access/tuptoaster.h"
#include "access/xact.h"

#include "catalog/catversion.h"
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

#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"


/*
 * Write BEGIN to the output stream.
 */
void
pglogical_json_write_begin(StringInfo out, ReorderBufferTXN *txn)
{
	/* TODO: needs the rest of the message data */
	appendStringInfoString(out, "{\"action\":\"B\"}");
}

/*
 * Write COMMIT to the output stream.
 */
void
pglogical_json_write_commit(StringInfo out, ReorderBufferTXN *txn,
						XLogRecPtr commit_lsn)
{
	/* TODO: needs the rest of the message data */
	appendStringInfoString(out, "{\"action\":\"C\"}");
}

/*
 * Write a tuple to the outputstream, in the most efficient format possible.
 */
static void
json_write_tuple(StringInfo out, Relation rel, HeapTuple tuple)
{
	TupleDesc	desc;
    Datum		tupdatum,
				json;

	desc = RelationGetDescr(rel);
    tupdatum = heap_copy_tuple_as_datum(tuple, desc);
    json = DirectFunctionCall1(row_to_json, tupdatum);

    appendStringInfoString(out, TextDatumGetCString(json));
}

/*
 * Write change.
 *
 * Generic function handling DML changes.
 */
static void
pglogical_json_write_change(StringInfo out, const char *change, Relation rel,
							HeapTuple oldtuple, HeapTuple newtuple)
{
	/* TODO: needs the rest of the message data */
	appendStringInfo(out, "{\"action\":\"%s\",\"relation\":[\"%s\",\"%s\"]",
					 change,
					 get_namespace_name(RelationGetNamespace(rel)),
					 RelationGetRelationName(rel));

	if (oldtuple)
	{
		appendStringInfoString(out, ",\"oldtuple\":");
		json_write_tuple(out, rel, oldtuple);
	}
	if (newtuple)
	{
		appendStringInfoString(out, ",\"newtuple\":");
		json_write_tuple(out, rel, newtuple);
	}
	appendStringInfoString(out, "}");
}

/*
 * Write INSERT to the output stream.
 */
void
pglogical_json_write_insert(StringInfo out, PGLogicalOutputData *data,
							Relation rel, HeapTuple newtuple)
{
	pglogical_json_write_change(out, "I", rel, NULL, newtuple);
}

/*
 * Write UPDATE to the output stream.
 */
void
pglogical_json_write_update(StringInfo out, PGLogicalOutputData *data,
							Relation rel, HeapTuple oldtuple,
							HeapTuple newtuple)
{
	pglogical_json_write_change(out, "U", rel, oldtuple, newtuple);
}

/*
 * Write DELETE to the output stream.
 */
void
pglogical_json_write_delete(StringInfo out, PGLogicalOutputData *data,
							Relation rel, HeapTuple oldtuple)
{
	pglogical_json_write_change(out, "D", rel, oldtuple, NULL);
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
			appendStringInfoString(out, ", ");
		appendStringInfo(out, "\"%s\": \"%s\"", param->defname, strVal(param->arg));
	}
	appendStringInfoString(out, "}}");
}
