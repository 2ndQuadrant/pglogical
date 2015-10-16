/*-------------------------------------------------------------------------
 *
 * pglogical_queue.c
 *		pglogical queue and connection catalog manipulation functions
 *
 * TODO: caching
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pglogical_queue.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"

#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"

#include "commands/trigger.h"

#include "miscadmin.h"

#include "nodes/makefuncs.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "pglogical_queue.h"
#include "pglogical.h"

#define CATALOG_QUEUE	"queue"

#define Natts_queue				4
#define Anum_queue_queued_at	1
#define Anum_queue_role			2
#define Anum_queue_message_type	3
#define Anum_queue_message		4

typedef struct QueueTuple
{
	TimestampTz	queued_at;
	NameData	role;
	char		message_type;
/*	json		message;*/
} QueueTuple;

/*
 * Add tuple to the queue table.
 */
void
queue_command(Oid roleoid, char message_type, char *message)
{
	RangeVar   *rv;
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_queue];
	bool		nulls[Natts_queue];
	const char *role = GetUserNameFromId(roleoid, false);
	TimestampTz ts = GetCurrentTimestamp();

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_QUEUE, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* Form a tuple. */
	memset(nulls, false, sizeof(nulls));

	values[Anum_queue_queued_at - 1] = TimestampTzGetDatum(ts);
	values[Anum_queue_role - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(role));
	values[Anum_queue_message_type - 1] = CharGetDatum(message_type);
	values[Anum_queue_message - 1] =
		DirectFunctionCall1(json_in, CStringGetDatum(message));

	tup = heap_form_tuple(tupDesc, values, nulls);

	/* Insert the tuple to the catalog. */
	simple_heap_insert(rel, tup);

	/* Update the indexes. */
	CatalogUpdateIndexes(rel, tup);

	/* Cleanup. */
	heap_freetuple(tup);
	heap_close(rel, RowExclusiveLock);
}

char
message_type_from_queued_tuple(HeapTuple queue_tup)
{
	QueueTuple *q = (QueueTuple *) GETSTRUCT(queue_tup);

	return q->message_type;
}


/*
 * Get individual values from the command queue tuple.
 */
char *
sql_from_queued_tuple(HeapTuple queue_tup, char **role)
{
	QueueTuple *q = (QueueTuple *) GETSTRUCT(queue_tup);
	RangeVar   *rv;
	Relation	rel;
	TupleDesc	tupDesc;
	Datum		datum;
	bool		isnull;
	char	   *message;
	char	   *res;
	Jsonb	   *data;
	JsonbIterator *it;
	JsonbValue	v;
	int			r;

	Assert(q->message_type == QUEUE_COMMAND_TYPE_SQL);

	/* Open relation to get the tuple descriptor. */
	rv = makeRangeVar(EXTENSION_NAME, CATALOG_QUEUE, -1);
	rel = heap_openrv(rv, NoLock);
	tupDesc = RelationGetDescr(rel);

	/* Parse the message. */
	datum = heap_getattr(queue_tup, Anum_queue_message, tupDesc, &isnull);
	if (isnull)
		elog(ERROR, "null message in queued SQL message tuple");

	message = DatumGetCString(DirectFunctionCall1(json_out, datum));
	data = DatumGetJsonb(
		DirectFunctionCall1(jsonb_in, CStringGetDatum(message)));

	if (!JB_ROOT_IS_SCALAR(data))
		elog(ERROR, "malformed message in queued SQL message tuple: root is not scalar");

	it = JsonbIteratorInit(&data->root);
	r = JsonbIteratorNext(&it, &v, false);
	if (r != WJB_BEGIN_ARRAY)
		elog(ERROR, "malformed message in queued SQL message tuple, item type %d expected %d", r, WJB_BEGIN_ARRAY);

	r = JsonbIteratorNext(&it, &v, false);
	if (r != WJB_ELEM)
		elog(ERROR, "malformed message in queued SQL message tuple, item type %d expected %d", r, WJB_ELEM);

	if (v.type != jbvString)
		elog(ERROR, "malformed message in queued SQL message tuple, expected value type %d got %d", jbvString, v.type);

	res = pnstrdup(v.val.string.val, v.val.string.len);

	r = JsonbIteratorNext(&it, &v, false);
	if (r != WJB_END_ARRAY)
		elog(ERROR, "malformed message in queued SQL message tuple, item type %d expected %d", r, WJB_END_ARRAY);

	r = JsonbIteratorNext(&it, &v, false);
	if (r != WJB_DONE)
		elog(ERROR, "malformed message in queued SQL message tuple, item type %d expected %d", r, WJB_DONE);

	/* Read role info if requested. */
	if (role)
		*role = pstrdup(NameStr(q->role));

	/* Close the relation. */
	heap_close(rel, NoLock);

	return res;
}

/*
 * Get oid of our queue table.
 */
Oid
get_queue_table_oid(void)
{
	Oid			nspoid;
	Oid			reloid;

	nspoid = get_namespace_oid(EXTENSION_NAME, false);

	reloid = get_relname_relid(CATALOG_QUEUE, nspoid);

	if (!reloid)
		elog(ERROR, "cache lookup failed for relation %s.%s",
			 EXTENSION_NAME, CATALOG_QUEUE);

	return reloid;
}


/*
 * Create a TRUNCATE trigger for a persistent table and mark
 * it tgisinternal so that it's not dumped by pg_dump.
 *
 * This is basically wrapper around CreateTrigger().
 */
void
create_truncate_trigger(char *schemaname, char *relname)
{
	CreateTrigStmt *tgstmt;
	RangeVar *relrv = makeRangeVar(schemaname, relname, -1);

	tgstmt = makeNode(CreateTrigStmt);
	tgstmt->trigname = "queue_truncate_trigger";
	tgstmt->relation = copyObject(relrv);
	tgstmt->funcname = list_make2(makeString(EXTENSION_NAME), makeString("queue_truncate"));
	tgstmt->args = NIL;
	tgstmt->row = false;
	tgstmt->timing = TRIGGER_TYPE_AFTER;
	tgstmt->events = TRIGGER_TYPE_TRUNCATE;
	tgstmt->columns = NIL;
	tgstmt->whenClause = NULL;
	tgstmt->isconstraint = false;
	tgstmt->deferrable = false;
	tgstmt->initdeferred = false;
	tgstmt->constrrel = NULL;

	(void) CreateTrigger(tgstmt, NULL, InvalidOid, InvalidOid,
						 InvalidOid, InvalidOid, true /* tgisinternal */);

	/* Make the new trigger visible within this session */
	CommandCounterIncrement();
}
