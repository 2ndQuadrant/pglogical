/*-------------------------------------------------------------------------
 *
 * pglogical_queue.c
 *		pglogical queue and connection catalog manipulation functions
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

#define Natts_queue					5
#define Anum_queue_queued_at		1
#define Anum_queue_replication_set	2
#define Anum_queue_role				3
#define Anum_queue_message_type		4
#define Anum_queue_message			5

typedef struct QueueTuple
{
	TimestampTz	queued_at;
	NameData	replication_set;
	NameData	role;
	char		message_type;
/*	json		message;*/
} QueueTuple;

/*
 * Add tuple to the queue table.
 */
void
queue_message(char *replication_set, Oid roleoid, char message_type,
			  char *message)
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
	values[Anum_queue_replication_set - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(replication_set));
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


/*
 * Parse the tuple from the queue table into palloc'd QueuedMessage struct.
 *
 * The caller must have the queue table locked in at least AccessShare mode.
 */
QueuedMessage *
queued_message_from_tuple(HeapTuple queue_tup)
{
	QueueTuple *q = (QueueTuple *) GETSTRUCT(queue_tup);
	RangeVar   *rv;
	Relation	rel;
	TupleDesc	tupDesc;
	bool		isnull;
	Datum		message;
	Jsonb	   *data;
	QueuedMessage *res;

	/* Open relation to get the tuple descriptor. */
	rv = makeRangeVar(EXTENSION_NAME, CATALOG_QUEUE, -1);
	rel = heap_openrv(rv, NoLock);
	tupDesc = RelationGetDescr(rel);

	/* Retriveve the message. */
	message = heap_getattr(queue_tup, Anum_queue_message, tupDesc, &isnull);
	if (isnull)
		elog(ERROR, "null message in queued message tuple");

	/* Parse the json inside the message into Jsonb object. */
	data = DatumGetJsonb(
		DirectFunctionCall1(jsonb_in, DirectFunctionCall1(json_out, message)));

	res = (QueuedMessage *) palloc(sizeof(QueuedMessage));
	res->message = data;

	res->queued_at = q->queued_at;
	res->replication_set = pstrdup(NameStr(q->replication_set));
	res->role = pstrdup(NameStr(q->role));
	res->message_type = q->message_type;

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
