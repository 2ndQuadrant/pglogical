/*-------------------------------------------------------------------------
 *
 * pglogical_manager.c
 * 		pglogical worker for managing apply workers in a database
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_manager.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"

#include "catalog/indexing.h"

#include "commands/sequence.h"

#include "nodes/makefuncs.h"

#include "replication/reorderbuffer.h"

#include "utils/fmgroids.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "pglogical.h"
#include "pglogical_queue.h"
#include "pglogical_repset.h"

#define CATALOG_SEQUENCE_STATE			"sequence_state"

#define SEQUENCE_REPLICATION_MIN_CACHE	1000
#define SEQUENCE_REPLICATION_MAX_CACHE	1000000

typedef struct SeqStateTuple {
	Oid		seqoid;
	int32	cache_size;
	int64	last_value;
} SeqStateTuple;

#define Natts_sequence_state			3
#define Anum_sequence_state_seqoid		1
#define Anum_sequence_state_cache_size	2
#define Anum_sequence_state_last_value	3


/* Get last value of individual sequence. */
int64
sequence_get_last_value(Oid seqoid)
{
	Relation            seqrel;
	SysScanDesc		scan;
	HeapTuple			tup;
	int64				last_value;
	Form_pg_sequence	seq;

	seqrel = table_open(seqoid, AccessShareLock);
	scan = systable_beginscan(seqrel, 0, false, NULL, 0, NULL);
	tup = systable_getnext(scan);
	Assert(HeapTupleIsValid(tup));
	seq = (Form_pg_sequence) GETSTRUCT(tup);
	last_value = seq->last_value;
	systable_endscan(scan);
	table_close(seqrel, AccessShareLock);

	return last_value;
}


/*
 * Process sequence updates.
 */
bool
synchronize_sequences(void)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	PGLogicalLocalNode	   *local_node;
	bool			ret = true;

	StartTransactionCommand();

	local_node = get_local_node(false, true);

	if (!local_node)
	{
		AbortCurrentTransaction();
		return ret;
	}

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_SEQUENCE_STATE, -1);

	rel = table_openrv(rv, RowExclusiveLock);
	scan = systable_beginscan(rel, 0, true, NULL, 0, NULL);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		SeqStateTuple  *oldseq = (SeqStateTuple *) GETSTRUCT(tuple);
		SeqStateTuple  *newseq;
		int64			last_value;
		HeapTuple		newtup;
		List		   *repsets;
		List		   *repset_names;
		ListCell	   *lc;
		char		   *nspname;
		char		   *relname;
		StringInfoData	json;

		CHECK_FOR_INTERRUPTS();

		last_value = sequence_get_last_value(oldseq->seqoid);

		/* Not enough of the sequence was consumed yet for us to care. */
		if (oldseq->last_value >= last_value + SEQUENCE_REPLICATION_MIN_CACHE / 2)
			continue;

		newtup = heap_copytuple(tuple);
		newseq = (SeqStateTuple *) GETSTRUCT(newtup);

		/* Consumed more than half of cache of the sequence. */
		if (newseq->last_value + newseq->cache_size / 2 < last_value)
			ret = false;

		/* The sequence is consumed too fast, increase the buffer cache. */
		if (newseq->last_value + newseq->cache_size <= last_value)
			newseq->cache_size = Min(SEQUENCE_REPLICATION_MAX_CACHE,
									 newseq->cache_size * 2);

		newseq->last_value = last_value + newseq->cache_size;
		CatalogTupleUpdate(rel, &tuple->t_self, newtup);

		repsets = get_seq_replication_sets(local_node->node->id,
										   oldseq->seqoid);
		repset_names = NIL;
		foreach (lc, repsets)
		{
			PGLogicalRepSet	    *repset = (PGLogicalRepSet *) lfirst(lc);
			repset_names = lappend(repset_names, pstrdup(repset->name));
		}

		nspname = get_namespace_name(get_rel_namespace(oldseq->seqoid));
		relname = get_rel_name(oldseq->seqoid);

		initStringInfo(&json);
		appendStringInfoString(&json, "{\"schema_name\": ");
		escape_json(&json, nspname);
		appendStringInfoString(&json, ",\"sequence_name\": ");
		escape_json(&json, relname);
		appendStringInfo(&json, ",\"last_value\": \""INT64_FORMAT"\"",
						 newseq->last_value);
		appendStringInfo(&json, "}");

		queue_message(repset_names, GetUserId(),
					  QUEUE_COMMAND_TYPE_SEQUENCE, json.data);
	}

	/* Cleanup */
	systable_endscan(scan);
	table_close(rel, NoLock);

	CommitTransactionCommand();

	return ret;
}

/*
 * Process sequence updates.
 */
void
synchronize_sequence(Oid seqoid)
{
	RangeVar	   *rv;
	Relation		rel;
	Relation		seqrel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];
	SeqStateTuple  *newseq;
	int64			last_value;
	HeapTuple		newtup;
	List		   *repsets;
	List		   *repset_names;
	ListCell	   *lc;
	char		   *nspname;
	char		   *relname;
	StringInfoData	json;
	PGLogicalLocalNode	   *local_node = get_local_node(true, false);

	/* Check if the oid points to actual sequence. */
	seqrel = table_open(seqoid, AccessShareLock);

	if (seqrel->rd_rel->relkind != RELKIND_SEQUENCE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a sequence",
						RelationGetRelationName(seqrel))));

	/* Now search for it in our tracking table. */
	rv = makeRangeVar(EXTENSION_NAME, CATALOG_SEQUENCE_STATE, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_sequence_state_seqoid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(seqoid));
	scan = systable_beginscan(rel, 0, true, NULL, 1, key);

	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("\"%s\" is not a replicated sequence",
						RelationGetRelationName(seqrel))));


	newtup = heap_copytuple(tuple);
	newseq = (SeqStateTuple *) GETSTRUCT(newtup);

	last_value = sequence_get_last_value(seqoid);

	newseq->last_value = last_value + newseq->cache_size;
	CatalogTupleUpdate(rel, &tuple->t_self, newtup);

	repsets = get_seq_replication_sets(local_node->node->id, seqoid);
	repset_names = NIL;
	foreach (lc, repsets)
	{
		PGLogicalRepSet	    *repset = (PGLogicalRepSet *) lfirst(lc);
		repset_names = lappend(repset_names, pstrdup(repset->name));
	}

	nspname = get_namespace_name(RelationGetNamespace(seqrel));
	relname = RelationGetRelationName(seqrel);

	initStringInfo(&json);
	appendStringInfoString(&json, "{\"schema_name\": ");
	escape_json(&json, nspname);
	appendStringInfoString(&json, ",\"sequence_name\": ");
	escape_json(&json, relname);
	appendStringInfo(&json, ",\"last_value\": \""INT64_FORMAT"\"",
					 newseq->last_value);
	appendStringInfo(&json, "}");

	queue_message(repset_names, GetUserId(),
				  QUEUE_COMMAND_TYPE_SEQUENCE, json.data);

	/* Cleanup */
	systable_endscan(scan);
	table_close(rel, NoLock);
	table_close(seqrel, AccessShareLock);
}

/*
 * Makes sure there is sequence state record for given sequence.
 */
void
pglogical_create_sequence_state_record(Oid seqoid)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_SEQUENCE_STATE, -1);

	rel = table_openrv(rv, RowExclusiveLock);

	/* Check if the state record already exists. */
	ScanKeyInit(&key[0],
				Anum_sequence_state_seqoid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(seqoid));
	scan = systable_beginscan(rel, 0, true, NULL, 1, key);

	tuple = systable_getnext(scan);

	/* And if it doesn't insert it. */
	if (!HeapTupleIsValid(tuple))
	{
		Datum		values[Natts_sequence_state];
		bool		nulls[Natts_sequence_state];
		TupleDesc	tupDesc = RelationGetDescr(rel);

		/* Form a tuple. */
		memset(nulls, false, sizeof(nulls));

		values[Anum_sequence_state_seqoid - 1] = ObjectIdGetDatum(seqoid);
		values[Anum_sequence_state_cache_size - 1] =
			Int32GetDatum(SEQUENCE_REPLICATION_MIN_CACHE);
		values[Anum_sequence_state_last_value - 1] =
			Int64GetDatum(sequence_get_last_value(seqoid));

		tuple = heap_form_tuple(tupDesc, values, nulls);

		/* Insert the tuple to the catalog. */
		CatalogTupleInsert(rel, tuple);
	}

	/* Cleanup. */
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	CommandCounterIncrement();
}

/*
 * Makes sure there isn't sequence state record for given sequence.
 */
void
pglogical_drop_sequence_state_record(Oid seqoid)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_SEQUENCE_STATE, -1);

	rel = table_openrv(rv, RowExclusiveLock);

	/* Check if the state record already exists. */
	ScanKeyInit(&key[0],
				Anum_sequence_state_seqoid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(seqoid));
	scan = systable_beginscan(rel, 0, true, NULL, 1, key);

	tuple = systable_getnext(scan);

	if (HeapTupleIsValid(tuple))
		simple_heap_delete(rel, &tuple->t_self);

	/* Cleanup. */
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	CommandCounterIncrement();
}
