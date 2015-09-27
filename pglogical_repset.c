/*-------------------------------------------------------------------------
 *
 * pglogical_repset.c
 *		pg_logical replication set manipulation functions
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pglogical_repset.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"

#include "catalog/indexing.h"
#include "catalog/pg_type.h"

#include "executor/spi.h"

#include "nodes/makefuncs.h"

#include "replication/reorderbuffer.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "pg_logical_node.h"
#include "pglogical_repset.h"
#include "pglogical.h"

#define CATALOG_REPSETS			"replication_sets"
#define CATALOG_REPSET_TABLES	"replication_set_tables"

typedef struct RepSetTuple
{
	int32		id;
	NameData	name;
	bool		replicate_inserts;
	bool		replicate_updates;
	bool		replicate_deletes;
} RepSetTuple;

#define Anum_repsets_id					1
#define Anum_repsets_name				2
#define Anum_repsets_replicate_inserts	3
#define Anum_repsets_replicate_updates	4
#define Anum_repsets_replicate_deletes	5

#define Anum_repset_tables_setid	1
#define Anum_repset_tables_reloid	2

static HTAB *RepSetRelationHash = NULL;

PGDLLEXPORT Datum pglogical_get_replication_set_tables(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pglogical_get_replication_set_tables);

Datum
pglogical_get_replication_set_tables(PG_FUNCTION_ARGS)
{
	ArrayType	   *arr = PG_GETARG_ARRAYTYPE_P(0);
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcontext;
	int				ret;
	int				i;
	Datum			vals[1] = {PointerGetDatum(arr)};
	Oid				types[1] = {TEXTARRAYOID};

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Run this through SPI. This is not performance critical function so it
	 * does not seem worthwile to implement the join in C.
	 */
	SPI_connect();
	ret = SPI_execute_with_args("SELECT DISTINCT n.nspname, r.relname"
								"  FROM pg_catalog.pg_namespace n,"
								"       pg_catalog.pg_class r,"
								"       pglogical.replication_set_tables t,"
								"       pglogical.replication_sets s"
								" WHERE s.set_name = ANY($1)"
								"   AND s.set_id = t.set_id"
								"   AND r.oid = t.set_relation"
								"   AND n.oid = r.relnamespace",
								1, types, vals, NULL, false, 0);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI error while querying replication sets");

	for (i = 0; i < SPI_processed; i++)
	{
		Datum		values[2];
		bool		nulls[2] = {false, false};

		values[0] = CStringGetTextDatum(SPI_getvalue(SPI_tuptable->vals[i],
													 SPI_tuptable->tupdesc,
													 1));
		values[1] = CStringGetTextDatum(SPI_getvalue(SPI_tuptable->vals[i],
													 SPI_tuptable->tupdesc,
													 2));

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	tuplestore_donestoring(tupstore);
	SPI_finish();

	return (Datum) 0;
}

/*
 * Load the info for specific node.
 */
PGLogicalRepSet *
replication_set_get(int setid)
{
	PGLogicalRepSet    *repset;
	RepSetTuple		   *repsettup;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSETS, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				Anum_repsets_id,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(setid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "replication set %d not found", setid);

	repsettup = (RepSetTuple *) GETSTRUCT(tuple);

	/* Create and fill the replication set struct. */
	repset = (PGLogicalRepSet *) palloc(sizeof(PGLogicalRepSet));
	repset->id = setid;

	repset->name = pstrdup(NameStr(repsettup->name));
	repset->replicate_inserts = repsettup->replicate_inserts;
	repset->replicate_updates = repsettup->replicate_updates;
	repset->replicate_deletes = repsettup->replicate_deletes;

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return repset;
}

static void
repset_relcache_invalidate_callback(Datum arg, Oid reloid)
{
	PGLogicalRepSetRelation *entry;

	/* Just to be sure. */
	if (RepSetRelationHash == NULL)
		return;

	if ((entry = hash_search(RepSetRelationHash, &reloid,
							 HASH_FIND, NULL)) != NULL)
	{
		entry->isvalid = false;
	}
}

static void
repset_relcache_init(void)
{
	HASHCTL		ctl;

	/* Make sure we've initialized CacheMemoryContext. */
	if (CacheMemoryContext == NULL)
		CreateCacheMemoryContext();

	/* Initialize the hash table. */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(PGLogicalRepSetRelation);
	ctl.hcxt = CacheMemoryContext;

	RepSetRelationHash = hash_create("pglogical repset relation cache", 128,
									 &ctl, HASH_ELEM | HASH_CONTEXT);

	/* Watch for invalidation events. */
	CacheRegisterRelcacheCallback(repset_relcache_invalidate_callback,
								  (Datum) 0);
}

List *
get_replication_sets(List *replication_set_names)
{
	RangeVar	   *rv;
	Relation		rel;
	ListCell	   *lc;
	List		   *replication_sets = NIL;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSETS, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	foreach(lc, replication_set_names)
	{
		char		   *setname = lfirst(lc);
		ScanKeyData		key[1];
		SysScanDesc		scan;
		HeapTuple		tuple;
		RepSetTuple	   *repsettup;
		PGLogicalRepSet	  *repset;

		/* Search for node record. */
		ScanKeyInit(&key[0],
					Anum_repsets_name,
					BTEqualStrategyNumber, F_NAMEEQ,
					NameGetDatum(setname));

		/* TODO: use index. */
		scan = systable_beginscan(rel, 0, true, NULL, 1, key);
		tuple = systable_getnext(scan);

		if (!HeapTupleIsValid(tuple))
			continue; /* error? */

		repsettup = (RepSetTuple *) GETSTRUCT(tuple);

		/* Create and fill the replication set struct. */
		repset = (PGLogicalRepSet *) palloc(sizeof(PGLogicalRepSet));
		repset->id = repsettup->id;
		repset->name = pstrdup(NameStr(repsettup->name));
		repset->replicate_inserts = repsettup->replicate_inserts;
		repset->replicate_updates = repsettup->replicate_updates;
		repset->replicate_deletes = repsettup->replicate_deletes;

		replication_sets = lappend(replication_sets, repset);

		systable_endscan(scan);
	}

	heap_close(rel, RowExclusiveLock);

	return replication_sets;
}

static PGLogicalRepSetRelation *
get_repset_relation(Oid reloid, List *replication_sets)
{
	PGLogicalRepSetRelation *entry;
	bool			found;
	ListCell	   *lc;

	if (RepSetRelationHash == NULL)
		repset_relcache_init();

	/*
	 * HASH_ENTER returns the existing entry if present or creates a new one.
	 */
	entry = hash_search(RepSetRelationHash, (void *) &reloid,
						HASH_ENTER, &found);

	if (found && entry->isvalid)
		return entry;

	/* Fill the entry */
	entry->reloid = reloid;

	foreach(lc, replication_sets)
	{
		PGLogicalRepSet	   *repset = lfirst(lc);

		if (repset->replicate_inserts)
			entry->replicate_inserts = true;
		if (repset->replicate_updates)
			entry->replicate_updates = true;
		if (repset->replicate_deletes)
			entry->replicate_deletes = true;

		/*
		 * Now we now everything is replicated, no point in trying to check
		 * more replication sets.
		 */
		if (entry->replicate_inserts && entry->replicate_updates &&
			entry->replicate_deletes)
			break;
	}

	entry->isvalid = true;

	return entry;
}

bool
relation_is_replicated(Relation rel, PGLogicalConnection *conn,
					   enum ReorderBufferChangeType change)
{
	PGLogicalRepSetRelation *r;

	r = get_repset_relation(RelationGetRelid(rel), conn->replication_sets);

	switch (change)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			return r->replicate_inserts;
		case REORDER_BUFFER_CHANGE_UPDATE:
			return r->replicate_updates;
		case REORDER_BUFFER_CHANGE_DELETE:
			return r->replicate_deletes;
		default:
			elog(ERROR, "should be unreachable");
	}

	/* Not reachable. */
	return false;
}
