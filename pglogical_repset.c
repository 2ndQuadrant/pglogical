/*-------------------------------------------------------------------------
 *
 * pglogical_repset.c
 *		pglogical replication set manipulation functions
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
#include "access/hash.h"
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

#include "pglogical_node.h"
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
	bool		replicate_truncate;
} RepSetTuple;

#define Natts_repsets					6
#define Anum_repsets_id					1
#define Anum_repsets_name				2
#define Anum_repsets_replicate_inserts	3
#define Anum_repsets_replicate_updates	4
#define Anum_repsets_replicate_deletes	5
#define Anum_repsets_replicate_truncate	6

#define Natts_repset_tables			2
#define Anum_repset_tables_setid	1
#define Anum_repset_tables_reloid	2

static HTAB *RepSetRelationHash = NULL;

/*
 * Read the replication set.
 */
PGLogicalRepSet *
get_replication_set(int setid)
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

	/* Search for repset record. */
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
	repset->replicate_truncate = repsettup->replicate_truncate;

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return repset;
}

/*
 * Find replication set by name
 */
PGLogicalRepSet *
get_replication_set_by_name(const char *setname, bool missing_ok)
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

	/* Search for repset record. */
	ScanKeyInit(&key[0],
				Anum_repsets_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(setname));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
	{
		if (missing_ok)
		{
			systable_endscan(scan);
			heap_close(rel, RowExclusiveLock);
			return NULL;
		}

		elog(ERROR, "replication set %s not found", setname);
	}

	repsettup = (RepSetTuple *) GETSTRUCT(tuple);

	/* Create and fill the replication set struct. */
	repset = (PGLogicalRepSet *) palloc(sizeof(PGLogicalRepSet));

	repset->id = repsettup->id;
	repset->name = pstrdup(NameStr(repsettup->name));
	repset->replicate_inserts = repsettup->replicate_inserts;
	repset->replicate_updates = repsettup->replicate_updates;
	repset->replicate_deletes = repsettup->replicate_deletes;
	repset->replicate_truncate = repsettup->replicate_truncate;

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

		/* Search for repset record. */
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
		repset->replicate_truncate = repsettup->replicate_truncate;

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
	entry->replicate_inserts = false;
	entry->replicate_updates = false;
	entry->replicate_deletes = false;
	entry->replicate_truncate = false;

	foreach(lc, replication_sets)
	{
		PGLogicalRepSet	   *repset = lfirst(lc);

		if (repset->replicate_inserts)
			entry->replicate_inserts = true;
		if (repset->replicate_updates)
			entry->replicate_updates = true;
		if (repset->replicate_deletes)
			entry->replicate_deletes = true;
		if (repset->replicate_truncate)
			entry->replicate_truncate = true;

		/*
		 * Now we now everything is replicated, no point in trying to check
		 * more replication sets.
		 */
		if (entry->replicate_inserts && entry->replicate_updates &&
			entry->replicate_deletes && entry->replicate_truncate)
			break;
	}

	entry->isvalid = true;

	return entry;
}

bool
relation_is_replicated(Relation rel, PGLogicalConnection *conn,
					   PGLogicalChangeType change_type)
{
	PGLogicalRepSetRelation *r;

	r = get_repset_relation(RelationGetRelid(rel), conn->replication_sets);

	switch (change_type)
	{
		case PGLogicalChangeInsert:
			return r->replicate_inserts;
		case PGLogicalChangeUpdate:
			return r->replicate_updates;
		case PGLogicalChangeDelete:
			return r->replicate_deletes;
		case PGLogicalChangeTruncate:
			return r->replicate_truncate;
		default:
			elog(ERROR, "should be unreachable");
	}

	/* Not reachable. */
	return false;
}


/*
 * Add new tuple to the replication_sets catalog.
 */
void
create_replication_set(PGLogicalRepSet *repset)
{
	RangeVar   *rv;
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_repsets];
	bool		nulls[Natts_repsets];
	NameData	repset_name;

	if (get_node_by_name(repset->name, true) != NULL)
		elog(ERROR, "replication set %s already exists", repset->name);

	/* Generate new id unless one was already specified. */
	if (repset->id == InvalidOid)
		repset->id = DatumGetInt32(hash_any((const unsigned char *) repset->name,
											strlen(repset->name)));

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSETS, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* Form a tuple. */
	memset(nulls, false, sizeof(nulls));

	values[Anum_repsets_id - 1] = Int32GetDatum(repset->id);
	namestrcpy(&repset_name, repset->name);
	values[Anum_repsets_name - 1] = NameGetDatum(&repset_name);
	values[Anum_repsets_replicate_inserts - 1] =
		BoolGetDatum(repset->replicate_inserts);
	values[Anum_repsets_replicate_updates - 1] =
		BoolGetDatum(repset->replicate_updates);
	values[Anum_repsets_replicate_deletes - 1] =
		BoolGetDatum(repset->replicate_deletes);

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
 * Delete the tuple from replication sets catalog.
 */
void
drop_replication_set(int setid)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSETS, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	/* Search for repset record. */
	ScanKeyInit(&key[0],
				Anum_repsets_id,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(setid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "replication set %d not found", setid);

	/* Remove the tuple. */
	simple_heap_delete(rel, &tuple->t_self);

	/* Cleanup. */
	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);
}

/*
 * Insert new replication set / relation mapping.
 *
 * The caller is responsible for ensuring the relation exists.
 */
void
replication_set_add_table(int setid, Oid reloid)
{
	RangeVar   *rv;
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_repset_tables];
	bool		nulls[Natts_repset_tables];
	PGLogicalRepSet *repset = get_replication_set(setid);

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET_TABLES, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	/* UNLOGGED and TEMP tables cannot be part of replication set. */
	if (!RelationNeedsWAL(rel))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("UNLOGGED and TEMP tables cannot be replicated")));

	/* Check of relation has replication index. */
	if (rel->rd_indexvalid == 0)
		RelationGetIndexList(rel);
	if (!OidIsValid(rel->rd_replidindex) &&
		(repset->replicate_updates || repset->replicate_deletes))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("table %s cannot be added to replication set %s "
						"because it does not have a PRIMARY KEY and given "
						"replication set is configured to replicate UPDATEs "
						"and DELETEs",
						RelationGetRelationName(rel), repset->name),
				 errhint("Add a PRIMARY KEY to the table")));

	/* Form a tuple. */
	tupDesc = RelationGetDescr(rel);
	memset(nulls, false, sizeof(nulls));

	values[Anum_repset_tables_setid - 1] = Int32GetDatum(repset->id);
	values[Anum_repset_tables_reloid - 1] = reloid;

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
 * Remove existing replication set / relation mapping.
 */
void
replication_set_remove_table(int setid, Oid reloid)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[2];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET_TABLES, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	/* Search for the record. */
	ScanKeyInit(&key[0],
				Anum_repset_tables_setid,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(setid));
	ScanKeyInit(&key[1],
				Anum_repset_tables_reloid,
				BTEqualStrategyNumber, F_OIDEQ,
				Int32GetDatum(reloid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "replication set mapping %d:%d not found", setid, reloid);

	/* Remove the tuple. */
	simple_heap_delete(rel, &tuple->t_self);

	/* Cleanup. */
	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);
}
