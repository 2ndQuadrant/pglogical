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

#define CATALOG_REPSET			"replication_set"
#define CATALOG_REPSET_TABLE	"replication_set_table"

typedef struct RepSetTuple
{
	Oid			id;
	NameData	name;
	bool		replicate_insert;
	bool		replicate_update;
	bool		replicate_delete;
	bool		replicate_truncate;
} RepSetTuple;

#define Natts_repset					6
#define Anum_repset_id					1
#define Anum_repset_name				2
#define Anum_repset_replicate_insert	3
#define Anum_repset_replicate_update	4
#define Anum_repset_replicate_delete	5
#define Anum_repset_replicate_truncate	6

#define Natts_repset_table			2
#define Anum_repset_table_setid	1
#define Anum_repset_table_reloid	2

#define REPLICATION_SET_ID_ALL		-2

static HTAB *RepSetRelationHash = NULL;

static void
check_for_reserved_replication_set(setid)
{
	if (setid == REPLICATION_SET_ID_ALL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("replication set 'all' is reserved and cannot be manipulated")));
}

static PGLogicalRepSet*
repset_fromtuple(HeapTuple tuple)

{
	RepSetTuple *repsettup = (RepSetTuple *) GETSTRUCT(tuple);
	PGLogicalRepSet *repset = (PGLogicalRepSet *)palloc(sizeof(PGLogicalRepSet));
	repset->id = repsettup->id;
	repset->name = pstrdup(NameStr(repsettup->name));
	repset->replicate_insert = repsettup->replicate_insert;
	repset->replicate_update = repsettup->replicate_update;
	repset->replicate_delete = repsettup->replicate_delete;
	repset->replicate_truncate = repsettup->replicate_truncate;
	return repset;
}

/*
 * Read the replication set.
 */
PGLogicalRepSet *
get_replication_set(int setid)
{
	PGLogicalRepSet    *repset;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];

	Assert(IsTransactionState());

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	/* Search for repset record. */
	ScanKeyInit(&key[0],
				Anum_repset_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(setid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "replication set %d not found", setid);

	repset = repset_fromtuple(tuple);

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
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];

	Assert(IsTransactionState());

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	/* Search for repset record. */
	ScanKeyInit(&key[0],
				Anum_repset_name,
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

	repset = repset_fromtuple(tuple);

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

	/*
	 * Watch for invalidation events fired when the relcache changes.
	 *
	 * Note that no invalidations are fired when the replication sets are
	 * created, destroyed, modified, or change membership since there's no
	 * syscache management for user catalogs. We do our own invalidations for
	 * those separately.
	 */
	CacheRegisterRelcacheCallback(repset_relcache_invalidate_callback,
								  (Datum) 0);
}

List *
get_replication_sets(List *replication_set_names, bool missing_ok)
{
	RangeVar	   *rv;
	Relation		rel;
	ListCell	   *lc;
	List		   *replication_sets = NIL;

	Assert(IsTransactionState());

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	foreach(lc, replication_set_names)
	{
		char		   *setname = lfirst(lc);
		ScanKeyData		key[1];
		SysScanDesc		scan;
		HeapTuple		tuple;

		/* Search for repset record. */
		ScanKeyInit(&key[0],
					Anum_repset_name,
					BTEqualStrategyNumber, F_NAMEEQ,
					NameGetDatum(setname));

		/* TODO: use index. */
		scan = systable_beginscan(rel, 0, true, NULL, 1, key);
		tuple = systable_getnext(scan);

		if (!HeapTupleIsValid(tuple))
		{
			if (missing_ok)
				continue;
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("replication set %s not found", setname)));
		}

		replication_sets = lappend(replication_sets, repset_fromtuple(tuple));

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
	entry->replicate_insert = false;
	entry->replicate_update = false;
	entry->replicate_delete = false;
	entry->replicate_truncate = false;

	foreach(lc, replication_sets)
	{
		PGLogicalRepSet	   *repset = lfirst(lc);

		if (repset->replicate_insert)
			entry->replicate_insert = true;
		if (repset->replicate_update)
			entry->replicate_update = true;
		if (repset->replicate_delete)
			entry->replicate_delete = true;
		if (repset->replicate_truncate)
			entry->replicate_truncate = true;

		/*
		 * Now we now everything is replicated, no point in trying to check
		 * more replication sets.
		 */
		if (entry->replicate_insert && entry->replicate_update &&
			entry->replicate_delete && entry->replicate_truncate)
			break;
	}

	entry->isvalid = true;

	return entry;
}

PGLogicalChangeType
to_pglogical_changetype(enum ReorderBufferChangeType change)
{
	/*
	 * Protect against changes in reorderbuffer change type definition or
	 * pglogical change type definition.
	 */
	switch (change)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			return PGLogicalChangeInsert;
		case REORDER_BUFFER_CHANGE_UPDATE:
			return PGLogicalChangeUpdate;
		case REORDER_BUFFER_CHANGE_DELETE:
			return PGLogicalChangeDelete;
		default:
			elog(ERROR, "Unhandled reorder buffer change type %d", change);
			return 0; /* shut compiler up */
	}
}

bool
relation_is_replicated(Relation rel, List *replication_sets,
					   PGLogicalChangeType change_type)
{
	PGLogicalRepSetRelation *r;

	r = get_repset_relation(RelationGetRelid(rel), replication_sets);

	switch (change_type)
	{
		case PGLogicalChangeInsert:
			return r->replicate_insert;
		case PGLogicalChangeUpdate:
			return r->replicate_update;
		case PGLogicalChangeDelete:
			return r->replicate_delete;
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
	Datum		values[Natts_repset];
	bool		nulls[Natts_repset];
	NameData	repset_name;

	if (strlen(repset->name) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("replication set name cannot be empty")));

	if (get_replication_set_by_name(repset->name, true) != NULL)
		elog(ERROR, "replication set %s already exists", repset->name);

	/* Generate new id unless one was already specified. */
	if (repset->id == InvalidOid)
		repset->id = DatumGetUInt32(hash_any((const unsigned char *) repset->name,
											 strlen(repset->name)));

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* Form a tuple. */
	memset(nulls, false, sizeof(nulls));

	values[Anum_repset_id - 1] = ObjectIdGetDatum(repset->id);
	namestrcpy(&repset_name, repset->name);
	values[Anum_repset_name - 1] = NameGetDatum(&repset_name);
	values[Anum_repset_replicate_insert - 1] =
		BoolGetDatum(repset->replicate_insert);
	values[Anum_repset_replicate_update - 1] =
		BoolGetDatum(repset->replicate_update);
	values[Anum_repset_replicate_delete - 1] =
		BoolGetDatum(repset->replicate_delete);
	values[Anum_repset_replicate_truncate - 1] =
		BoolGetDatum(repset->replicate_truncate);

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

	check_for_reserved_replication_set(setid);

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	/* Search for repset record. */
	ScanKeyInit(&key[0],
				Anum_repset_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(setid));

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
	Relation	targetrel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_repset_table];
	bool		nulls[Natts_repset_table];
	PGLogicalRepSet *repset = get_replication_set(setid);

	check_for_reserved_replication_set(setid);

	/* Validate the relation. */
	targetrel = heap_open(reloid, AccessShareLock);

	/* UNLOGGED and TEMP tables cannot be part of replication set. */
	if (!RelationNeedsWAL(targetrel))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("UNLOGGED and TEMP tables cannot be replicated")));

	/* Check of relation has replication index. */
	if (targetrel->rd_indexvalid == 0)
		RelationGetIndexList(targetrel);
	if (!OidIsValid(targetrel->rd_replidindex) &&
		(repset->replicate_update || repset->replicate_delete))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("table %s cannot be added to replication set %s",
						RelationGetRelationName(targetrel), repset->name),
				 errdetail("table does not have PRIMARY KEY and given "
						   "replication set is configured to replicate "
						   "UPDATEs and/or DELETEs"),
				 errhint("Add a PRIMARY KEY to the table")));

	heap_close(targetrel, NoLock);

	/* Open the catalog. */
	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET_TABLE, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* Form a tuple. */
	memset(nulls, false, sizeof(nulls));

	values[Anum_repset_table_setid - 1] = ObjectIdGetDatum(repset->id);
	values[Anum_repset_table_reloid - 1] = reloid;

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

	check_for_reserved_replication_set(setid);

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET_TABLE, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	/* Search for the record. */
	ScanKeyInit(&key[0],
				Anum_repset_table_setid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(setid));
	ScanKeyInit(&key[1],
				Anum_repset_table_reloid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(reloid));

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
