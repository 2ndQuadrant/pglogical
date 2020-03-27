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
#include "access/sysattr.h"
#include "access/xact.h"

#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
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

#include "pglogical_dependency.h"
#include "pglogical_node.h"
#include "pglogical_queue.h"
#include "pglogical_repset.h"
#include "pglogical.h"

#define CATALOG_REPSET			"replication_set"
#define CATALOG_REPSET_SEQ		"replication_set_seq"
#define CATALOG_REPSET_TABLE	"replication_set_table"
#define CATALOG_REPSET_RELATION	"replication_set_relation"

typedef struct RepSetTuple
{
	Oid			id;
	Oid			nodeid;
	NameData	name;
	bool		replicate_insert;
	bool		replicate_update;
	bool		replicate_delete;
	bool		replicate_truncate;
} RepSetTuple;

#define Natts_repset					7
#define Anum_repset_id					1
#define Anum_repset_nodeid				2
#define Anum_repset_name				3
#define Anum_repset_replicate_insert	4
#define Anum_repset_replicate_update	5
#define Anum_repset_replicate_delete	6
#define Anum_repset_replicate_truncate	7

typedef struct RepSetSeqTuple
{
	Oid			id;
	Oid			seqoid;
} RepSetSeqTuple;

#define Natts_repset_seq				2
#define Anum_repset_seq_setid			1
#define Anum_repset_seq_seqoid			2

typedef struct RepSetTableTuple
{
	Oid			setid;
	Oid			reloid;
#if 0 /* Only for info here. */
	text		att_list[1];
	text		row_filter;
#endif
} RepSetTableTuple;

#define Natts_repset_table				4
#define Anum_repset_table_setid			1
#define Anum_repset_table_reloid		2
#define Anum_repset_table_att_list	3
#define Anum_repset_table_row_filter	4


#define REPSETTABLEHASH_INITIAL_SIZE 128
static HTAB *RepSetTableHash = NULL;

/*
 * Read the replication set.
 */
PGLogicalRepSet *
get_replication_set(Oid setid)
{
	PGLogicalRepSet    *repset;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];

	Assert(IsTransactionState());

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Search for repset record. */
	ScanKeyInit(&key[0],
				Anum_repset_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(setid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "replication set %u not found", setid);

	repset = replication_set_from_tuple(tuple);

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	return repset;
}

/*
 * Find replication set by name
 */
PGLogicalRepSet *
get_replication_set_by_name(Oid nodeid, const char *setname, bool missing_ok)
{
	PGLogicalRepSet    *repset;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[2];

	Assert(IsTransactionState());

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Search for repset record. */
	ScanKeyInit(&key[0],
				Anum_repset_nodeid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(nodeid));
	ScanKeyInit(&key[1],
				Anum_repset_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(setname));

	scan = systable_beginscan(rel, 0, true, NULL, 2, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
	{
		if (missing_ok)
		{
			systable_endscan(scan);
			table_close(rel, RowExclusiveLock);
			return NULL;
		}

		elog(ERROR, "replication set %s not found", setname);
	}

	repset = replication_set_from_tuple(tuple);

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	return repset;
}

static void
repset_relcache_invalidate_callback(Datum arg, Oid reloid)
{
	PGLogicalTableRepInfo *entry;

	/* Just to be sure. */
	if (RepSetTableHash == NULL)
		return;

	if (reloid == InvalidOid)
	{
		HASH_SEQ_STATUS status;
		hash_seq_init(&status, RepSetTableHash);

		while ((entry = hash_seq_search(&status)) != NULL)
		{
			entry->isvalid = false;
			if (entry->att_list)
				pfree(entry->att_list);
			entry->att_list = NULL;
			if (list_length(entry->row_filter))
				list_free_deep(entry->row_filter);
			entry->row_filter = NIL;
		}
	}
	else if ((entry = hash_search(RepSetTableHash, &reloid,
								  HASH_FIND, NULL)) != NULL)
	{
		entry->isvalid = false;
		if (entry->att_list)
			pfree(entry->att_list);
		entry->att_list = NULL;
		if (list_length(entry->row_filter))
			list_free_deep(entry->row_filter);
		entry->row_filter = NIL;
	}
}

static void
repset_relcache_init(void)
{
	HASHCTL		ctl;
	int hashflags;

	/* Make sure we've initialized CacheMemoryContext. */
	if (CacheMemoryContext == NULL)
		CreateCacheMemoryContext();

	/* Initialize the hash table. */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(PGLogicalTableRepInfo);
	ctl.hcxt = CacheMemoryContext;
	hashflags = HASH_ELEM | HASH_CONTEXT;
#if PG_VERSION_NUM < 90500
	/*
	 * Handle the old hash API in PostgreSQL 9.4.
	 *
	 * See postgres commit:
	 *
	 * 4a14f13a0ab Improve hash_create's API for selecting simple-binary-key hash functions.
	 */
	ctl.hash = oid_hash;
	hashflags |= HASH_FUNCTION;
#else
	hashflags |= HASH_BLOBS;
#endif

	RepSetTableHash = hash_create("pglogical repset table cache",
                                      REPSETTABLEHASH_INITIAL_SIZE, &ctl,
                                      hashflags);

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
get_node_replication_sets(Oid nodeid)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];
	List		   *replication_sets = NIL;

	Assert(IsTransactionState());

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_repset_nodeid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(nodeid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		RepSetTuple	*t = (RepSetTuple *) GETSTRUCT(tuple);
		PGLogicalRepSet	    *repset = get_replication_set(t->id);
		replication_sets = lappend(replication_sets, repset);
	}

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	return replication_sets;
}

List *
get_replication_sets(Oid nodeid, List *replication_set_names, bool missing_ok)
{
	RangeVar	   *rv;
	Relation		rel;
	ListCell	   *lc;
	ScanKeyData		key[2];
	List		   *replication_sets = NIL;

	Assert(IsTransactionState());

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Setup common part of key. */
	ScanKeyInit(&key[0],
				Anum_repset_nodeid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(nodeid));

	foreach(lc, replication_set_names)
	{
		char		   *setname = lfirst(lc);
		SysScanDesc		scan;
		HeapTuple		tuple;

		/* Search for repset record. */
		ScanKeyInit(&key[1],
					Anum_repset_name,
					BTEqualStrategyNumber, F_NAMEEQ,
					CStringGetDatum(setname));

		/* TODO: use index. */
		scan = systable_beginscan(rel, 0, true, NULL, 2, key);
		tuple = systable_getnext(scan);

		if (!HeapTupleIsValid(tuple))
		{
			if (missing_ok)
			{
				systable_endscan(scan);
				continue;
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("replication set %s not found", setname)));
		}

		replication_sets = lappend(replication_sets,
								   replication_set_from_tuple(tuple));

		systable_endscan(scan);
	}

	table_close(rel, RowExclusiveLock);

	return replication_sets;
}

PGLogicalTableRepInfo *
get_table_replication_info(Oid nodeid, Relation table,
						   List *subs_replication_sets)
{
	PGLogicalTableRepInfo *entry;
	bool			found;
	RangeVar	   *rv;
	Oid				reloid = RelationGetRelid(table);
	Oid				repset_reloid;
	Relation		repset_rel;
	ScanKeyData		key[1];
	SysScanDesc		scan;
	HeapTuple		tuple;
	TupleDesc		table_desc,
					repset_rel_desc;

	if (RepSetTableHash == NULL)
		repset_relcache_init();

	/*
	 * HASH_ENTER returns the existing entry if present or creates a new one.
	 *
	 * It might seem that it's weird to use just reloid here for the cache key
	 * when we are searching for nodeid + relation. But this function is only
	 * used by the output plugin which means the nodeid is always the same as
	 * only one node is connected to current process.
	 */
	entry = hash_search(RepSetTableHash, (void *) &reloid,
						HASH_ENTER, &found);

	if (found && entry->isvalid)
		return entry;

	/* Fill the entry */
	entry->reloid = reloid;
	entry->replicate_insert = false;
	entry->replicate_update = false;
	entry->replicate_delete = false;
	entry->att_list = NULL;
	entry->row_filter = NIL;

	/*
	 * Check for match between table's replication sets and the subscription
	 * list of replication sets that was given as parameter.
	 *
	 * Note that tables can have no replication sets. This will be commonly
	 * true for example for internal tables which are created during table
	 * rewrites, so if we'll want to support replicating those, we'll have
	 * to have special handling for them.
	 */
	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET_TABLE, -1);
	repset_reloid = RangeVarGetRelid(rv, RowExclusiveLock, true);
	/* Backwards compat with 1.1/1.2 where the relation name was different. */
	if (!OidIsValid(repset_reloid))
	{
		rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET_RELATION, -1);
		repset_reloid = RangeVarGetRelid(rv, RowExclusiveLock, true);
		if (!OidIsValid(repset_reloid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("relation \"%s.%s\" does not exist",
							rv->schemaname, rv->relname)));
	}
	repset_rel = table_open(repset_reloid, NoLock);
	repset_rel_desc = RelationGetDescr(repset_rel);
	table_desc = RelationGetDescr(table);

	ScanKeyInit(&key[0],
				Anum_repset_table_reloid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(reloid));

	/* TODO: use index */
	scan = systable_beginscan(repset_rel, 0, true, NULL, 1, key);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		RepSetTableTuple   *t = (RepSetTableTuple *) GETSTRUCT(tuple);
		ListCell		   *lc;

		foreach (lc, subs_replication_sets)
		{
			PGLogicalRepSet	   *repset = lfirst(lc);
			bool				isnull;
			Datum				d;

			if (t->setid == repset->id)
			{
				/* Update the action filter. */
				if (repset->replicate_insert)
					entry->replicate_insert = true;
				if (repset->replicate_update)
					entry->replicate_update = true;
				if (repset->replicate_delete)
					entry->replicate_delete = true;

				/* Update replicated column map. */
				d = heap_getattr(tuple, Anum_repset_table_att_list,
								 repset_rel_desc, &isnull);
				if (!isnull)
				{
					Datum	   *elems;
					int			nelems, i;

					deconstruct_array(DatumGetArrayTypePCopy(d),
									  TEXTOID, -1, false, 'i',
									  &elems, NULL, &nelems);

					for (i = 0; i < nelems; i++)
					{
						const char *attname = TextDatumGetCString(elems[i]);
						int			attnum = get_att_num_by_name(table_desc,
																 attname);

						MemoryContext olctx = MemoryContextSwitchTo(CacheMemoryContext);
						entry->att_list = bms_add_member(entry->att_list,
								attnum - FirstLowInvalidHeapAttributeNumber);
						MemoryContextSwitchTo(olctx);
					}
				}

				/* Add row filter if any. */
				d = heap_getattr(tuple, Anum_repset_table_row_filter,
								 repset_rel_desc, &isnull);
				if (!isnull)
				{
					MemoryContext olctx = MemoryContextSwitchTo(CacheMemoryContext);
					Node   *row_filter = stringToNode(TextDatumGetCString(d));
					entry->row_filter = lappend(entry->row_filter, row_filter);
					MemoryContextSwitchTo(olctx);
				}
			}
		}
	}

	systable_endscan(scan);
	table_close(repset_rel, RowExclusiveLock);
	entry->isvalid = true;

	return entry;
}

List *
get_table_replication_sets(Oid nodeid, Oid reloid)
{
	RangeVar	   *rv;
	Oid				relid;
	Relation		rel;
	ScanKeyData		key[1];
	SysScanDesc		scan;
	HeapTuple		tuple;
	List		   *replication_sets = NIL;

	Assert(IsTransactionState());

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET_TABLE, -1);
	relid = RangeVarGetRelid(rv, RowExclusiveLock, true);
	/* Backwards compat with 1.1/1.2 where the relation name was different. */
	if (!OidIsValid(relid))
	{
		rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET_RELATION, -1);
		relid = RangeVarGetRelid(rv, RowExclusiveLock, true);
		if (!OidIsValid(relid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("relation \"%s.%s\" does not exist",
							rv->schemaname, rv->relname)));
	}
	rel = table_open(relid, NoLock);

	ScanKeyInit(&key[0],
				Anum_repset_table_reloid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(reloid));

	/* TODO: use index */
	scan = systable_beginscan(rel, 0, true, NULL, 1, key);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		RepSetSeqTuple		*t = (RepSetSeqTuple *) GETSTRUCT(tuple);
		PGLogicalRepSet	    *repset = get_replication_set(t->id);

		if (repset->nodeid != nodeid)
			continue;

		replication_sets = lappend(replication_sets, repset);
	}

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	return replication_sets;
}

static bool
sequence_has_replication_sets(Oid nodeid, Oid seqoid)
{
	RangeVar	   *rv;
	Relation		rel;
	ScanKeyData		key[1];
	SysScanDesc		scan;
	HeapTuple		tuple;
	bool			res = false;

	Assert(IsTransactionState());

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET_SEQ, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_repset_seq_seqoid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(seqoid));

	/* TODO: use index */
	scan = systable_beginscan(rel, 0, true, NULL, 1, key);

	if (HeapTupleIsValid(tuple = systable_getnext(scan)))
		res = true;

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	return res;
}

List *
get_seq_replication_sets(Oid nodeid, Oid seqoid)
{
	RangeVar	   *rv;
	Relation		rel;
	ScanKeyData		key[1];
	SysScanDesc		scan;
	HeapTuple		tuple;
	List		   *replication_sets = NIL;

	Assert(IsTransactionState());

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET_SEQ, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_repset_table_reloid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(seqoid));

	/* TODO: use index */
	scan = systable_beginscan(rel, 0, true, NULL, 1, key);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		RepSetSeqTuple		*t = (RepSetSeqTuple *) GETSTRUCT(tuple);
		PGLogicalRepSet	    *repset = get_replication_set(t->id);

		if (repset->nodeid != nodeid)
			continue;

		replication_sets = lappend(replication_sets, repset);
	}

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	return replication_sets;
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

	if (get_replication_set_by_name(repset->nodeid, repset->name, true) != NULL)
		elog(ERROR, "replication set %s already exists", repset->name);

	/*
	 * Generate new id unless one was already specified.
	 */
	if (repset->id == InvalidOid)
	{
		uint32	hashinput[2];

		hashinput[0] = repset->nodeid;
		hashinput[1] = DatumGetUInt32(hash_any((const unsigned char *) repset->name,
											   strlen(repset->name)));

		repset->id = DatumGetUInt32(hash_any((const unsigned char *) hashinput,
											 (int) sizeof(hashinput)));
	}

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET, -1);
	rel = table_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* Form a tuple. */
	memset(nulls, false, sizeof(nulls));

	values[Anum_repset_id - 1] = ObjectIdGetDatum(repset->id);
	values[Anum_repset_nodeid - 1] = ObjectIdGetDatum(repset->nodeid);
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
	CatalogTupleInsert(rel, tup);

	/* Cleanup. */
	heap_freetuple(tup);
	table_close(rel, RowExclusiveLock);

	CommandCounterIncrement();
}

/*
 * Alter the existing replication set.
 */
void
alter_replication_set(PGLogicalRepSet *repset)
{
	RangeVar	   *rv;
	SysScanDesc		scan;
	ScanKeyData		key[1];
	Relation		rel;
	TupleDesc		tupDesc;
	HeapTuple		oldtup,
					newtup;
	Datum			values[Natts_repset];
	bool			nulls[Natts_repset];
	bool			replaces[Natts_repset];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET, -1);
	rel = table_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* Search for repset record. */
	ScanKeyInit(&key[0],
				Anum_repset_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(repset->id));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	oldtup = systable_getnext(scan);

	if (!HeapTupleIsValid(oldtup))
		elog(ERROR, "replication set %u not found", repset->id);

	/*
	 * Validate that replication is not being changed to replicate UPDATEs
	 * and DELETEs if it contains any tables without replication identity.
	 */
	if (repset->replicate_update || repset->replicate_delete)
	{
		RangeVar	   *tablesrv;
		Relation		tablesrel;
		SysScanDesc		tablesscan;
		HeapTuple		tablestup;
		ScanKeyData		tableskey[1];

		tablesrv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET_TABLE, -1);
		tablesrel = table_openrv(tablesrv, RowExclusiveLock);

		/* Search for the record. */
		ScanKeyInit(&tableskey[0],
					Anum_repset_table_setid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(repset->id));

		tablesscan = systable_beginscan(tablesrel, 0, true, NULL, 1, tableskey);

		/* Process every individual table in the set. */
		while (HeapTupleIsValid(tablestup = systable_getnext(tablesscan)))
		{
			RepSetTableTuple   *t = (RepSetTableTuple *) GETSTRUCT(tablestup);
			Relation			targetrel;

			targetrel = table_open(t->reloid, AccessShareLock);

			/* Check of relation has replication index. */
			if (RelationGetForm(targetrel)->relkind == RELKIND_RELATION)
			{

				if (targetrel->rd_indexvalid == 0)
					RelationGetIndexList(targetrel);
				if (!OidIsValid(targetrel->rd_replidindex) &&
					(repset->replicate_update || repset->replicate_delete))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("replication set %s cannot be altered to "
									"replicate UPDATEs or DELETEs because it "
									"contains tables without PRIMARY KEY",
									repset->name)));
			}

			table_close(targetrel, NoLock);
		}

		systable_endscan(tablesscan);
		table_close(tablesrel, RowExclusiveLock);
	}

	/* Everything ok, form a new tuple. */
	memset(nulls, false, sizeof(nulls));
	memset(replaces, true, sizeof(replaces));

	replaces[Anum_repset_id - 1] = false;
	replaces[Anum_repset_name - 1] = false;
	replaces[Anum_repset_nodeid - 1] = false;

	values[Anum_repset_replicate_insert - 1] =
		BoolGetDatum(repset->replicate_insert);
	values[Anum_repset_replicate_update - 1] =
		BoolGetDatum(repset->replicate_update);
	values[Anum_repset_replicate_delete - 1] =
		BoolGetDatum(repset->replicate_delete);
	values[Anum_repset_replicate_truncate - 1] =
		BoolGetDatum(repset->replicate_truncate);

	newtup = heap_modify_tuple(oldtup, tupDesc, values, nulls, replaces);

	/* Update the tuple in catalog. */
	CatalogTupleUpdate(rel, &oldtup->t_self, newtup);

	/* Cleanup. */
	heap_freetuple(newtup);
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);
}

/*
 * Remove all tables from replication set.
 */
static void
replication_set_remove_tables(Oid setid, Oid nodeid)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];
	ObjectAddress	myself;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET_TABLE, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	myself.classId = get_replication_set_table_rel_oid();
	myself.objectId = setid;

	/* Search for the record. */
	ScanKeyInit(&key[0],
				Anum_repset_table_setid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(setid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		RepSetTableTuple   *t = (RepSetTableTuple *) GETSTRUCT(tuple);
		Oid					reloid = t->reloid;

		/* Remove the tuple. */
		simple_heap_delete(rel, &tuple->t_self);
		CacheInvalidateRelcacheByRelid(reloid);

		/* Dependency cleanup. */
		myself.objectSubId = reloid;
		pglogical_tryDropDependencies(&myself, DROP_CASCADE);
	}

	/* Cleanup. */
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);
}

/*
 * Remove all sequences from replication set.
 */
static void
replication_set_remove_seqs(Oid setid, Oid nodeid)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];
	ObjectAddress	myself;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET_SEQ, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Search for the record. */
	ScanKeyInit(&key[0],
				Anum_repset_table_setid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(setid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);

	myself.classId = get_replication_set_seq_rel_oid();
	myself.objectId = setid;

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		RepSetSeqTuple	   *t = (RepSetSeqTuple *) GETSTRUCT(tuple);
		Oid					seqoid = t->seqoid;

		/* Remove the tuple. */
		simple_heap_delete(rel, &tuple->t_self);

		/* Make sure the sequence_has_replication_sets sees the changes. */
		CommandCounterIncrement();
		if (!sequence_has_replication_sets(nodeid, seqoid))
			pglogical_drop_sequence_state_record(seqoid);

		CacheInvalidateRelcacheByRelid(seqoid);

		/* Dependency cleanup. */
		myself.objectSubId = seqoid;
		pglogical_tryDropDependencies(&myself, DROP_CASCADE);
	}

	/* Cleanup. */
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);
}


/*
 * Delete the tuple from replication sets catalog.
 */
void
drop_replication_set(Oid setid)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];
	RepSetTuple	   *repset;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Search for repset record. */
	ScanKeyInit(&key[0],
				Anum_repset_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(setid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "replication set %u not found", setid);

	repset = (RepSetTuple *) GETSTRUCT(tuple);

	/* Remove all tables and sequences associated with the repset. */
	replication_set_remove_tables(setid, repset->nodeid);
	replication_set_remove_seqs(setid, repset->nodeid);

	/* Remove the tuple. */
	simple_heap_delete(rel, &tuple->t_self);

	/* Cleanup. */
	CacheInvalidateRelcache(rel);
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	CommandCounterIncrement();
}

void
drop_node_replication_sets(Oid nodeid)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];

	Assert(IsTransactionState());

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_repset_nodeid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(nodeid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);

	/* Remove matching tuples. */
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		RepSetTuple		*repset = (RepSetTuple *) GETSTRUCT(tuple);

		/* Remove all tables and sequences associated with the repset. */
		replication_set_remove_tables(repset->id, repset->nodeid);
		replication_set_remove_seqs(repset->id, repset->nodeid);

		/* Remove the repset. */
		simple_heap_delete(rel, &tuple->t_self);
	}

	/* Cleanup. */
	CacheInvalidateRelcache(rel);
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	CommandCounterIncrement();
}

/*
 * Insert new replication set / table mapping.
 */
void
replication_set_add_table(Oid setid, Oid reloid, List *att_list,
						  Node *row_filter)
{
	RangeVar   *rv;
	Relation	rel;
	Relation	targetrel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_repset_table];
	bool		nulls[Natts_repset_table];
	PGLogicalRepSet *repset = get_replication_set(setid);
	ObjectAddress	referenced;
	ObjectAddress	myself;

	/* Open the relation. */
	targetrel = table_open(reloid, ShareRowExclusiveLock);

	/* UNLOGGED and TEMP relations cannot be part of replication set. */
	if (!RelationNeedsWAL(targetrel))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("UNLOGGED and TEMP tables cannot be replicated")));

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

	create_truncate_trigger(targetrel);

	table_close(targetrel, NoLock);

	/* Open the catalog. */
	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET_TABLE, -1);
	rel = table_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* Form a tuple. */
	memset(nulls, false, sizeof(nulls));

	values[Anum_repset_table_setid - 1] = ObjectIdGetDatum(repset->id);
	values[Anum_repset_table_reloid - 1] = ObjectIdGetDatum(reloid);

	if (list_length(att_list))
		values[Anum_repset_table_att_list - 1] =
			PointerGetDatum(strlist_to_textarray(att_list));
	else
		nulls[Anum_repset_table_att_list - 1] = true;

	if (row_filter)
		values[Anum_repset_table_row_filter - 1] =
			CStringGetTextDatum(nodeToString(row_filter));
	else
		nulls[Anum_repset_table_row_filter - 1] = true;

	tup = heap_form_tuple(tupDesc, values, nulls);

	/* Insert the tuple to the catalog. */
	CatalogTupleInsert(rel, tup);

	/* Cleanup. */
	CacheInvalidateRelcacheByRelid(reloid);
	heap_freetuple(tup);

	myself.classId = get_replication_set_table_rel_oid();
	myself.objectId = setid;
	myself.objectSubId = reloid;

	referenced.classId = RelationRelationId;
	referenced.objectId = reloid;
	referenced.objectSubId = 0;

	pglogical_recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	/* Make sure we record dependencies for the row_filter as well. */
	if (row_filter)
	{
		pglogical_recordDependencyOnSingleRelExpr(&myself, row_filter,
												  reloid, DEPENDENCY_NORMAL,
												  DEPENDENCY_NORMAL);
	}

	table_close(rel, RowExclusiveLock);

	CommandCounterIncrement();
}

/*
 * Insert new replication set / sequence mapping.
 */
void
replication_set_add_seq(Oid setid, Oid seqoid)
{
	RangeVar   *rv;
	Relation	rel;
	Relation	targetrel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_repset_table];
	bool		nulls[Natts_repset_table];
	PGLogicalRepSet *repset = get_replication_set(setid);
	ObjectAddress	referenced;
	ObjectAddress	myself;

	/* Open the relation. */
	targetrel = table_open(seqoid, ShareRowExclusiveLock);

	/* UNLOGGED and TEMP relations cannot be part of replication set. */
	if (!RelationNeedsWAL(targetrel))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("UNLOGGED and TEMP sequences cannot be replicated")));

	/* Ensure track the state of the sequence. */
	pglogical_create_sequence_state_record(seqoid);

	table_close(targetrel, NoLock);

	/* Open the catalog. */
	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET_SEQ, -1);
	rel = table_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* Form a tuple. */
	memset(nulls, false, sizeof(nulls));

	values[Anum_repset_seq_setid - 1] = ObjectIdGetDatum(repset->id);
	values[Anum_repset_seq_seqoid - 1] = ObjectIdGetDatum(seqoid);

	tup = heap_form_tuple(tupDesc, values, nulls);

	/* Insert the tuple to the catalog. */
	CatalogTupleInsert(rel, tup);

	/* Cleanup. */
	CacheInvalidateRelcacheByRelid(seqoid);
	heap_freetuple(tup);

	myself.classId = get_replication_set_seq_rel_oid();
	myself.objectId = setid;
	myself.objectSubId = seqoid;

	referenced.classId = RelationRelationId;
	referenced.objectId = seqoid;
	referenced.objectSubId = 0;

	pglogical_recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

	table_close(rel, RowExclusiveLock);

	CommandCounterIncrement();
}


/*
 * Get list of table oids.
 */
List *
replication_set_get_tables(Oid setid)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];
	List		   *res = NIL;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET_TABLE, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Setup the search. */
	ScanKeyInit(&key[0],
				Anum_repset_table_setid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(setid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);

	/* Build the list from the table. */
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		RepSetTableTuple   *t = (RepSetTableTuple *) GETSTRUCT(tuple);

		res = lappend_oid(res, t->reloid);
	}

	/* Cleanup. */
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	return res;
}

/*
 * Get list of sequence oids.
 */
List *
replication_set_get_seqs(Oid setid)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];
	List		   *res = NIL;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET_SEQ, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Setup the search. */
	ScanKeyInit(&key[0],
				Anum_repset_seq_setid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(setid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);

	/* Build the list from the table. */
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		RepSetSeqTuple	   *s = (RepSetSeqTuple *) GETSTRUCT(tuple);

		res = lappend_oid(res, s->seqoid);
	}

	/* Cleanup. */
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	return res;
}

/*
 * Remove existing replication set / table mapping.
 */
void
replication_set_remove_table(Oid setid, Oid reloid, bool from_drop)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[2];
	ObjectAddress	myself;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET_TABLE, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Search for the record. */
	ScanKeyInit(&key[0],
				Anum_repset_table_setid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(setid));
	ScanKeyInit(&key[1],
				Anum_repset_table_reloid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(reloid));

	scan = systable_beginscan(rel, 0, true, NULL, 2, key);
	tuple = systable_getnext(scan);

	/*
	 * Remove the tuple if found, if not found report error unless this
	 * function was called as result of table drop.
	 */
	if (HeapTupleIsValid(tuple))
		simple_heap_delete(rel, &tuple->t_self);
	else if (!from_drop)
		elog(ERROR, "replication set table mapping %u:%u not found",
			 setid, reloid);

	/* We can only invalidate the relcache when relation still exists. */
	if (!from_drop)
		CacheInvalidateRelcacheByRelid(reloid);

	/* Dependency cleanup. */
	myself.classId = get_replication_set_table_rel_oid();
	myself.objectId = setid;
	myself.objectSubId = reloid;
	pglogical_tryDropDependencies(&myself, DROP_CASCADE);

	/* Make sure the has_relation_replication_sets sees the changes. */
	CommandCounterIncrement();

	/* Cleanup. */
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);
}

/*
 * Remove existing replication set / sequence mapping.
 */
void
replication_set_remove_seq(Oid setid, Oid seqoid, bool from_drop)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[2];
	ObjectAddress	myself;
	PGLogicalRepSet *repset = get_replication_set(setid);

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_REPSET_SEQ, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Search for the record. */
	ScanKeyInit(&key[0],
				Anum_repset_seq_setid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(setid));
	ScanKeyInit(&key[1],
				Anum_repset_seq_seqoid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(seqoid));

	scan = systable_beginscan(rel, 0, true, NULL, 2, key);
	tuple = systable_getnext(scan);

	/*
	 * Remove the tuple if found, if not found report error uless this function
	 * was called as result of table drop.
	 */
	if (HeapTupleIsValid(tuple))
		simple_heap_delete(rel, &tuple->t_self);
	else if (!from_drop)
		elog(ERROR, "replication set sequence mapping %u:%u not found",
			 setid, seqoid);

	/* We can only invalidate the relcache when relation still exists. */
	if (!from_drop)
		CacheInvalidateRelcacheByRelid(seqoid);

	/* Dependency cleanup. */
	myself.classId = get_replication_set_seq_rel_oid();
	myself.objectId = setid;
	myself.objectSubId = seqoid;
	pglogical_tryDropDependencies(&myself, DROP_CASCADE);

	/* Make sure the has_relation_replication_sets sees the changes. */
	CommandCounterIncrement();
	if (from_drop || !sequence_has_replication_sets(repset->nodeid, seqoid))
		pglogical_drop_sequence_state_record(seqoid);

	/* Cleanup. */
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);
}

/*
 * Utility functions for working with PGLogicalRepSet struct.
 */
PGLogicalRepSet*
replication_set_from_tuple(HeapTuple tuple)
{
	RepSetTuple *repsettup = (RepSetTuple *) GETSTRUCT(tuple);
	PGLogicalRepSet *repset = (PGLogicalRepSet *) palloc(sizeof(PGLogicalRepSet));
	repset->id = repsettup->id;
	repset->nodeid = repsettup->nodeid;
	repset->name = pstrdup(NameStr(repsettup->name));
	repset->replicate_insert = repsettup->replicate_insert;
	repset->replicate_update = repsettup->replicate_update;
	repset->replicate_delete = repsettup->replicate_delete;
	repset->replicate_truncate = repsettup->replicate_truncate;
	return repset;
}

/*
 * Get (cached) oid of the replication set table.
 */
Oid
get_replication_set_rel_oid(void)
{
	static Oid	repsetreloid = InvalidOid;

	if (repsetreloid == InvalidOid)
		repsetreloid = get_pglogical_table_oid(CATALOG_REPSET);

	return repsetreloid;
}

/*
 * Get (cached) oid of the replication set table mapping table.
 */
Oid
get_replication_set_table_rel_oid(void)
{
	static Oid	repsettablereloid = InvalidOid;

	if (repsettablereloid == InvalidOid)
		repsettablereloid = get_pglogical_table_oid(CATALOG_REPSET_TABLE);

	return repsettablereloid;
}

/*
 * Get (cached) oid of the replication set sequence mapping table.
 */
Oid
get_replication_set_seq_rel_oid(void)
{
	static Oid	repsetseqreloid = InvalidOid;

	if (repsetseqreloid == InvalidOid)
		repsetseqreloid = get_pglogical_table_oid(CATALOG_REPSET_SEQ);

	return repsetseqreloid;
}


/*
 * Given a List of strings, return it as single comma separated
 * string, quoting identifiers as needed.
 *
 * This is essentially the reverse of SplitIdentifierString.
 *
 * The caller should free the result.
 */
char *
stringlist_to_identifierstr(List *strings)
{
	ListCell *lc;
	StringInfoData res;
	bool first = true;

	initStringInfo(&res);

	foreach (lc, strings)
	{
		if (first)
			first = false;
		else
			appendStringInfoChar(&res, ',');

		appendStringInfoString(&res, quote_identifier((char *)lfirst(lc)));
	}

	return res.data;
}

int
get_att_num_by_name(TupleDesc desc, const char *attname)
{
	int		i;

	for (i = 0; i < desc->natts; i++)
	{
		if (TupleDescAttr(desc,i)->attisdropped)
			continue;

		if (namestrcmp(&(TupleDescAttr(desc,i)->attname), attname) == 0)
			return TupleDescAttr(desc,i)->attnum;
	}

	return FirstLowInvalidHeapAttributeNumber;
}
