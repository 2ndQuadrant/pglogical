/* -------------------------------------------------------------------------
 *
 * pg_logical_relcache.c
 *     Caching relation specific information
 *
 * Copyright (C) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pg_logical_relcache.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"

#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/hsearch.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/rel.h"

#include "pg_logical_relcache.h"

static HTAB *PGLogicalRelationHash = NULL;


static void pg_logical_relcache_init(void);
static int tupdesc_get_att_by_name(TupleDesc desc, const char *attname);

static void
pg_logical_free_entry(PGLogicalRelation *entry)
{
	if (entry->natts > 0)
	{
		int	i;

		for (i = 0; i < entry->natts; i++)
			pfree(entry->attnames[i]);

		pfree(entry->attnames);
	}

	if (entry->attmap)
		pfree(entry->attmap);

	entry->natts = 0;
	entry->reloid = InvalidOid;
}


PGLogicalRelation *
pg_logical_relation_open(uint32 relid, LOCKMODE lockmode)
{
	PGLogicalRelation *entry;
	bool		found;

	if (PGLogicalRelationHash == NULL)
		pg_logical_relcache_init();

	/*
	 * HASH_ENTER returns the existing entry if present or creates a new one.
	 */
	entry = hash_search(PGLogicalRelationHash, (void *) &relid,
						HASH_ENTER, &found);

	if (!found || !entry->used)
		elog(ERROR, "cache lookup failed for remote relation %u", relid);

	/* Need to update the local cache? */
	if (!OidIsValid(entry->relid))
	{
		RangeVar   *rv = makeNode(RangeVar);
		int			i;
		TupleDesc	desc;

		rv->schemaname = (char *) entry->nspname;
		rv->relname = (char *) entry->relname;
		entry->rel = heap_openrv(rv, lockmode);

		desc = RelationGetDescr(entry->rel);
		for (i = 0; i < entry->natts; i++)
			entry->attmap[i] = tupdesc_get_att_by_name(desc, entry->attnames[i]);

		entry->relid = RelationGetRelid(entry->rel);
	}
	else
		entry->rel = heap_open(entry->relid, lockmode);

	return entry;
}

void
pg_logical_relation_cache_update(uint32 relid, char *schemaname, char *relname,
								 int natts, char **attnames)
{
	MemoryContext		oldcontext;
	PGLogicalRelation  *entry;
	bool				found;
	int					i;

	if (PGLogicalRelationHash == NULL)
		pg_logical_relcache_init();

	/*
	 * HASH_ENTER returns the existing entry if present or creates a new one.
	 */
	entry = hash_search(PGLogicalRelationHash, (void *) &relid,
						HASH_ENTER, &found);

	if (found)
		pg_logical_free_entry(entry);

	/* Make cached copy of the data */
	oldcontext = MemoryContextSwitchTo(CacheMemoryContext);
	entry->nspname = pstrdup(schemaname);
	entry->relname = pstrdup(relname);
	entry->natts = natts;
	entry->attnames = palloc(natts * sizeof(char *));
	for (i = 0; i < natts; i++)
		entry->attnames[i] = pstrdup(attnames[i]);
	entry->attmap = palloc(natts * sizeof(int));
	MemoryContextSwitchTo(oldcontext);

	/* XXX Should we validate the relation against local schema here? */

	entry->used = true;
}

void
pg_logical_relation_close(PGLogicalRelation * rel, LOCKMODE lockmode)
{
	heap_close(rel->rel, lockmode);
	rel->rel = NULL;
}

static void
pg_logical_invalidate_relation(uint32 relid)
{
	HASH_SEQ_STATUS status;
	PGLogicalRelation *entry;

	/*
	 * We sometimes explicitly invalidate the entire bdr relcache -
	 * independent of actual system caused invalidations. Without that this
	 * situation could not happen as the normall inval callback only gets
	 * registered after creating the hash.
	 */
	if (PGLogicalRelationHash == NULL)
		return;

	/*
	 * If relid is InvalidOid, signalling a complete reset, we have to remove
	 * all entries, otherwise just invalidate the specific relation's entry.
	 */
	if (relid == InvalidOid)
	{
		hash_seq_init(&status, PGLogicalRelationHash);

		while ((entry = (PGLogicalRelation *) hash_seq_search(&status)) != NULL)
		{
			entry->relid = InvalidOid;
		}
	}
	else
	{
		if ((entry = hash_search(PGLogicalRelationHash, &relid,
								 HASH_FIND, NULL)) != NULL)
		{
			entry->relid = InvalidOid;
		}
	}
}

void
pg_logical_invalidate_callback(Datum arg, Oid reloid)
{
	//pg_logical_invalidate_relation();
}

static void
pg_logical_relcache_init(void)
{
	HASHCTL		ctl;

	/* Make sure we've initialized CacheMemoryContext. */
	if (CacheMemoryContext == NULL)
		CreateCacheMemoryContext();

	/* Initialize the hash table. */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(uint32);
	ctl.entrysize = sizeof(PGLogicalRelation);
	ctl.hash = tag_hash;
	ctl.hcxt = CacheMemoryContext;

	PGLogicalRelationHash = hash_create("pg_logical relation cache", 128, &ctl,
										HASH_ELEM | HASH_FUNCTION |
										HASH_CONTEXT);

	/* Watch for invalidation events. */
	CacheRegisterRelcacheCallback(pg_logical_invalidate_callback,
								  (Datum) 0);
}


/*
 * Find attribute index in TupleDesc struct by attribute name.
 */
static int
tupdesc_get_att_by_name(TupleDesc desc, const char *attname)
{
	int		i;

	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = desc->attrs[i];

		if (strcmp(NameStr(att->attname), attname) == 0)
			return i;
	}

	elog(ERROR, "Unknown column name %s", attname);
}
