/* -------------------------------------------------------------------------
 *
 * pglogical_relcache.c
 *     Caching relation specific information
 *
 * Copyright (C) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pglogical_relcache.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"

#include "catalog/pg_trigger.h"

#include "commands/trigger.h"

#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/hsearch.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/rel.h"

#include "pglogical.h"
#include "pglogical_relcache.h"

#define PGLOGICALRELATIONHASH_INITIAL_SIZE 128
static HTAB *PGLogicalRelationHash = NULL;


static void pglogical_relcache_init(void);
static int tupdesc_get_att_by_name(TupleDesc desc, const char *attname);

static void
relcache_free_entry(PGLogicalRelation *entry)
{
	pfree(entry->nspname);
	pfree(entry->relname);

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
	entry->rel = NULL;
}


PGLogicalRelation *
pglogical_relation_open(uint32 remoteid, LOCKMODE lockmode)
{
	PGLogicalRelation *entry;
	bool		found;

	if (PGLogicalRelationHash == NULL)
		pglogical_relcache_init();

	/* Search for existing entry. */
	entry = hash_search(PGLogicalRelationHash, (void *) &remoteid,
						HASH_FIND, &found);

	if (!found)
		elog(ERROR, "cache lookup failed for remote relation %u",
			 remoteid);

	/* Need to update the local cache? */
	if (!OidIsValid(entry->reloid))
	{
		RangeVar   *rv = makeNode(RangeVar);
		int			i;
		TupleDesc	desc;

		rv->schemaname = (char *) entry->nspname;
		rv->relname = (char *) entry->relname;
		entry->rel = table_openrv(rv, lockmode);

		desc = RelationGetDescr(entry->rel);
		for (i = 0; i < entry->natts; i++)
			entry->attmap[i] = tupdesc_get_att_by_name(desc, entry->attnames[i]);

		entry->reloid = RelationGetRelid(entry->rel);

		/* Cache trigger info. */
		entry->hasTriggers = false;
		if (entry->rel->trigdesc != NULL)
		{
			TriggerDesc	   *trigdesc = entry->rel->trigdesc;
			int				i;

			for (i = 0; i < trigdesc->numtriggers; i++)
			{
				Trigger *trigger = &trigdesc->triggers[i];

				/* We only fire replica triggers on rows */
				if (!(trigger->tgenabled == TRIGGER_FIRES_ON_ORIGIN ||
					  trigger->tgenabled == TRIGGER_DISABLED) &&
					TRIGGER_FOR_ROW(trigger->tgtype))
				{
					entry->hasTriggers = true;
					break;
				}
			}
		}
	}
	else if (!entry->rel)
		entry->rel = table_open(entry->reloid, lockmode);

	return entry;
}

void
pglogical_relation_cache_update(uint32 remoteid, char *schemaname,
								 char *relname, int natts, char **attnames)
{
	MemoryContext		oldcontext;
	PGLogicalRelation  *entry;
	bool				found;
	int					i;

	if (PGLogicalRelationHash == NULL)
		pglogical_relcache_init();

	/*
	 * HASH_ENTER returns the existing entry if present or creates a new one.
	 */
	entry = hash_search(PGLogicalRelationHash, (void *) &remoteid,
						HASH_ENTER, &found);

	if (found)
		relcache_free_entry(entry);

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

	entry->reloid = InvalidOid;
}

void
pglogical_relation_cache_updater(PGLogicalRemoteRel *remoterel)
{
	MemoryContext		oldcontext;
	PGLogicalRelation  *entry;
	bool				found;
	int					i;

	if (PGLogicalRelationHash == NULL)
		pglogical_relcache_init();

	/*
	 * HASH_ENTER returns the existing entry if present or creates a new one.
	 */
	entry = hash_search(PGLogicalRelationHash, (void *) &remoterel->relid,
						HASH_ENTER, &found);

	if (found)
		relcache_free_entry(entry);

	/* Make cached copy of the data */
	oldcontext = MemoryContextSwitchTo(CacheMemoryContext);
	entry->nspname = pstrdup(remoterel->nspname);
	entry->relname = pstrdup(remoterel->relname);
	entry->natts = remoterel->natts;
	entry->attnames = palloc(remoterel->natts * sizeof(char *));
	for (i = 0; i < remoterel->natts; i++)
		entry->attnames[i] = pstrdup(remoterel->attnames[i]);
	entry->attmap = palloc(remoterel->natts * sizeof(int));
	MemoryContextSwitchTo(oldcontext);

	/* XXX Should we validate the relation against local schema here? */

	entry->reloid = InvalidOid;
}

void
pglogical_relation_close(PGLogicalRelation * rel, LOCKMODE lockmode)
{
	table_close(rel->rel, lockmode);
	rel->rel = NULL;
}

static void
pglogical_relcache_invalidate_callback(Datum arg, Oid reloid)
{
	PGLogicalRelation *entry;

	/* Just to be sure. */
	if (PGLogicalRelationHash == NULL)
		return;

	if (reloid != InvalidOid)
	{
		HASH_SEQ_STATUS status;

		hash_seq_init(&status, PGLogicalRelationHash);

		/* TODO, use inverse lookup hastable */
		while ((entry = (PGLogicalRelation *) hash_seq_search(&status)) != NULL)
		{
			if (entry->reloid == reloid)
				entry->reloid = InvalidOid;
		}
	}
	else
	{
		/* invalidate all cache entries */
		HASH_SEQ_STATUS status;

		hash_seq_init(&status, PGLogicalRelationHash);

		while ((entry = (PGLogicalRelation *) hash_seq_search(&status)) != NULL)
			entry->reloid = InvalidOid;
	}
}

static void
pglogical_relcache_init(void)
{
	HASHCTL		ctl;
	int			hashflags;

	/* Make sure we've initialized CacheMemoryContext. */
	if (CacheMemoryContext == NULL)
		CreateCacheMemoryContext();

	/* Initialize the hash table. */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(uint32);
	ctl.entrysize = sizeof(PGLogicalRelation);
	ctl.hcxt = CacheMemoryContext;
	hashflags = HASH_ELEM | HASH_CONTEXT;
#if PG_VERSION_NUM < 90500
	/*
	 * Handle the old hash API in PostgreSQL 9.4.
	 * Note, this assumes that Oid is uint32 which is the case for 9.4 anyway.
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

	PGLogicalRelationHash = hash_create("pglogical relation cache",
                                            PGLOGICALRELATIONHASH_INITIAL_SIZE,
                                            &ctl, hashflags);

	/* Watch for invalidation events. */
	CacheRegisterRelcacheCallback(pglogical_relcache_invalidate_callback,
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
		Form_pg_attribute att = TupleDescAttr(desc,i);

		if (strcmp(NameStr(att->attname), attname) == 0)
			return i;
	}

	elog(ERROR, "unknown column name %s", attname);
}
