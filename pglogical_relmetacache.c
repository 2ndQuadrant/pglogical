/*-------------------------------------------------------------------------
 *
 * pglogical_relmetacache.c
 *		  Logical Replication relmetacache plugin
 *
 * Copyright (c) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_relmetacache.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "pglogical_output.h"

#include "utils/inval.h"
#include "utils/rel.h"

#include "pglogical_output_internal.h"
#include "pglogical_relmetacache.h"


static void relmeta_cache_callback(Datum arg, Oid relid);

/*
 * We need a global that invalidation callbacks can access because they
 * survive past the logical decoding context and therefore past our
 * local PGLogicalOutputData's lifetime when using the SQL interface. We
 * cannot just pass them a pointer to a palloc'd struct.
 *
 * If the hash table has been destroyed the callbacks know to do nothing.
 */
static HTAB *RelMetaCache = NULL;

/*
 * The callback persists across decoding sessions so we should only
 * register it once.
 */
static bool callback_registered = false;


/*
 * Initialize the relation metadata cache for a decoding session.
 *
 * The hash table is destoyed at the end of a decoding session. While
 * relcache invalidations still exist and will still be invoked, they
 * will just see the null hash table global and take no action.
 */
void
pglogical_init_relmetacache(MemoryContext decoding_context)
{
	HASHCTL	ctl;
	int		hash_flags;

	Assert(RelMetaCache == NULL);

	/* Make a new hash table for the cache */
	hash_flags = HASH_ELEM | HASH_CONTEXT;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(struct PGLRelMetaCacheEntry);
	ctl.hcxt = decoding_context;

#if PG_VERSION_NUM >= 90500
	hash_flags |= HASH_BLOBS;
#else
	ctl.hash = tag_hash;
	hash_flags |= HASH_FUNCTION;
#endif

	RelMetaCache = hash_create("pglogical relation metadata cache", 128,
								&ctl, hash_flags);

	Assert(RelMetaCache != NULL);

	/*
	 * Watch for invalidation events.
	 *
	 * We don't pass PGLogicalOutputData here because it's scoped to the
	 * individual decoding session, which with the SQL interface has a
	 * shorter lifetime than the relcache invalidation callback
	 * registration. We have no way to remove invalidation callbacks at
	 * the end of the decoding session or change them - so we have to
	 * cope with them being called later. If we wanted to pass the
	 * decoding private data we'd need to stash it in a global.
	 */
	if (!callback_registered)
	{
		CacheRegisterRelcacheCallback(relmeta_cache_callback, (Datum)0);
		callback_registered = true;
	}
}

/*
 * Relation metadata invalidation, for when a relcache invalidation
 * means that we need to resend table metadata to the client.
 */
static void
relmeta_cache_callback(Datum arg, Oid relid)
 {
	/*
	 * We can be called after decoding session teardown becaues the
	 * relcache callback isn't cleared. In that case there's no action
	 * to take.
	 */
	if (RelMetaCache == NULL)
		return;

	/*
	 * Nobody keeps pointers to entries in this hash table around so
	 * it's safe to directly HASH_REMOVE the entries as soon as they are
	 * invalidated. Finding them and flagging them invalid then removing
	 * them lazily might save some memory churn for tables that get
	 * repeatedly invalidated and re-sent, but it doesn't seem worth
	 * doing.
	 *
	 * Getting invalidations for relations that aren't in the table is
	 * entirely normal, since there's no way to unregister for an
	 * invalidation event. So we don't care if it's found or not.
	 */
	(void) hash_search(RelMetaCache, &relid, HASH_REMOVE, NULL);
 }

/*
 * Look up an entry, creating it if not found.
 *
 * Newly created entries are returned as is_cached=false. The API
 * hook can set is_cached to skip subsequent updates if it sent a
 * complete response that the client will cache.
 *
 * Returns true on a cache hit, false on a miss.
 */
bool
pglogical_cache_relmeta(struct PGLogicalOutputData *data,
		Relation rel, struct PGLRelMetaCacheEntry **entry)
{
	struct PGLRelMetaCacheEntry *hentry;
	bool found;

	if (data->relmeta_cache_size == 0)
	{
		/*
		 * If cache is disabled must treat every search as a miss
		 * and return no entry to populate.
		 */
		*entry = NULL;
		return false;
	}

	/* Find cached function info, creating if not found */
	hentry = (struct PGLRelMetaCacheEntry*) hash_search(RelMetaCache,
										 (void *)(&RelationGetRelid(rel)),
										 HASH_ENTER, &found);

	if (!found)
	{
		Assert(hentry->relid = RelationGetRelid(rel));
		hentry->is_cached = false;
	}

	Assert(hentry != NULL);

	*entry = hentry;
	return hentry->is_cached;
}


/*
 * Tear down the relation metadata cache at the end of a decoding
 * session.
 */
void
pglogical_destroy_relmetacache(void)
{
	if (RelMetaCache != NULL)
	{
		hash_destroy(RelMetaCache);
		RelMetaCache = NULL;
	}
}
