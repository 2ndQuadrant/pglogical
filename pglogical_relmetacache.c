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
#include "pglogical_relmetacache.h"

#include "utils/catcache.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/rel.h"

static void relmeta_cache_callback(Datum arg, Oid relid);

/*
 * We need a global hash table that invalidation callbacks can
 * access because they survive past the logical decoding context and
 * therefore past our local PGLogicalOutputData's lifetime when
 * using the SQL interface. We cannot just pass them a pointer to a
 * palloc'd struct.
 */
static HTAB *RelMetaCache = NULL;


/*
 * Initialize the relation metadata cache if not already initialized.
 *
 * Purge it if it already exists.
 *
 * The hash table its self must be in CacheMemoryContext or TopMemoryContext
 * since it persists outside the decoding session.
 */
void
pglogical_init_relmetacache(void)
{
	HASHCTL	ctl;

	if (RelMetaCache == NULL)
	{
		/* first time, init the cache */
		int hash_flags = HASH_ELEM | HASH_CONTEXT;

		/* Make sure we've initialized CacheMemoryContext. */
		if (CacheMemoryContext == NULL)
			CreateCacheMemoryContext();

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(struct PGLRelMetaCacheEntry);
		/* safe to allocate to CacheMemoryContext since it's never reset */
		ctl.hcxt = CacheMemoryContext;

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
		 * individual decoding session, which with the SQL interface has a shorter
		 * lifetime than the relcache invalidation callback registration. We have
		 * no way to remove invalidation callbacks at the end of the decoding
		 * session so we have to cope with them being called later.
		 */
		CacheRegisterRelcacheCallback(relmeta_cache_callback, (Datum)0);
	}
	else
	{
		/*
		 * On re-init we must flush the cache since there could be
		 * dangling pointers to api_private data in the freed
		 * decoding context of a prior session. We could go through
		 * and clear them and the is_cached flag but it seems best
		 * to have a clean slate.
		 */
		HASH_SEQ_STATUS status;
		struct PGLRelMetaCacheEntry *hentry;
		hash_seq_init(&status, RelMetaCache);

		while ((hentry = (struct PGLRelMetaCacheEntry*) hash_seq_search(&status)) != NULL)
		{
			if (hash_search(RelMetaCache,
						(void *) &hentry->relid,
						HASH_REMOVE, NULL) == NULL)
				elog(ERROR, "pglogical RelMetaCache hash table corrupted");
		}

		return;
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
	 * Nobody keeps pointers to entries in this hash table around so
	 * it's safe to directly HASH_REMOVE the entries as soon as they are
	 * invalidated. Finding them and flagging them invalid then removing
	 * them lazily might save some memory churn for tables that get
	 * repeatedly invalidated and re-sent, but it dodesn't seem worth
	 * doing.
	 *
	 * Getting invalidations for relations that aren't in the table is
	 * entirely normal, since there's no way to unregister for an
	 * invalidation event. So we don't care if it's found or not.
	 */
	(void) hash_search(RelMetaCache, &relid, HASH_REMOVE, NULL);
 }

/*
 * Look up an entry, creating it not found.
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
		hentry->api_private = NULL;
	}

	Assert(hentry != NULL);

	*entry = hentry;
	return hentry->is_cached;
}


/*
 * Tear down the relation metadata cache.
 *
 * Do *not* call this at decoding shutdown. The hash table must
 * continue to exist so that relcache invalidation callbacks can
 * continue to reference it after a SQL decoding session finishes.
 * It must be called at backend shutdown only.
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
