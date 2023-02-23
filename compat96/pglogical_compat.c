/*-------------------------------------------------------------------------
 *
 * pglogical_compat.c
 *              compatibility functions (mainly with different PG versions)
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *              pglogical_compat.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "pglogical_compat.h"

/*
 * CatalogTupleInsert - do heap and indexing work for a new catalog tuple
 *
 * Insert the tuple data in "tup" into the specified catalog relation.
 * The Oid of the inserted tuple is returned.
 *
 * This is a convenience routine for the common case of inserting a single
 * tuple in a system catalog; it inserts a new heap tuple, keeping indexes
 * current.  Avoid using it for multiple tuples, since opening the indexes
 * and building the index info structures is moderately expensive.
 * (Use CatalogTupleInsertWithInfo in such cases.)
 */
Oid
CatalogTupleInsert(Relation heapRel, HeapTuple tup)
{
	CatalogIndexState indstate;
	Oid			oid;

	indstate = CatalogOpenIndexes(heapRel);

	oid = simple_heap_insert(heapRel, tup);

	CatalogIndexInsert(indstate, tup);
	CatalogCloseIndexes(indstate);

	return oid;
}

/*
 * CatalogTupleUpdate - do heap and indexing work for updating a catalog tuple
 *
 * Update the tuple identified by "otid", replacing it with the data in "tup".
 *
 * This is a convenience routine for the common case of updating a single
 * tuple in a system catalog; it updates one heap tuple, keeping indexes
 * current.  Avoid using it for multiple tuples, since opening the indexes
 * and building the index info structures is moderately expensive.
 * (Use CatalogTupleUpdateWithInfo in such cases.)
 */
void
CatalogTupleUpdate(Relation heapRel, ItemPointer otid, HeapTuple tup)
{
	CatalogIndexState indstate;

	indstate = CatalogOpenIndexes(heapRel);

	simple_heap_update(heapRel, otid, tup);

	CatalogIndexInsert(indstate, tup);
	CatalogCloseIndexes(indstate);
}


/*
 * CatalogTupleDelete - do heap and indexing work for deleting a catalog tuple
 *
 * Delete the tuple identified by "tid" in the specified catalog.
 *
 * With Postgres heaps, there is no index work to do at deletion time;
 * cleanup will be done later by VACUUM.  However, callers of this function
 * shouldn't have to know that; we'd like a uniform abstraction for all
 * catalog tuple changes.  Hence, provide this currently-trivial wrapper.
 *
 * The abstraction is a bit leaky in that we don't provide an optimized
 * CatalogTupleDeleteWithInfo version, because there is currently nothing to
 * optimize.  If we ever need that, rather than touching a lot of call sites,
 * it might be better to do something about caching CatalogIndexState.
 */
void
CatalogTupleDelete(Relation heapRel, ItemPointer tid)
{
	simple_heap_delete(heapRel, tid);
}

/*
 * Copied wholesale from Postgres source since it's declared static
 * there until 10.2.
 *
 * Alternate version that allows caller to specify the elevel for any
 * error report.  If elevel < ERROR, returns NULL on any error.
 */
struct dirent *
ReadDirExtended(DIR *dir, const char *dirname, int elevel)
{
        struct dirent *dent;

        /* Give a generic message for AllocateDir failure, if caller didn't */
        if (dir == NULL)
        {
                ereport(elevel,
                                (errcode_for_file_access(),
                                 errmsg("could not open directory \"%s\": %m",
                                                dirname)));
                return NULL;
        }

        errno = 0;
        if ((dent = readdir(dir)) != NULL)
                return dent;

        if (errno)
                ereport(elevel,
                                (errcode_for_file_access(),
                                 errmsg("could not read directory \"%s\": %m",
                                                dirname)));
        return NULL;
}
