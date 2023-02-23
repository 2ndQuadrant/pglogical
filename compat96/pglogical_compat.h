#ifndef PG_LOGICAL_COMPAT_H
#define PG_LOGICAL_COMPAT_H

#include "pgstat.h"
#include "catalog/indexing.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "replication/origin.h"

#define PGLCreateTrigger CreateTrigger

#define RawStmt Node

#define	PGLDoCopy(stmt, queryString, processed) DoCopy(stmt, queryString, processed)

#ifdef PGXC
#define PGLstandard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, sentToRemote, qc) \
	standard_ProcessUtility(pstmt, queryString, context, params, dest, sentToRemote, qc)

#define PGLnext_ProcessUtility_hook(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, sentToRemote, qc) \
	next_ProcessUtility_hook(pstmt, queryString, context, params, dest, sentToRemote, qc)

#else

#define PGLstandard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, sentToRemote, qc) \
	standard_ProcessUtility(pstmt, queryString, context, params, dest, qc)

#define PGLnext_ProcessUtility_hook(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, sentToRemote, qc) \
	next_ProcessUtility_hook(pstmt, queryString, context, params, dest, qc)
#endif

extern Oid CatalogTupleInsert(Relation heapRel, HeapTuple tup);
extern void CatalogTupleUpdate(Relation heapRel, ItemPointer otid, HeapTuple tup);
extern void CatalogTupleDelete(Relation heapRel, ItemPointer tid);

extern struct dirent * ReadDirExtended(DIR *dir, const char *dirname, int elevel);

/*
 * nowait=true is the standard behavior.  If nowait=false is called,
 * we ignore that, meaning we don't wait even if the caller asked to
 * wait.  This could lead to spurious errors in race conditions, but
 * it's the best we can do.
 */
#define replorigin_drop(roident, nowait) replorigin_drop(roident)

#define pgl_heap_attisnull(tup, attnum, tupledesc) \
	heap_attisnull(tup, attnum)

#ifndef rbtxn_has_catalog_changes
#define rbtxn_has_catalog_changes(txn) (txn->has_catalog_changes)
#endif

#define IndexRelationGetNumberOfKeyAttributes(rel) RelationGetNumberOfAttributes(rel)

/* deprecated in PG12, removed in PG13 */
#define table_open(r, l)		heap_open(r, l)
#define table_openrv(r, l)		heap_openrv(r, l)
#define table_openrv_extended(r, l, m)	heap_openrv_extended(r, l, m)
#define table_close(r, l)		heap_close(r, l)

/* 29c94e03c7 */
#define ExecStoreHeapTuple(tuple, slot, shouldFree) ExecStoreTuple(tuple, slot, InvalidBuffer, shouldFree)

/* c2fe139c20 */
#define TableScanDesc HeapScanDesc
#define table_beginscan(relation, snapshot, nkeys, keys) heap_beginscan(relation, snapshot, nkeys, keys)
#define table_beginscan_catalog(relation, nkeys, keys) heap_beginscan_catalog(relation, nkeys, keys)
#define table_endscan(scan) heap_endscan(scan)

/* 578b229718e8 */
#define CreateTemplateTupleDesc(natts) \
	CreateTemplateTupleDesc(natts, false)

/* 2f9661311b83 */
#define CommandTag const char *
#define QueryCompletion char

/* 6aba63ef3e60 */
#define pg_plan_queries(querytrees, query_string, cursorOptions, boundParams) \
	pg_plan_queries(querytrees, cursorOptions, boundParams)

/* cd142e032ebd50ec7974b3633269477c2c72f1cc removed replorigin_drop */
inline static void
replorigin_drop_by_name(char *name, bool missing_ok, bool nowait)
{
	RepOriginId	originid;

	originid = replorigin_by_name(name, missing_ok);
	if (originid != InvalidRepOriginId)
		replorigin_drop(originid, nowait);
}

#endif
