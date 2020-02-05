#ifndef PG_LOGICAL_COMPAT_H
#define PG_LOGICAL_COMPAT_H


#include "pgstat.h"
#include "catalog/indexing.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "replication/origin.h"
#include "storage/lwlock.h"

extern LWLockPadded *GetNamedLWLockTranche(const char *tranche_name);
extern void RequestNamedLWLockTranche(const char *tranche_name, int num_lwlocks);

#define GetConfigOptionByName(name, varname, missing_ok) \
(\
	AssertMacro(!missing_ok), \
	GetConfigOptionByName(name, varname) \
)

#define PGLCreateTrigger CreateTrigger

#define RawStmt Node

#define	PGLDoCopy(stmt, queryString, processed) DoCopy(stmt, queryString, processed)

#ifdef PGXC
#define PGLstandard_ProcessUtility(pstmt, queryString, context, params, queryEnv, dest, sentToRemote, completionTag) \
	standard_ProcessUtility(pstmt, queryString, context, params, dest, sentToRemote, completionTag)

#define PGLnext_ProcessUtility_hook(pstmt, queryString, context, params, queryEnv, dest, sentToRemote, completionTag) \
	next_ProcessUtility_hook(pstmt, queryString, context, params, dest, sentToRemote, completionTag)

#else

#define PGLstandard_ProcessUtility(pstmt, queryString, context, params, queryEnv, dest, sentToRemote, completionTag) \
	standard_ProcessUtility(pstmt, queryString, context, params, dest, completionTag)

#define PGLnext_ProcessUtility_hook(pstmt, queryString, context, params, queryEnv, dest, sentToRemote, completionTag) \
	next_ProcessUtility_hook(pstmt, queryString, context, params, dest, completionTag)
#endif

extern Oid CatalogTupleInsert(Relation heapRel, HeapTuple tup);
extern void CatalogTupleUpdate(Relation heapRel, ItemPointer otid, HeapTuple tup);
extern void CatalogTupleDelete(Relation heapRel, ItemPointer tid);

#define pgl_replorigin_drop(roident) \
	replorigin_drop(roident)

#define pgl_heap_attisnull(tup, attnum, tupledesc) \
	heap_attisnull(tup, attnum)

#define ALLOCSET_DEFAULT_SIZES \
		ALLOCSET_DEFAULT_MINSIZE, \
		ALLOCSET_DEFAULT_INITSIZE, \
		ALLOCSET_DEFAULT_MAXSIZE

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

#endif
