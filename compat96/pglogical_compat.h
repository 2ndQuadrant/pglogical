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

#define standard_ProcessUtility(pstmt, queryString, context, params, queryEnv, dest, completionTag) \
	standard_ProcessUtility((Node *)pstmt, queryString, context, params, dest, completionTag)

#define next_ProcessUtility_hook(pstmt, queryString, context, params, queryEnv, dest, completionTag) \
	next_ProcessUtility_hook((Node *)pstmt, queryString, context, params, dest, completionTag)

extern Oid CatalogTupleInsert(Relation heapRel, HeapTuple tup);
extern void CatalogTupleUpdate(Relation heapRel, ItemPointer otid, HeapTuple tup);
extern void CatalogTupleDelete(Relation heapRel, ItemPointer tid);

#endif
