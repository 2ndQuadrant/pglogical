#ifndef PG_LOGICAL_COMPAT_H
#define PG_LOGICAL_COMPAT_H

#include "access/heapam.h"
#include "catalog/heap.h"

#define RawStmt Node

#define PG_WAIT_EXTENSION 0

#define PGLCreateTrigger CreateTrigger

#define WaitLatchOrSocket(latch, wakeEvents, sock, timeout, wait_event_info) \
	WaitLatchOrSocket(latch, wakeEvents, sock, timeout);

#define WaitLatch(latch, wakeEvents, timeout, wait_event_info) \
	WaitLatch(latch, wakeEvents, timeout)

#define castNode(_type_, nodeptr) ((_type_ *) (nodeptr))

#define pg_analyze_and_rewrite(parsetree, query_string, paramTypes, numParams, queryEnv) \
	pg_analyze_and_rewrite((Node *) parsetree, query_string, paramTypes, numParams)

#define PortalRun(portal, count, isTopLevel, run_once, dest, altdest, completionTag) \
	PortalRun(portal, count, isTopLevel, dest, altdest, completionTag)

extern Oid	CatalogTupleInsert(Relation heapRel, HeapTuple tup);
extern void CatalogTupleUpdate(Relation heapRel, ItemPointer otid,
				   HeapTuple tup);
extern void CatalogTupleDelete(Relation heapRel, ItemPointer tid);

#undef ExecEvalExpr
#define ExecEvalExpr(expr, econtext, isNull) \
	((*(expr)->evalfunc) (expr, econtext, isNull, NULL))

#define Form_pg_sequence_data Form_pg_sequence

#define	PGLDoCopy(stmt, queryString, stmt_location, stmt_len, processed) DoCopy(stmt, queryString, processed)

#define ExecAlterExtensionStmt(pstate, stmt) ExecAlterExtensionStmt(stmt)

#define standard_ProcessUtility(pstmt, queryString, context, params, queryEnv, dest, completionTag) \
	standard_ProcessUtility((Node *)pstmt, queryString, context, params, dest, completionTag)

#define next_ProcessUtility_hook(pstmt, queryString, context, params, queryEnv, dest, completionTag) \
	next_ProcessUtility_hook((Node *)pstmt, queryString, context, params, dest, completionTag)

#define InitResultRelInfo(resultRelInfo, resultRelationDesc, resultRelationIndex, partition_root, instrument_options) \
	InitResultRelInfo(resultRelInfo, resultRelationDesc, resultRelationIndex, instrument_options)

#define makeDefElem(name, arg, location) makeDefElem(name, arg)

#endif
