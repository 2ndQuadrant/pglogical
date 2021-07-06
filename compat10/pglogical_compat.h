#ifndef PG_LOGICAL_COMPAT_H
#define PG_LOGICAL_COMPAT_H

#include "pgstat.h"
#include "catalog/indexing.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "replication/origin.h"
#include "utils/varlena.h"

#define PGLCreateTrigger CreateTrigger

#define WaitLatchOrSocket(latch, wakeEvents, sock, timeout) \
	WaitLatchOrSocket(latch, wakeEvents, sock, timeout, PG_WAIT_EXTENSION)

#define WaitLatch(latch, wakeEvents, timeout) \
	WaitLatch(latch, wakeEvents, timeout, PG_WAIT_EXTENSION)

#define GetCurrentIntegerTimestamp() GetCurrentTimestamp()

#define	PGLDoCopy(stmt, queryString, processed) \
	do \
	{ \
		ParseState* pstate = make_parsestate(NULL); \
		DoCopy(pstate, stmt, -1, 0, processed); \
		free_parsestate(pstate); \
	} while (false);

#define pg_analyze_and_rewrite(parsetree, query_string, paramTypes, numParams) \
	pg_analyze_and_rewrite(parsetree, query_string, paramTypes, numParams, NULL)

#define CreateCommandTag(raw_parsetree) \
	CreateCommandTag(raw_parsetree->stmt)

#define PortalRun(portal, count, isTopLevel, dest, altdest, qc) \
	PortalRun(portal, count, isTopLevel, true, dest, altdest, qc)

#define ExecAlterExtensionStmt(stmt) \
	ExecAlterExtensionStmt(NULL, stmt)

#undef ExecEvalExpr
#define ExecEvalExpr(expr, econtext, isNull, isDone) \
	((*(expr)->evalfunc) (expr, econtext, isNull))

#define Form_pg_sequence Form_pg_sequence_data

#define InitResultRelInfo(resultRelInfo, resultRelationDesc, resultRelationIndex, instrument_options) \
	InitResultRelInfo(resultRelInfo, resultRelationDesc, resultRelationIndex, NULL, instrument_options)

#define ExecARUpdateTriggers(estate, relinfo, tupleid, fdw_trigtuple, newtuple, recheckIndexes) \
	ExecARUpdateTriggers(estate, relinfo, tupleid, fdw_trigtuple, newtuple, recheckIndexes, NULL)

#define ExecARInsertTriggers(estate, relinfo, trigtuple, recheckIndexes) \
	ExecARInsertTriggers(estate, relinfo, trigtuple, recheckIndexes, NULL)

#define ExecARDeleteTriggers(estate, relinfo, tupleid, fdw_trigtuple) \
	ExecARDeleteTriggers(estate, relinfo, tupleid, fdw_trigtuple, NULL)

#define makeDefElem(name, arg) makeDefElem(name, arg, -1)

#define PGLstandard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, sentToRemote, qc) \
	standard_ProcessUtility(pstmt, queryString, context, params, queryEnv, dest, qc)

#define PGLnext_ProcessUtility_hook(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, sentToRemote, qc) \
	next_ProcessUtility_hook(pstmt, queryString, context, params, queryEnv, dest, qc)

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
