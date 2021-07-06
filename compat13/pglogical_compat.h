#ifndef PG_LOGICAL_COMPAT_H
#define PG_LOGICAL_COMPAT_H

#include "access/amapi.h"
#include "access/heapam.h"
#include "access/table.h"
#include "access/tableam.h"
#include "replication/origin.h"
#include "utils/varlena.h"

#define WaitLatchOrSocket(latch, wakeEvents, sock, timeout) \
	WaitLatchOrSocket(latch, wakeEvents, sock, timeout, PG_WAIT_EXTENSION)

#define WaitLatch(latch, wakeEvents, timeout) \
	WaitLatch(latch, wakeEvents, timeout, PG_WAIT_EXTENSION)

#define GetCurrentIntegerTimestamp() GetCurrentTimestamp()

#define pg_analyze_and_rewrite(parsetree, query_string, paramTypes, numParams) \
	pg_analyze_and_rewrite(parsetree, query_string, paramTypes, numParams, NULL)

#define CreateCommandTag(raw_parsetree) \
	CreateCommandTag(raw_parsetree->stmt)

#define PortalRun(portal, count, isTopLevel, dest, altdest, qc) \
	PortalRun(portal, count, isTopLevel, true, dest, altdest, qc)

#define ExecAlterExtensionStmt(stmt) \
	ExecAlterExtensionStmt(NULL, stmt)

/*
 * Pg 11 adds an argument here.  We don't need to special-case 2ndQPostgres
 * anymore because it adds a separate ExecBRDeleteTriggers2 now, so this only
 * handles the stock Pg11 change.
 */ 
#define ExecBRDeleteTriggers(estate, epqstate, relinfo, tupleid, fdw_trigtuple) \
 	ExecBRDeleteTriggers(estate, epqstate, relinfo, tupleid, fdw_trigtuple, NULL)

#undef ExecEvalExpr
#define ExecEvalExpr(expr, econtext, isNull, isDone) \
	((*(expr)->evalfunc) (expr, econtext, isNull))

#define Form_pg_sequence Form_pg_sequence_data

#define InitResultRelInfo(resultRelInfo, resultRelationDesc, resultRelationIndex, instrument_options) \
	InitResultRelInfo(resultRelInfo, resultRelationDesc, resultRelationIndex, NULL, instrument_options)

#define ExecARUpdateTriggers(estate, relinfo, tupleid, fdw_trigtuple, newslot, recheckIndexes) \
	ExecARUpdateTriggers(estate, relinfo, tupleid, fdw_trigtuple, newslot, recheckIndexes, NULL)

#define ExecARInsertTriggers(estate, relinfo, slot, recheckIndexes) \
	ExecARInsertTriggers(estate, relinfo, slot, recheckIndexes, NULL)

#define ExecARDeleteTriggers(estate, relinfo, tupleid, fdw_trigtuple) \
	ExecARDeleteTriggers(estate, relinfo, tupleid, fdw_trigtuple, NULL)

#define makeDefElem(name, arg) makeDefElem(name, arg, -1)

#define PGLstandard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, sentToRemote, qc) \
	standard_ProcessUtility(pstmt, queryString, context, params, queryEnv, dest, qc)

#define PGLnext_ProcessUtility_hook(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, sentToRemote, qc) \
	next_ProcessUtility_hook(pstmt, queryString, context, params, queryEnv, dest, qc)

#define PGLCreateTrigger(stmt, queryString, relOid, refRelOid, constraintOid, indexOid, isInternal) \
	CreateTrigger(stmt, queryString, relOid, refRelOid, constraintOid, indexOid, InvalidOid, InvalidOid, NULL, isInternal, false);

#define	PGLDoCopy(stmt, queryString, processed) \
	do \
	{ \
		ParseState* pstate = make_parsestate(NULL); \
		DoCopy(pstate, stmt, -1, 0, processed); \
		free_parsestate(pstate); \
	} while (false);

#define PGLReplicationSlotCreate(name, db_specific, persistency) ReplicationSlotCreate(name, db_specific, persistency)

#ifndef rbtxn_has_catalog_changes
#define rbtxn_has_catalog_changes(txn) (txn->has_catalog_changes)
#endif

/* ad7dbee368a */
#define ExecInitExtraTupleSlot(estate) \
	ExecInitExtraTupleSlot(estate, NULL, &TTSOpsHeapTuple)

#define ACL_OBJECT_RELATION OBJECT_TABLE
#define ACL_OBJECT_SEQUENCE OBJECT_SEQUENCE

#define DatumGetJsonb DatumGetJsonbP

#define pgl_heap_attisnull(tup, attnum, tupledesc) \
	heap_attisnull(tup, attnum, tupledesc)

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
