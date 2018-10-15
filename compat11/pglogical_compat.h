#ifndef PG_LOGICAL_COMPAT_H
#define PG_LOGICAL_COMPAT_H

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

#define PortalRun(portal, count, isTopLevel, dest, altdest, completionTag) \
	PortalRun(portal, count, isTopLevel, true, dest, altdest, completionTag)

#define ExecAlterExtensionStmt(stmt) \
	ExecAlterExtensionStmt(NULL, stmt)

#define pgl_replorigin_drop(roident) \
	replorigin_drop(roident, true)

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

#define PGLstandard_ProcessUtility(pstmt, queryString, context, params, queryEnv, dest, sentToRemote, completionTag) \
	standard_ProcessUtility(pstmt, queryString, context, params, queryEnv, dest, completionTag)

#define PGLnext_ProcessUtility_hook(pstmt, queryString, context, params, queryEnv, dest, sentToRemote, completionTag) \
	next_ProcessUtility_hook(pstmt, queryString, context, params, queryEnv, dest, completionTag)

#define PGLCreateTrigger(stmt, queryString, relOid, refRelOid, constraintOid, indexOid, isInternal) \
	CreateTrigger(stmt, queryString, relOid, refRelOid, constraintOid, indexOid, InvalidOid, InvalidOid, NULL, isInternal, false);

#define	PGLDoCopy(stmt, queryString, processed) DoCopy(NULL, stmt, -1, 0, processed)

#define PGLReplicationSlotCreate(name, db_specific, persistency) ReplicationSlotCreate(name, db_specific, persistency)

#ifdef PG2Q
#define ExecBRUpdateTriggers(estate, epqstate, relinfo, tupleid, fdw_trigtuple, slot) \
	ExecBRUpdateTriggers(estate, epqstate, relinfo, tupleid, fdw_trigtuple, slot, NULL)

#define ExecBRDeleteTriggers(estate, epqstate, relinfo, tupleid, fdw_trigtuple) \
	ExecBRDeleteTriggers(estate, epqstate, relinfo, tupleid, fdw_trigtuple, NULL)
#endif

#ifndef rbtxn_has_catalog_changes
#define rbtxn_has_catalog_changes(txn) (txn->has_catalog_changes)
#endif

/* ad7dbee368a */
#define ExecInitExtraTupleSlot(estate) \
	ExecInitExtraTupleSlot(estate, NULL)

#define ACL_OBJECT_RELATION OBJECT_TABLE
#define ACL_OBJECT_SEQUENCE OBJECT_SEQUENCE

#define DatumGetJsonb DatumGetJsonbP

#endif
