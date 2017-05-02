#ifndef PG_LOGICAL_COMPAT_H
#define PG_LOGICAL_COMPAT_H

#include <signal.h>

#include "catalog/heap.h"
#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "catalog/heap.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_trigger.h"
#include "commands/trigger.h"
#include "nodes/pg_list.h"
#include "storage/lwlock.h"
#include "utils/array.h"

/* 9.4 lacks PG_*_MAX */
#ifndef PG_UINT32_MAX
#define PG_UINT32_MAX	(0xFFFFFFFF)
#endif

#ifndef PG_INT32_MAX
#define PG_INT32_MAX	(0x7FFFFFFF)
#endif

#ifndef PG_INT32_MIN
#define PG_INT32_MIN	(-0x7FFFFFFF-1)
#endif

#ifndef PG_UINT16_MAX
#define PG_UINT16_MAX	(0xFFFF)
#endif

#define RawStmt Node

#define PG_WAIT_EXTENSION 0

extern PGDLLIMPORT XLogRecPtr XactLastCommitEnd;

extern void BackgroundWorkerInitializeConnectionByOid(Oid dboid, Oid useroid);

extern ArrayType *strlist_to_textarray(List *list);

extern LWLockPadded *GetNamedLWLockTranche(const char *tranche_name);
extern void RequestNamedLWLockTranche(const char *tranche_name, int num_lwlocks);

#define GetConfigOptionByName(name, varname, missing_ok) \
(\
	AssertMacro(!missing_ok), \
	GetConfigOptionByName(name, varname) \
)

/* missing macros in 9.4 */
#define ObjectAddressSubSet(addr, class_id, object_id, object_sub_id) \
	do { \
		(addr).classId = (class_id); \
		(addr).objectId = (object_id); \
		(addr).objectSubId = (object_sub_id); \
	} while (0)

#define ObjectAddressSet(addr, class_id, object_id) \
	ObjectAddressSubSet(addr, class_id, object_id, 0)

static inline ObjectAddress
PGLCreateTrigger(CreateTrigStmt *stmt, const char *queryString,
				 Oid relOid, Oid refRelOid, Oid constraintOid, Oid indexOid,
				 bool isInternal)
{
	ObjectAddress myself;
	myself.classId = TriggerRelationId;
	myself.objectId = CreateTrigger(stmt, queryString, relOid, refRelOid, constraintOid, indexOid, isInternal);
	myself.objectSubId = 0;
	return myself;
}

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
