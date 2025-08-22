#ifndef PG_LOGICAL_COMPAT_H
#define PG_LOGICAL_COMPAT_H

#include "access/amapi.h"
#include "access/heapam.h"
#include "access/table.h"
#include "access/tableam.h"
#include "utils/varlena.h"

#define WaitLatchOrSocket(latch, wakeEvents, sock, timeout) \
	WaitLatchOrSocket(latch, wakeEvents, sock, timeout, PG_WAIT_EXTENSION)

#define WaitLatch(latch, wakeEvents, timeout) \
	WaitLatch(latch, wakeEvents, timeout, PG_WAIT_EXTENSION)

#define GetCurrentIntegerTimestamp() GetCurrentTimestamp()

#define pg_analyze_and_rewrite(parsetree, query_string, paramTypes, numParams) \
	pg_analyze_and_rewrite_fixedparams(parsetree, query_string, paramTypes, numParams, NULL)

#define CreateCommandTag(raw_parsetree) \
	CreateCommandTag(raw_parsetree->stmt)

#define ExecAlterExtensionStmt(stmt) \
	ExecAlterExtensionStmt(NULL, stmt)

#undef ExecEvalExpr
#define ExecEvalExpr(expr, econtext, isNull, isDone) \
	((*(expr)->evalfunc) (expr, econtext, isNull))

#define Form_pg_sequence Form_pg_sequence_data

#define InitResultRelInfo(resultRelInfo, resultRelationDesc, resultRelationIndex, instrument_options) \
	InitResultRelInfo(resultRelInfo, resultRelationDesc, resultRelationIndex, NULL, instrument_options)

#define ExecARUpdateTriggers(estate, relinfo, tupleid, fdw_trigtuple, newslot, recheckIndexes) \
	ExecARUpdateTriggers(estate, relinfo, NULL, NULL, tupleid, fdw_trigtuple, newslot, recheckIndexes, NULL, false)

#define ExecARInsertTriggers(estate, relinfo, slot, recheckIndexes) \
	ExecARInsertTriggers(estate, relinfo, slot, recheckIndexes, NULL)

#define ExecARDeleteTriggers(estate, relinfo, tupleid, fdw_trigtuple) \
	ExecARDeleteTriggers(estate, relinfo, tupleid, fdw_trigtuple, NULL, false)

#define makeDefElem(name, arg) makeDefElem(name, arg, -1)

#define PGLstandard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, sentToRemote, qc) \
	standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc)

#define PGLnext_ProcessUtility_hook(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, sentToRemote, qc) \
	next_ProcessUtility_hook(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc)

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

/* 2a10fdc4307a667883f7a3369cb93a721ade9680 */
#define getObjectDescription(object) getObjectDescription(object, false)

/* e997a0c642860a96df0151cbeccfecbdf0450d08 */
#define GetFlushRecPtr() GetFlushRecPtr(NULL)

/* 216a784829c2c5f03ab0c43e009126cbb819e9b2 */
#define PGLreplorigin_session_setup(node) replorigin_session_setup(node, 0)

/* 19d8e2308bc51ec4ab993ce90077342c915dd116 */
#define ExecInsertIndexTuples(resultRelInfo, slot, estate, update, noDupErr, specConflict, arbiterIndexes) \
	ExecInsertIndexTuples(resultRelInfo, slot, estate, update, noDupErr, specConflict, arbiterIndexes, false)

/* 70b42f2790292cc30aa07563f343f7ba6749af01 */
#define EvalPlanQualInit(epqstate, parentestate, subplan, auxrowmarks, epqParam) \
	EvalPlanQualInit(epqstate, parentestate, subplan, auxrowmarks, epqParam, NIL)

/* 6a72c42fd5af7ada49584694f543eb06dddb4a87 */
#define MemoryContextResetAndDeleteChildren(ctx) MemoryContextReset(ctx)

/* 75680c3d805e2323cd437ac567f0677fdfc7b680 */
#define SPI_push() ((void) 0)
#define SPI_pop()  ((void) 0)
#define tuplestore_donestoring(state)  ((void) 0)

/* 89e5ef7e21812916c9cf9fcf56e45f0f74034656 */
typedef enum ObjectClass
{
   OCLASS_CLASS,               /* pg_class */
   OCLASS_PROC,                /* pg_proc */
   OCLASS_TYPE,                /* pg_type */
   OCLASS_CAST,                /* pg_cast */
   OCLASS_COLLATION,           /* pg_collation */
   OCLASS_CONSTRAINT,          /* pg_constraint */
   OCLASS_CONVERSION,          /* pg_conversion */
   OCLASS_DEFAULT,             /* pg_attrdef */
   OCLASS_LANGUAGE,            /* pg_language */
   OCLASS_LARGEOBJECT,         /* pg_largeobject */
   OCLASS_OPERATOR,            /* pg_operator */
   OCLASS_OPCLASS,             /* pg_opclass */
   OCLASS_OPFAMILY,            /* pg_opfamily */
   OCLASS_AM,                  /* pg_am */
   OCLASS_AMOP,                /* pg_amop */
   OCLASS_AMPROC,              /* pg_amproc */
   OCLASS_REWRITE,             /* pg_rewrite */
   OCLASS_TRIGGER,             /* pg_trigger */
   OCLASS_SCHEMA,              /* pg_namespace */
   OCLASS_STATISTIC_EXT,       /* pg_statistic_ext */
   OCLASS_TSPARSER,            /* pg_ts_parser */
   OCLASS_TSDICT,              /* pg_ts_dict */
   OCLASS_TSTEMPLATE,          /* pg_ts_template */
   OCLASS_TSCONFIG,            /* pg_ts_config */
   OCLASS_ROLE,                /* pg_authid */
   OCLASS_ROLE_MEMBERSHIP,     /* pg_auth_members */
   OCLASS_DATABASE,            /* pg_database */
   OCLASS_TBLSPACE,            /* pg_tablespace */
   OCLASS_FDW,                 /* pg_foreign_data_wrapper */
   OCLASS_FOREIGN_SERVER,      /* pg_foreign_server */
   OCLASS_USER_MAPPING,        /* pg_user_mapping */
   OCLASS_DEFACL,              /* pg_default_acl */
   OCLASS_EXTENSION,           /* pg_extension */
   OCLASS_EVENT_TRIGGER,       /* pg_event_trigger */
   OCLASS_PARAMETER_ACL,       /* pg_parameter_acl */
   OCLASS_POLICY,              /* pg_policy */
   OCLASS_PUBLICATION,         /* pg_publication */
   OCLASS_PUBLICATION_NAMESPACE,   /* pg_publication_namespace */
   OCLASS_PUBLICATION_REL,     /* pg_publication_rel */
   OCLASS_SUBSCRIPTION,        /* pg_subscription */
   OCLASS_TRANSFORM            /* pg_transform */
} ObjectClass;

#define LAST_OCLASS        OCLASS_TRANSFORM

/* 0fbceae841cb5a31b13d3f284ac8fdd19822eceb added "instrument" param */
#define index_beginscan(heapRelation, indexRelation, snapshot, nkeys, norderbys) \
	index_beginscan(heapRelation, indexRelation, snapshot, NULL, nkeys, norderbys)

/*
 * 27c7c11366f72b1933e298481954a24c742036de added is_merge_update, which is
 * false for us just like in core execReplication.c.
 */
#define ExecBRDeleteTriggers(estate, epqstate, relinfo, tupleid, fdw_trigtuple) \
	ExecBRDeleteTriggers(estate, epqstate, relinfo, tupleid, fdw_trigtuple, NULL, NULL, NULL, false)
#define ExecBRUpdateTriggers(estate, epqstate, relinfo, tupleid, fdw_trigtuple, slot) \
	ExecBRUpdateTriggers(estate, epqstate, relinfo, tupleid, fdw_trigtuple, slot, NULL, NULL, false)

/* cbc127917e04a978a788b8bc9d35a70244396d5b added unpruned_relids */
#define ExecInitRangeTable(estate, rangeTable, permInfos) \
	ExecInitRangeTable(estate, rangeTable, permInfos, bms_make_singleton(1))

#endif
