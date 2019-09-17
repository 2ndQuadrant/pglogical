/*-------------------------------------------------------------------------
 *
 * pglogical_executor.c
 * 		pglogical executor related functions
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_executor.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"

#include "access/hash.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "access/xlog.h"

#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_type.h"

#include "commands/extension.h"
#include "commands/trigger.h"

#include "executor/executor.h"

#include "nodes/nodeFuncs.h"

#include "optimizer/planner.h"

#include "parser/parse_coerce.h"

#include "tcop/utility.h"

#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "pglogical_node.h"
#include "pglogical_executor.h"
#include "pglogical_repset.h"
#include "pglogical_queue.h"
#include "pglogical_dependency.h"
#include "pglogical.h"

List *pglogical_truncated_tables = NIL;

static DropBehavior	pglogical_lastDropBehavior = DROP_RESTRICT;
static bool			dropping_pglogical_obj = false;
static object_access_hook_type next_object_access_hook = NULL;

static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;

EState *
create_estate_for_relation(Relation rel, bool forwrite)
{
	EState	   *estate;
	ResultRelInfo *resultRelInfo;
	RangeTblEntry *rte;


	/* Dummy range table entry needed by executor. */
	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(rel);
	rte->relkind = rel->rd_rel->relkind;

	resultRelInfo = makeNode(ResultRelInfo);
	InitResultRelInfo(resultRelInfo,
					  rel,
					  1,
					  0);

	/* Initialize executor state. */
	estate = CreateExecutorState();
	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;
#if PG_VERSION_NUM >= 110000 && SECONDQ_VERSION_NUM >= 103
	/* 2ndQPostgres 11 r1.3 changes executor API */
	estate->es_range_table = alist_add(NULL, rte);
#else
	estate->es_range_table = list_make1(rte);
#endif

	if (forwrite)
		resultRelInfo->ri_TrigDesc = CopyTriggerDesc(rel->trigdesc);

	if (resultRelInfo->ri_TrigDesc)
	{
		int			n = resultRelInfo->ri_TrigDesc->numtriggers;

		resultRelInfo->ri_TrigFunctions = (FmgrInfo *)
			palloc0(n * sizeof(FmgrInfo));
#if PG_VERSION_NUM >= 100000
		resultRelInfo->ri_TrigWhenExprs = (ExprState **)
			palloc0(n * sizeof(ExprState *));
#else
		resultRelInfo->ri_TrigWhenExprs = (List **)
			palloc0(n * sizeof(List *));
#endif

		/* Triggers might need a slot */
		estate->es_trig_tuple_slot = ExecInitExtraTupleSlot(estate);
	}
	else
	{
		resultRelInfo->ri_TrigFunctions = NULL;
		resultRelInfo->ri_TrigWhenExprs = NULL;
	}

	estate->es_output_cid = GetCurrentCommandId(forwrite);

	return estate;
}

ExprContext *
prepare_per_tuple_econtext(EState *estate, TupleDesc tupdesc)
{
	ExprContext	   *econtext;
	MemoryContext	oldContext;

	econtext = GetPerTupleExprContext(estate);

	oldContext = MemoryContextSwitchTo(estate->es_query_cxt);
	econtext->ecxt_scantuple = ExecInitExtraTupleSlot(estate);
	MemoryContextSwitchTo(oldContext);

	ExecSetSlotDescriptor(econtext->ecxt_scantuple, tupdesc);

	return econtext;
}

ExprState *
pglogical_prepare_row_filter(Node *row_filter)
{
	ExprState  *exprstate;
	Expr	   *expr;
	Oid			exprtype;

	exprtype = exprType(row_filter);
	expr = (Expr *) coerce_to_target_type(NULL,	/* no UNKNOWN params here */
										  row_filter, exprtype,
										  BOOLOID, -1,
										  COERCION_ASSIGNMENT,
										  COERCE_IMPLICIT_CAST,
										  -1);

	/* This should never happen but just to be sure. */
	if (expr == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("cannot cast the row_filter to boolean"),
			   errhint("You will need to rewrite the row_filter.")));

	expr = expression_planner(expr);
	exprstate = ExecInitExpr(expr, NULL);

	return exprstate;
}

static void
pglogical_start_truncate(void)
{
	pglogical_truncated_tables = NIL;
}

static void
pglogical_finish_truncate(void)
{
	ListCell	   *tlc;
	PGLogicalLocalNode *local_node;

	/* If this is not pglogical node, don't do anything. */
	local_node = get_local_node(false, true);
	if (!local_node || !list_length(pglogical_truncated_tables))
		return;

	foreach (tlc, pglogical_truncated_tables)
	{
		Oid			reloid = lfirst_oid(tlc);
		List		*reltargets;
		ListCell	*lc;

		/* And now prepare the messages for the queue */
		reltargets = get_table_replication_sets_targets(local_node->node->id, reloid);

		/*
		 * Compute a message for each unique (reloid,nsptarget,reltarget) triplet
		 */
		foreach (lc, reltargets)
		{
			StringInfoData		json;
			List				*repset_names = NIL;
			char				*nspname;
			char				*relname;
			PGLogicalRepSetRel	*t = (PGLogicalRepSetRel *) lfirst(lc);

			nspname = pstrdup(t->nsptarget);
			relname = pstrdup(t->reltarget);
			repset_names = lappend(repset_names, t->repset_name);

			initStringInfo(&json);
			appendStringInfoString(&json, "{\"schema_name\": ");
			escape_json(&json, nspname);
			appendStringInfoString(&json, ",\"table_name\": ");
			escape_json(&json, relname);
			appendStringInfo(&json, "}");

			/* Queue the truncate for replication. */
			queue_message(repset_names, GetUserId(),
						  QUEUE_COMMAND_TYPE_TRUNCATE, json.data);
		}
	}

	list_free(pglogical_truncated_tables);
	pglogical_truncated_tables = NIL;
}

static void
pglogical_ProcessUtility(
#if PG_VERSION_NUM >= 100000
						 PlannedStmt *pstmt,
#else
						 Node *pstmt,
#endif
						 const char *queryString,
						 ProcessUtilityContext context,
						 ParamListInfo params,
#if PG_VERSION_NUM >= 100000
						 QueryEnvironment *queryEnv,
#endif
						 DestReceiver *dest,
#ifdef XCP
						 bool sentToRemote,
#endif
						 char *completionTag)
{
#if PG_VERSION_NUM >= 100000
	Node	   *parsetree = pstmt->utilityStmt;
#else
	Node	   *parsetree = pstmt;
	#define		queryEnv NULL
#endif
#ifndef XCP
	#define		sentToRemote NULL
#endif

	dropping_pglogical_obj = false;

	if (nodeTag(parsetree) == T_TruncateStmt)
		pglogical_start_truncate();

	if (nodeTag(parsetree) == T_DropStmt)
		pglogical_lastDropBehavior = ((DropStmt *)parsetree)->behavior;

	/* There's no reason we should be in a long lived context here */
	Assert(CurrentMemoryContext != TopMemoryContext
		   && CurrentMemoryContext != CacheMemoryContext);

	if (next_ProcessUtility_hook)
		PGLnext_ProcessUtility_hook(pstmt, queryString, context, params,
									queryEnv, dest,
									sentToRemote,
									completionTag);
	else
		PGLstandard_ProcessUtility(pstmt, queryString, context, params,
								   queryEnv, dest,
								   sentToRemote,
								   completionTag);

	if (nodeTag(parsetree) == T_TruncateStmt)
		pglogical_finish_truncate();
}


/*
 * Handle object drop.
 *
 * Calls to dependency tracking code.
 */
static void
pglogical_object_access(ObjectAccessType access,
						Oid classId,
						Oid objectId,
						int subId,
						void *arg)
{
	if (next_object_access_hook)
		(*next_object_access_hook) (access, classId, objectId, subId, arg);

	if (access == OAT_DROP)
	{
		ObjectAccessDrop   *drop_arg = (ObjectAccessDrop *) arg;
		ObjectAddress		object;
		DropBehavior		behavior;

		/* No need to check for internal deletions. */
		if ((drop_arg->dropflags & PERFORM_DELETION_INTERNAL) != 0)
			return;

		/* Dropping pglogical itself? */
		if (classId == ExtensionRelationId &&
			objectId == get_extension_oid(EXTENSION_NAME, true) &&
			objectId != InvalidOid /* Should not happen but check anyway */)
			dropping_pglogical_obj = true;

		/* Dropping relation within pglogical? */
		if (classId == RelationRelationId)
		{
			Oid			relnspoid;
			Oid			pglnspoid;

			pglnspoid = get_namespace_oid(EXTENSION_NAME, true);
			relnspoid = get_rel_namespace(objectId);

			if (pglnspoid == relnspoid)
				dropping_pglogical_obj = true;
		}

		/*
		 * Don't do extra dependency checks for internal objects, those
		 * should be handled by Postgres.
		 */
		if (dropping_pglogical_obj)
			return;

		/* No local node? */
		if (!get_local_node(false, true))
			return;

		ObjectAddressSubSet(object, classId, objectId, subId);

		if (SessionReplicationRole == SESSION_REPLICATION_ROLE_REPLICA)
			behavior = DROP_CASCADE;
		else
			behavior = pglogical_lastDropBehavior;

		pglogical_checkDependency(&object, behavior);
	}
}

void
pglogical_executor_init(void)
{
	next_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = pglogical_ProcessUtility;

	/* Object access hook */
	next_object_access_hook = object_access_hook;
	object_access_hook = pglogical_object_access;
}
