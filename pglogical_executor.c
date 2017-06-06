/*-------------------------------------------------------------------------
 *
 * pglogical_executor.c
 * 		pglogical execurot related functions
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
create_estate_for_relation(Relation rel, bool hasTriggers)
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
	estate->es_range_table = list_make1(rte);

	if (hasTriggers)
		resultRelInfo->ri_TrigDesc = CopyTriggerDesc(rel->trigdesc);

	if (resultRelInfo->ri_TrigDesc)
	{
		int			n = resultRelInfo->ri_TrigDesc->numtriggers;

		resultRelInfo->ri_TrigFunctions = (FmgrInfo *)
			palloc0(n * sizeof(FmgrInfo));
		resultRelInfo->ri_TrigWhenExprs = (List **)
			palloc0(n * sizeof(List *));

		/* Triggers might need a slot */
		estate->es_trig_tuple_slot = ExecInitExtraTupleSlot(estate);
	}
	else
	{
		resultRelInfo->ri_TrigFunctions = NULL;
		resultRelInfo->ri_TrigWhenExprs = NULL;
	}

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
		char	   *nspname;
		char	   *relname;
		List	   *repsets;
		StringInfoData	json;

		/* Format the query. */
		nspname = get_namespace_name(get_rel_namespace(reloid));
		relname = get_rel_name(reloid);

		/* It's easier to construct json manually than via Jsonb API... */
		initStringInfo(&json);
		appendStringInfo(&json, "{\"schema_name\": ");
		escape_json(&json, nspname);
		appendStringInfo(&json, ",\"table_name\": ");
		escape_json(&json, relname);
		appendStringInfo(&json, "}");

		repsets = get_table_replication_sets(local_node->node->id, reloid);

		if (list_length(repsets))
		{
			List	   *repset_names = NIL;
			ListCell   *rlc;

			foreach (rlc, repsets)
			{
				PGLogicalRepSet	    *repset = (PGLogicalRepSet *) lfirst(rlc);
				repset_names = lappend(repset_names, pstrdup(repset->name));
			}

			/* Queue the truncate for replication. */
			queue_message(repset_names, GetUserId(),
						  QUEUE_COMMAND_TYPE_TRUNCATE, json.data);
		}
	}

	list_free(pglogical_truncated_tables);
	pglogical_truncated_tables = NIL;
}

static void
pglogical_ProcessUtility(Node *parsetree,
						 const char *queryString,
						 ProcessUtilityContext context,
						 ParamListInfo params,
						 DestReceiver *dest,
#ifdef XCP
						 bool sentToRemote,
#endif
						 char *completionTag)
{
	dropping_pglogical_obj = false;

	if (nodeTag(parsetree) == T_TruncateStmt)
		pglogical_start_truncate();

	if (nodeTag(parsetree) == T_DropStmt)
		pglogical_lastDropBehavior = ((DropStmt *)parsetree)->behavior;

	if (next_ProcessUtility_hook)
		next_ProcessUtility_hook(parsetree, queryString, context, params,
								 dest,
#ifdef XCP
								 sentToRemote,
#endif
								 completionTag);
	else
		standard_ProcessUtility(parsetree, queryString, context, params,
								dest,
#ifdef XCP
								sentToRemote,
#endif
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
			objectId == get_extension_oid(EXTENSION_NAME, false))
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
