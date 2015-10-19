/*-------------------------------------------------------------------------
 *
 * pglogical_commandfilter.c
 * 		filtering of allowed/not allowed commands
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_commandfilter.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"

#include "access/hash.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "access/xlog.h"

#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"

#include "executor/executor.h"

#include "parser/parsetree.h"

#include "storage/ipc.h"
#include "storage/proc.h"

#include "tcop/utility.h"

#include "utils/builtins.h"
#include "utils/rel.h"

#include "pglogical_node.h"
#include "pglogical_conflict.h"
#include "pglogical_worker.h"
#include "pglogical.h"

static ExecutorStart_hook_type PrevExecutorStart_hook = NULL;

static const char *
CreateWritableStmtTag(PlannedStmt *plannedstmt)
{
	if (plannedstmt->commandType == CMD_SELECT)
		return "UPDATE/DELETE"; /* SELECT INTO/WCTE */

	return CreateCommandTag((Node *) plannedstmt);
}

/*
 * The BDR ExecutorStart_hook that does DDL lock checks and forbids
 * writing into tables without replica identity index.
 *
 * Runs in all backends and workers.
 */
static void
PGLogicalExecutorStart(QueryDesc *queryDesc, int eflags)
{
	ListCell	   *l;
	List		   *rangeTable;
	PlannedStmt	   *plannedstmt = queryDesc->plannedstmt;
	PGLogicalNode  *node;

	/*
	 * Identify whether this is a modifying statement. We do this first because
	 * these checks are relatively cheap compared to checks on a node.
	 */
	if (plannedstmt != NULL &&
		(plannedstmt->hasModifyingCTE ||
		 plannedstmt->rowMarks != NIL))
		goto done;
	else if (queryDesc->operation != CMD_SELECT)
		goto done;

	/* plain INSERTs are ok */
	if (queryDesc->operation == CMD_INSERT &&
		!plannedstmt->hasModifyingCTE)
		goto done;

	node = get_local_node(true);

	/* The checks need to be done onnly on a pglogical node. */
	if (!node)
		goto done;

	/* Fail if query tries to UPDATE or DELETE any of tables without PK */
	rangeTable = plannedstmt->rtable;
	foreach(l, plannedstmt->resultRelations)
	{
		Index			rtei = lfirst_int(l);
		RangeTblEntry  *rte = rt_fetch(rtei, rangeTable);
		Relation		rel;

		rel = RelationIdGetRelation(rte->relid);

		/* Skip UNLOGGED and TEMP tables */
		if (!RelationNeedsWAL(rel))
		{
			RelationClose(rel);
			continue;
		}

		/*
		 * Since changes to pg_catalog aren't replicated directly there's
		 * no strong need to suppress direct UPDATEs on them. The usual
		 * rule of "it's dumb to modify the catalogs directly if you don't
		 * know what you're doing" applies.
		 */
		if (RelationGetNamespace(rel) == PG_CATALOG_NAMESPACE)
		{
			RelationClose(rel);
			continue;
		}

		/* Check of relation has replication index. */
		if (rel->rd_indexvalid == 0)
			RelationGetIndexList(rel);
		if (OidIsValid(rel->rd_replidindex))
		{
			RelationClose(rel);
			continue;
		}

		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot run %s on table %s because it does not have a PRIMARY KEY.",
						CreateWritableStmtTag(plannedstmt),
						RelationGetRelationName(rel)),
				 errhint("Add a PRIMARY KEY to the table")));

		RelationClose(rel);
	}

done:
	if (PrevExecutorStart_hook)
		(*PrevExecutorStart_hook) (queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}


void
pglogical_commandfilter_init(void)
{
	PrevExecutorStart_hook = ExecutorStart_hook;
	ExecutorStart_hook = PGLogicalExecutorStart;
}
