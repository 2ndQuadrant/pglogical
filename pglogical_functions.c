/*-------------------------------------------------------------------------
 *
 * pglogical_functions.c
 *		pglogical SQL visible interfaces
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pglogical_functions.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"

#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"

#include "commands/event_trigger.h"
#include "commands/trigger.h"

#include "executor/spi.h"

#include "miscadmin.h"

#include "nodes/makefuncs.h"

#include "replication/reorderbuffer.h"

#include "storage/latch.h"
#include "storage/proc.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/json.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "pglogical_node.h"
#include "pglogical_queue.h"
#include "pglogical_repset.h"
#include "pglogical_worker.h"

#include "pglogical.h"

/* Node management. */
PG_FUNCTION_INFO_V1(pglogical_create_provider);
PG_FUNCTION_INFO_V1(pglogical_drop_provider);
PG_FUNCTION_INFO_V1(pglogical_create_subscriber);
PG_FUNCTION_INFO_V1(pglogical_drop_subscriber);
PG_FUNCTION_INFO_V1(pglogical_wait_for_subscriber_ready);

/* Replication set manipulation. */
PG_FUNCTION_INFO_V1(pglogical_create_replication_set);
PG_FUNCTION_INFO_V1(pglogical_drop_replication_set);
PG_FUNCTION_INFO_V1(pglogical_replication_set_add_table);
PG_FUNCTION_INFO_V1(pglogical_replication_set_remove_table);

/* DDL */
PG_FUNCTION_INFO_V1(pglogical_replicate_ddl_command);
PG_FUNCTION_INFO_V1(pglogical_queue_truncate);
PG_FUNCTION_INFO_V1(pglogical_truncate_trigger_add);
PG_FUNCTION_INFO_V1(pglogical_dependency_check_trigger);

/*
 * Create new provider
 */
Datum
pglogical_create_provider(PG_FUNCTION_ARGS)
{
	PGLogicalProvider	provider;

	provider.id = InvalidOid;
	provider.name = NameStr(*PG_GETARG_NAME(0));

	create_provider(&provider);

	PG_RETURN_OID(provider.id);
}

/*
 * Drop the named node.
 */
Datum
pglogical_drop_provider(PG_FUNCTION_ARGS)
{
	char	   *provider_name = NameStr(*PG_GETARG_NAME(0));
	bool		ifexists = PG_GETARG_BOOL(1);
	PGLogicalProvider  *provider;

	provider = get_provider_by_name(provider_name, !ifexists);

	if (provider != NULL)
	{
		drop_provider(provider->id);
		pglogical_connections_changed();
	}

	PG_RETURN_BOOL(provider != NULL);
}

/*
 * Connect two existing nodes.
 *
 * TODO: handle different sync modes correctly.
 */
Datum
pglogical_create_subscriber(PG_FUNCTION_ARGS)
{
	PGLogicalSubscriber		sub;
//	bool			sync_schema = PG_GET_BOOL(5);
//	bool			sync_data = PG_GET_BOOL(6);

	sub.id = InvalidOid;
	sub.name = NameStr(*PG_GETARG_NAME(0));
	sub.local_dsn = text_to_cstring(PG_GETARG_TEXT_PP(1));
	sub.provider_name = NameStr(*PG_GETARG_NAME(2));
	sub.provider_dsn = text_to_cstring(PG_GETARG_TEXT_PP(3));
	sub.replication_sets = textarray_to_list(PG_GETARG_ARRAYTYPE_P(4));

	sub.status = SUBSCRIBER_STATUS_INIT;

	create_subscriber(&sub);

	pglogical_connections_changed();

	PG_RETURN_OID(sub.id);
}

/*
 * Remove subscriber.
 */
Datum
pglogical_drop_subscriber(PG_FUNCTION_ARGS)
{
	char	   *sub_name = NameStr(*PG_GETARG_NAME(0));
	bool		ifexists = PG_GETARG_BOOL(1);
	PGLogicalSubscriber	   *sub;

	sub = get_subscriber_by_name(sub_name, !ifexists);

	if (sub != NULL)
	{
		drop_subscriber(sub->id);
		pglogical_connections_changed();
	}

	PG_RETURN_BOOL(sub != NULL);
}

/*
 * Wait until local node is ready.
 */
Datum
pglogical_wait_for_subscriber_ready(PG_FUNCTION_ARGS)
{
	char	   *sub_name = NameStr(*PG_GETARG_NAME(0));

	for (;;)
	{
		PGLogicalSubscriber *sub = get_subscriber_by_name(sub_name, false);

		if (sub->status == SUBSCRIBER_STATUS_READY)
			break;

		pfree(sub);

		CHECK_FOR_INTERRUPTS();

		(void) WaitLatch(&MyProc->procLatch,
						 WL_LATCH_SET | WL_TIMEOUT, 1000L);

        ResetLatch(&MyProc->procLatch);
	}

	PG_RETURN_BOOL(true);
}

/*
 * Create new replication set.
 */
Datum
pglogical_create_replication_set(PG_FUNCTION_ARGS)
{
	PGLogicalRepSet	repset;

	repset.id = InvalidOid;

	repset.name = NameStr(*PG_GETARG_NAME(0));

	repset.replicate_insert = PG_GETARG_BOOL(1);
	repset.replicate_update = PG_GETARG_BOOL(2);
	repset.replicate_delete = PG_GETARG_BOOL(3);
	repset.replicate_truncate = PG_GETARG_BOOL(4);

	create_replication_set(&repset);

	PG_RETURN_OID(repset.id);
}

/*
 * Drop existing replication set.
 */
Datum
pglogical_drop_replication_set(PG_FUNCTION_ARGS)
{
	char	   *set_name = NameStr(*PG_GETARG_NAME(0));
	bool		ifexists = PG_GETARG_BOOL(1);
	PGLogicalRepSet    *repset;
	List			   *providers;

	providers = get_providers();
	if (list_length(providers) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("replication sets can be only dropped on provider"),
				 errhint("run the command on a provider")));

	repset = get_replication_set_by_name(set_name, !ifexists);

	if (repset != NULL)
		drop_replication_set(repset->id);

	PG_RETURN_BOOL(repset != NULL);
}

/*
 * Add replication set / relation mapping.
 */
Datum
pglogical_replication_set_add_table(PG_FUNCTION_ARGS)
{
	Oid			reloid = PG_GETARG_OID(1);
	PGLogicalRepSet    *repset;
	Relation			rel;

	/* Find the replication set. */
	repset = get_replication_set_by_name(NameStr(*PG_GETARG_NAME(0)), false);

	/* Make sure the relation exists. */
	rel = heap_open(reloid, AccessShareLock);

	replication_set_add_table(repset->id, reloid);

	/* Cleanup. */
	heap_close(rel, NoLock);

	PG_RETURN_BOOL(true);
}

/*
 * Remove replication set / relation mapping.
 *
 * Unlike the pglogical_replication_set_add_table, this function does not care
 * if table is valid or not, as we are just removing the record from repset.
 */
Datum
pglogical_replication_set_remove_table(PG_FUNCTION_ARGS)
{
	Oid			reloid = PG_GETARG_OID(1);

	PGLogicalRepSet    *repset;

	/* Find the replication set. */
	repset = get_replication_set_by_name(NameStr(*PG_GETARG_NAME(0)), false);

	replication_set_remove_table(repset->id, reloid, false);

	PG_RETURN_BOOL(true);
}

/*
 * pglogical_replicate_ddl_command
 *
 * Queues the input SQL for replication.
 */
Datum
pglogical_replicate_ddl_command(PG_FUNCTION_ARGS)
{
	text   *command = PG_GETARG_TEXT_PP(0);
	char   *query = text_to_cstring(command);
	StringInfoData	cmd;

	/* Force everything in the query to be fully qualified. */
	(void) set_config_option("search_path", "",
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SAVE, true, 0, false);

	/* Convert the query to json string. */
	initStringInfo(&cmd);
	escape_json(&cmd, query);

	/*
	 * Queue the query for replication.
	 *
	 * Note, we keep "DDL" message type for the future when we have deparsing
	 * support.
	 */
	queue_message("all", GetUserId(), QUEUE_COMMAND_TYPE_SQL, cmd.data);

	/* Execute the query locally. */
	pglogical_execute_sql_command(query, GetUserNameFromId(GetUserId(), false),
								  false);

	PG_RETURN_BOOL(true);
}

/*
 * pglogical_queue_trigger
 *
 * Trigger which queues the TRUNCATE command.
 *
 * XXX: There does not seem to be a way to support RESTART IDENTITY at the
 * moment.
 */
Datum
pglogical_queue_truncate(PG_FUNCTION_ARGS)
{
	TriggerData	   *trigdata = (TriggerData *) fcinfo->context;
	const char	   *funcname = "queue_truncate";
	char		   *nspname;
	char		   *relname;
	StringInfoData	json;

	/* Return if this function was called from apply process. */
	if (MyPGLogicalWorker)
		PG_RETURN_VOID();

	/* Make sure this is being called as an AFTER TRUNCTATE trigger. */
	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" was not called by trigger manager",
						funcname)));

	if (!TRIGGER_FIRED_AFTER(trigdata->tg_event) ||
		!TRIGGER_FIRED_BY_TRUNCATE(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" must be fired AFTER TRUNCATE",
						funcname)));

	/* Format the query. */
	nspname = get_namespace_name(RelationGetNamespace(trigdata->tg_relation));
	relname = RelationGetRelationName(trigdata->tg_relation);

	/* It's easier to construct json manually than via Jsonb API... */
	initStringInfo(&json);
	appendStringInfo(&json, "{\"schema_name\": ");
	escape_json(&json, nspname);
	appendStringInfo(&json, ",\"table_name\": ");
	escape_json(&json, relname);
	appendStringInfo(&json, "}");

	/* Queue the truncate for replication. */
	queue_message("all", GetUserId(), QUEUE_COMMAND_TYPE_TRUNCATE, json.data);

	PG_RETURN_VOID();
}

/*
 * pglogical_truncate_trigger_add
 *
 * This function, which is called as an event trigger handler, adds TRUNCATE
 * trigger to newly created tables where appropriate.
 *
 * Since triggers are created tgisinternal and their creation is
 * not replicated or dumped we must create truncate triggers on
 * tables even if they're created by a replicated command or
 * restore of a dump. Recursion is not a problem since we don't
 * queue anything for replication anymore.
 */
Datum
pglogical_truncate_trigger_add(PG_FUNCTION_ARGS)
{
	EventTriggerData   *trigdata = (EventTriggerData *) fcinfo->context;
	const char	   *funcname = "truncate_trigger_add";

	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" was not called by event trigger manager",
						funcname)));

	/* Check if this is CREATE TABLE [AS] and if it is, add the trigger. */
	if (strncmp(trigdata->tag, "CREATE TABLE", strlen("CREATE TABLE")) == 0 &&
		IsA(trigdata->parsetree, CreateStmt))
	{
		CreateStmt *stmt = (CreateStmt *)trigdata->parsetree;
		char *nspname;

		/* Skip temporary and unlogged tables */
		if (stmt->relation->relpersistence != RELPERSISTENCE_PERMANENT)
			PG_RETURN_VOID();

		nspname = get_namespace_name(RangeVarGetCreationNamespace(stmt->relation));

		/*
		 * By this time the relation has been created so it's safe to
		 * call RangeVarGetRelid.
		 */
		create_truncate_trigger(nspname, stmt->relation->relname);

		pfree(nspname);
	}

	PG_RETURN_VOID();
}


/*
 * pglogical_dependency_check_trigger
 *
 * This function, which is called as an event trigger handler, does
 * our custom dependency checking.
 */
Datum
pglogical_dependency_check_trigger(PG_FUNCTION_ARGS)
{
	EventTriggerData   *trigdata = (EventTriggerData *) fcinfo->context;
	const char	   *funcname = "dependency_check_trigger";
	int				res,
					i;
	DropStmt	   *stmt;
	StringInfoData	logdetail;
	int				numDependentObjects = 0;

	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" was not called by event trigger manager",
						funcname)));

	stmt = (DropStmt *)trigdata->parsetree;
	initStringInfo(&logdetail);

	SPI_connect();

	res = SPI_execute("SELECT objid, object_identity "
					  "FROM pg_event_trigger_dropped_objects() "
					  "WHERE object_type = 'table'",
					  false, 0);
	if (res != SPI_OK_SELECT)
		elog(ERROR, "SPI query failed: %d", res);

	for (i = 0; i < SPI_processed; i++)
	{
		Oid		reloid;
		char   *table_name;
		bool	isnull;
		List   *repsets;

		reloid = (Oid) SPI_getbinval(SPI_tuptable->vals[i],
									 SPI_tuptable->tupdesc, 1, &isnull);
		Assert(!isnull);
		table_name = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2);

		repsets = get_relation_replication_sets(reloid);

		if (list_length(repsets))
		{
			ListCell	   *lc;

			foreach (lc, repsets)
			{
				PGLogicalRepSet	   *repset = (PGLogicalRepSet *) lfirst(lc);

				if (numDependentObjects++)
					appendStringInfoString(&logdetail, "\n");
				appendStringInfo(&logdetail, "table %s in replication set %s",
								 table_name, repset->name);

				if (stmt->behavior == DROP_CASCADE)
					replication_set_remove_table(repset->id, reloid, true);
			}
		}
	}

	SPI_finish();

	if (numDependentObjects)
	{
		if (stmt->behavior != DROP_CASCADE)
			ereport(ERROR,
					(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
					 errmsg("cannot drop desired object(s) because other objects depend on them"),
					 errdetail("%s", logdetail.data),
					 errhint("Use DROP ... CASCADE to drop the dependent objects too.")));
		else
			ereport(NOTICE,
					(errmsg_plural("drop cascades to %d other object",
								   "drop cascades to %d other objects",
								   numDependentObjects, numDependentObjects),
					 errdetail("%s", logdetail.data)));
	}

	PG_RETURN_VOID();
}
