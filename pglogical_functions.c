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
#include "access/xlog.h"

#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"

#include "commands/dbcommands.h"
#include "commands/event_trigger.h"
#include "commands/trigger.h"

#include "executor/spi.h"

#include "funcapi.h"

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
#include "pglogical_rpc.h"
#include "pglogical_sync.h"
#include "pglogical_worker.h"

#include "pglogical.h"

/* Node management. */
PG_FUNCTION_INFO_V1(pglogical_create_node);
PG_FUNCTION_INFO_V1(pglogical_drop_node);
PG_FUNCTION_INFO_V1(pglogical_alter_node_disable);
PG_FUNCTION_INFO_V1(pglogical_alter_node_enable);

/* Subscription management. */
PG_FUNCTION_INFO_V1(pglogical_create_subscription);
PG_FUNCTION_INFO_V1(pglogical_drop_subscription);

PG_FUNCTION_INFO_V1(pglogical_alter_subscription_disable);
PG_FUNCTION_INFO_V1(pglogical_alter_subscription_enable);

/* Replication set manipulation. */
PG_FUNCTION_INFO_V1(pglogical_create_replication_set);
PG_FUNCTION_INFO_V1(pglogical_alter_replication_set);
PG_FUNCTION_INFO_V1(pglogical_drop_replication_set);
PG_FUNCTION_INFO_V1(pglogical_replication_set_add_table);
PG_FUNCTION_INFO_V1(pglogical_replication_set_remove_table);

/* DDL */
PG_FUNCTION_INFO_V1(pglogical_replicate_ddl_command);
PG_FUNCTION_INFO_V1(pglogical_queue_truncate);
PG_FUNCTION_INFO_V1(pglogical_truncate_trigger_add);
PG_FUNCTION_INFO_V1(pglogical_dependency_check_trigger);

/* Internal utils */
PG_FUNCTION_INFO_V1(pglogical_gen_slot_name);
PG_FUNCTION_INFO_V1(pglogical_node_info);

/*
 * Create new node
 */
Datum
pglogical_create_node(PG_FUNCTION_ARGS)
{
	PGLogicalNode	node;
	PGlogicalInterface	nodeif;
	PGLogicalRepSet		repset;

	node.id = InvalidOid;
	node.name = NameStr(*PG_GETARG_NAME(0));
	create_node(&node);

	nodeif.id = InvalidOid;
	nodeif.name = node.name;
	nodeif.nodeid = node.id;
	nodeif.dsn = text_to_cstring(PG_GETARG_TEXT_PP(1));
	create_node_interface(&nodeif);

	repset.id = InvalidOid;
	repset.nodeid = node.id;
	repset.name = DEFAULT_REPSET_NAME;
	repset.replicate_insert = true;
	repset.replicate_update = true;
	repset.replicate_delete = true;
	repset.replicate_truncate = true;
	create_replication_set(&repset);

	create_local_node(node.id, nodeif.id);

	PG_RETURN_OID(node.id);
}

/*
 * Drop the named node.
 *
 * TODO: drop subscriptions
 */
Datum
pglogical_drop_node(PG_FUNCTION_ARGS)
{
	char	   *node_name = NameStr(*PG_GETARG_NAME(0));
	bool		ifexists = PG_GETARG_BOOL(1);
	PGLogicalNode  *node;

	node = get_node_by_name(node_name, !ifexists);

	if (node != NULL)
	{
		PGLogicalLocalNode *local_node;

		/* Drop all the interfaces. */
		drop_node_interfaces(node->id);

		/* If the node is local node, drop the record as well. */
		local_node = get_local_node(true);
		if (local_node && local_node->node->id == node->id)
			drop_local_node();

		/* Drop the node itself. */
		drop_node(node->id);

		pglogical_connections_changed();
	}

	PG_RETURN_BOOL(node != NULL);
}

/*
 * Connect two existing nodes.
 */
Datum
pglogical_create_subscription(PG_FUNCTION_ARGS)
{
	PGconn				   *conn;
	PGLogicalSubscription	sub;
	PGLogicalSyncStatus		sync;
	char				   *origin_dsn;
	bool					sync_structure = PG_GETARG_BOOL(3);
	bool					sync_data = PG_GETARG_BOOL(4);
	PGLogicalNode			origin;
	PGlogicalInterface		originif;
	PGLogicalLocalNode     *localnode;
	PGlogicalInterface		targetif;

	/* Check that this is actually a node. */
	localnode = get_local_node(true);

	/* Now, fetch info about remote node. */
	origin_dsn = text_to_cstring(PG_GETARG_TEXT_PP(1));
	conn = pglogical_connect(origin_dsn, "create_subscription");
	pglogical_remote_node_info(conn, &origin.id, &origin.name, NULL, NULL, NULL);
	PQfinish(conn);

	/* Next, create local representation of remote node and interface. */
	create_node(&origin);

	originif.id = InvalidOid;
	originif.name = origin.name;
	originif.nodeid = origin.id;
	originif.dsn = origin_dsn;
	create_node_interface(&originif);

	/*
	 * Next, create subscription.
	 *
	 * Note for now we don't care much about the target interface so we fake
	 * it here to be invalid.
	 */
	targetif.id = localnode->interface->id;
	targetif.nodeid = localnode->node->id;
	sub.id = InvalidOid;
	sub.name = NameStr(*PG_GETARG_NAME(0));
	sub.origin_if = &originif;
	sub.target_if = &targetif;
	sub.replication_sets = textarray_to_list(PG_GETARG_ARRAYTYPE_P(2));
	sub.enabled = true;
	create_subscription(&sub);

	/* Create synchronization status for the subscription. */
	if (sync_structure && sync_data)
		sync.kind = 'f';
	else if (sync_structure)
		sync.kind = 's';
	else if (sync_data)
		sync.kind = 'd';
	else
		sync.kind = 'i';

	sync.subid = sub.id;
	sync.nspname = NULL;
	sync.relname = NULL;
	sync.status = SYNC_STATUS_INIT;
	create_subscription_sync_status(&sync);

	pglogical_connections_changed();

	PG_RETURN_OID(sub.id);
}

/*
 * Remove subscribption.
 */
Datum
pglogical_drop_subscription(PG_FUNCTION_ARGS)
{
	char	   *sub_name = NameStr(*PG_GETARG_NAME(0));
	bool		ifexists = PG_GETARG_BOOL(1);
	PGLogicalSubscription  *sub;

	sub = get_subscription_by_name(sub_name, !ifexists);

	if (sub != NULL)
	{
		/* First drop the status. */
		drop_subscription_sync_status(sub->id);

		/* Drop the actual subscription. */
		drop_subscription(sub->id);

		/* This will make manager kill the apply worker on commit. */
		pglogical_connections_changed();
	}

	PG_RETURN_BOOL(sub != NULL);
}

/*
 * Disable subscription.
 */
Datum
pglogical_alter_subscription_disable(PG_FUNCTION_ARGS)
{
	char				   *sub_name = NameStr(*PG_GETARG_NAME(0));
	bool					immediate = PG_GETARG_BOOL(1);
	PGLogicalSubscription  *sub = get_subscription_by_name(sub_name, false);

	sub->enabled = false;

	alter_subscription(sub);

	if (immediate)
	{
		PGLogicalWorker		   *apply;

		LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);
		apply = pglogical_apply_find(MyDatabaseId, sub->id);
		kill(apply->proc->pid, SIGTERM);
		LWLockRelease(PGLogicalCtx->lock);
	}

	PG_RETURN_BOOL(true);
}

/*
 * Enable subscriber.
 */
Datum
pglogical_alter_subscription_enable(PG_FUNCTION_ARGS)
{
	char				   *sub_name = NameStr(*PG_GETARG_NAME(0));
	bool					immediate = PG_GETARG_BOOL(1);
	PGLogicalSubscription  *sub = get_subscription_by_name(sub_name, false);

	sub->enabled = true;

	alter_subscription(sub);

	if (immediate)
	{
		PGLogicalWorker		   *manager;

		LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);
		manager = pglogical_manager_find(MyDatabaseId);
		SetLatch(&manager->proc->procLatch);
		LWLockRelease(PGLogicalCtx->lock);
	}

	PG_RETURN_BOOL(true);
}

/*
 * Create new replication set.
 */
Datum
pglogical_create_replication_set(PG_FUNCTION_ARGS)
{
	PGLogicalRepSet		repset;
	PGLogicalLocalNode *node;

	node = get_local_node(true);
	if (!node)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("current database is not configured as pglogical node"),
				 errhint("create pglogical node first")));

	repset.id = InvalidOid;

	repset.nodeid = node->node->id;
	repset.name = NameStr(*PG_GETARG_NAME(0));

	repset.replicate_insert = PG_GETARG_BOOL(1);
	repset.replicate_update = PG_GETARG_BOOL(2);
	repset.replicate_delete = PG_GETARG_BOOL(3);
	repset.replicate_truncate = PG_GETARG_BOOL(4);

	create_replication_set(&repset);

	PG_RETURN_OID(repset.id);
}

/*
 * Alter existing replication set.
 */
Datum
pglogical_alter_replication_set(PG_FUNCTION_ARGS)
{
	PGLogicalRepSet	   *repset;
	PGLogicalLocalNode *node;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("set_name cannot be NULL")));

	node = get_local_node(true);
	if (!node)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("current database is not configured as pglogical node"),
				 errhint("create pglogical node first")));

	repset = get_replication_set_by_name(node->node->id,
										 NameStr(*PG_GETARG_NAME(0)), false);

	if (!PG_ARGISNULL(1))
		repset->replicate_insert = PG_GETARG_BOOL(1);
	if (!PG_ARGISNULL(2))
		repset->replicate_update = PG_GETARG_BOOL(2);
	if (!PG_ARGISNULL(3))
		repset->replicate_delete = PG_GETARG_BOOL(3);
	if (!PG_ARGISNULL(4))
		repset->replicate_truncate = PG_GETARG_BOOL(4);

	alter_replication_set(repset);

	PG_RETURN_OID(repset->id);
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
	PGLogicalLocalNode *node;

	node = get_local_node(true);
	if (!node)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("current database is not configured as pglogical node"),
				 errhint("create pglogical node first")));

	repset = get_replication_set_by_name(node->node->id, set_name, !ifexists);

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
	PGLogicalLocalNode *node;

	node = get_local_node(true);
	if (!node)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("current database is not configured as pglogical node"),
				 errhint("create pglogical node first")));

	/* Find the replication set. */
	repset = get_replication_set_by_name(node->node->id,
										 NameStr(*PG_GETARG_NAME(0)), false);

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
	PGLogicalLocalNode *node;

	node = get_local_node(true);
	if (!node)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("current database is not configured as pglogical node"),
				 errhint("create pglogical node first")));

	/* Find the replication set. */
	repset = get_replication_set_by_name(node->node->id,
										 NameStr(*PG_GETARG_NAME(0)), false);

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
	PGLogicalLocalNode *node;

	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" was not called by event trigger manager",
						funcname)));

	/* No local node? */
	node = get_local_node(true);
	if (!node)
		PG_RETURN_VOID();

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

		repsets = get_relation_replication_sets(node->node->id, reloid);

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

Datum
pglogical_node_info(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[5];
	bool		nulls[5];
	HeapTuple	htup;
	char		sysid[32];
	List	   *repsets;
	PGLogicalLocalNode *node;

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	tupdesc = BlessTupleDesc(tupdesc);

	node = get_local_node(false);

	memset(nulls, 0, sizeof(nulls));

	snprintf(sysid, sizeof(sysid), UINT64_FORMAT,
			 GetSystemIdentifier());
	repsets = get_node_replication_sets(node->node->id);

	values[0] = ObjectIdGetDatum(node->node->id);
	values[1] = CStringGetTextDatum(node->node->name);
	values[2] = CStringGetTextDatum(sysid);
	values[3] = CStringGetTextDatum(get_database_name(MyDatabaseId));
	values[4] = CStringGetTextDatum(stringlist_to_identifierstr(repsets));

	htup = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(htup));

}

Datum
pglogical_gen_slot_name(PG_FUNCTION_ARGS)
{
	char	   *subscriber_name = NameStr(*PG_GETARG_NAME(0));
	Name		slot_name;
	PGLogicalLocalNode *node;

	node = get_local_node(false);

	slot_name = (Name) palloc0(NAMEDATALEN);

	gen_slot_name(slot_name, get_database_name(MyDatabaseId), node->node->name, subscriber_name, NULL);

	PG_RETURN_NAME(slot_name);
}
