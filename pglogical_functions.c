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

#include "catalog/catalog.h"
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

#include "replication/origin.h"
#include "replication/reorderbuffer.h"
#include "replication/slot.h"

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
PG_FUNCTION_INFO_V1(pglogical_alter_node_add_interface);
PG_FUNCTION_INFO_V1(pglogical_alter_node_drop_interface);

/* Subscription management. */
PG_FUNCTION_INFO_V1(pglogical_create_subscription);
PG_FUNCTION_INFO_V1(pglogical_drop_subscription);

PG_FUNCTION_INFO_V1(pglogical_alter_subscription_interface);

PG_FUNCTION_INFO_V1(pglogical_alter_subscription_disable);
PG_FUNCTION_INFO_V1(pglogical_alter_subscription_enable);

PG_FUNCTION_INFO_V1(pglogical_alter_subscription_add_replication_set);
PG_FUNCTION_INFO_V1(pglogical_alter_subscription_remove_replication_set);

PG_FUNCTION_INFO_V1(pglogical_alter_subscription_synchronize);
PG_FUNCTION_INFO_V1(pglogical_alter_subscription_resynchronize_table);

PG_FUNCTION_INFO_V1(pglogical_show_subscription_table);
PG_FUNCTION_INFO_V1(pglogical_show_subscription_status);

/* Replication set manipulation. */
PG_FUNCTION_INFO_V1(pglogical_create_replication_set);
PG_FUNCTION_INFO_V1(pglogical_alter_replication_set);
PG_FUNCTION_INFO_V1(pglogical_drop_replication_set);
PG_FUNCTION_INFO_V1(pglogical_replication_set_add_table);
PG_FUNCTION_INFO_V1(pglogical_replication_set_add_all_tables);
PG_FUNCTION_INFO_V1(pglogical_replication_set_remove_table);
PG_FUNCTION_INFO_V1(pglogical_replication_set_add_sequence);
PG_FUNCTION_INFO_V1(pglogical_replication_set_add_all_sequences);
PG_FUNCTION_INFO_V1(pglogical_replication_set_remove_sequence);

/* DDL */
PG_FUNCTION_INFO_V1(pglogical_replicate_ddl_command);
PG_FUNCTION_INFO_V1(pglogical_queue_truncate);
PG_FUNCTION_INFO_V1(pglogical_truncate_trigger_add);
PG_FUNCTION_INFO_V1(pglogical_dependency_check_trigger);

/* Internal utils */
PG_FUNCTION_INFO_V1(pglogical_gen_slot_name);
PG_FUNCTION_INFO_V1(pglogical_node_info);

/* Information */
PG_FUNCTION_INFO_V1(pglogical_version);
PG_FUNCTION_INFO_V1(pglogical_version_num);
PG_FUNCTION_INFO_V1(pglogical_min_proto_version);
PG_FUNCTION_INFO_V1(pglogical_max_proto_version);

static void gen_slot_name(Name slot_name, char *dbname,
						  const char *provider_name,
						  const char *subscriber_name);

/*
 * Create new node
 */
Datum
pglogical_create_node(PG_FUNCTION_ARGS)
{
	char			   *node_name = NameStr(*PG_GETARG_NAME(0));
	char			   *node_dsn = text_to_cstring(PG_GETARG_TEXT_PP(1));
	PGLogicalNode		node;
	PGlogicalInterface	nodeif;
	PGLogicalRepSet		repset;

	node.id = InvalidOid;
	node.name = node_name;
	create_node(&node);

	nodeif.id = InvalidOid;
	nodeif.name = node.name;
	nodeif.nodeid = node.id;
	nodeif.dsn = node_dsn;
	create_node_interface(&nodeif);

	/* Create predefined repsets. */
	repset.id = InvalidOid;
	repset.nodeid = node.id;
	repset.name = DEFAULT_REPSET_NAME;
	repset.replicate_insert = true;
	repset.replicate_update = true;
	repset.replicate_delete = true;
	repset.replicate_truncate = true;
	create_replication_set(&repset);

	repset.id = InvalidOid;
	repset.nodeid = node.id;
	repset.name = DEFAULT_INSONLY_REPSET_NAME;
	repset.replicate_insert = true;
	repset.replicate_update = false;
	repset.replicate_delete = false;
	repset.replicate_truncate = true;
	create_replication_set(&repset);

	repset.id = InvalidOid;
	repset.nodeid = node.id;
	repset.name = DDL_SQL_REPSET_NAME;
	repset.replicate_insert = true;
	repset.replicate_update = false;
	repset.replicate_delete = false;
	repset.replicate_truncate = false;
	create_replication_set(&repset);

	create_local_node(node.id, nodeif.id);

	PG_RETURN_OID(node.id);
}

/*
 * Drop the named node.
 *
 * TODO: support cascade (drop subscribers)
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
		List			   *osubs;
		List			   *tsubs;

		osubs = get_node_subscriptions(node->id, true);
		tsubs = get_node_subscriptions(node->id, false);
		if (list_length(osubs) != 0 || list_length(tsubs) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot drop node \"%s\" because it still has subscriptions associated with it", node_name),
					 errhint("drop the subscriptions first")));

		/* If the node is local node, drop the record as well. */
		local_node = get_local_node(true, true);
		if (local_node && local_node->node->id == node->id)
		{
			int		slotno;

			/* Also drop all the slots associated with the node. */
			for (slotno = 0; slotno < max_replication_slots; slotno++)
			{
				ReplicationSlot *slot = &ReplicationSlotCtl->replication_slots[slotno];

				LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
				SpinLockAcquire(&slot->mutex);
				/*
				 * Check the slot is in use and it's slot belonging to current
				 * pglogical node.
				 */
				if (!slot->in_use ||
					slot->data.database != MyDatabaseId ||
					namestrcmp(&slot->data.plugin, "pglogical_output") != 0 ||
					strncmp(NameStr(slot->data.name), "pgl_", 4) != 0)
				{
					SpinLockRelease(&slot->mutex);
					LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
					continue;
				}

#if PG_VERSION_NUM < 90500
				if (slot->active)
#else
				if (slot->active_pid != 0)
#endif
				{
					SpinLockRelease(&slot->mutex);
					LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
					ereport(ERROR,
							(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							 errmsg("cannot drop node \"%s\" because replication slot \"%s\" on the node is still active",
									node_name, NameStr(slot->data.name)),
							 errhint("drop the subscriptions first")));
				}
				SpinLockRelease(&slot->mutex);
				LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

				ReplicationSlotDrop(NameStr(slot->data.name));
			}

			drop_local_node();
		}

		/* Drop all the interfaces. */
		drop_node_interfaces(node->id);

		/* Drop replication sets associated with the node. */
		drop_node_replication_sets(node->id);

		/* Drop the node itself. */
		drop_node(node->id);
	}

	PG_RETURN_BOOL(node != NULL);
}

/*
 * Add interface to a node.
 */
Datum
pglogical_alter_node_add_interface(PG_FUNCTION_ARGS)
{
	char	   *node_name = NameStr(*PG_GETARG_NAME(0));
	char	   *if_name = NameStr(*PG_GETARG_NAME(1));
	char	   *if_dsn = text_to_cstring(PG_GETARG_TEXT_PP(2));
	PGLogicalNode	   *node;
	PGlogicalInterface *oldif,
						newif;

	node = get_node_by_name(node_name, false);
	if (node == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("node \"%s\" not found", node_name)));

	oldif = get_node_interface_by_name(node->id, if_name, true);
	if (oldif != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("node \"%s\" already has interface named \"%s\"",
				 node_name, if_name)));

	newif.id = InvalidOid;
	newif.name = if_name;
	newif.nodeid = node->id;
	newif.dsn = if_dsn;
	create_node_interface(&newif);

	PG_RETURN_OID(newif.id);
}

/*
 * Drop interface from a node.
 */
Datum
pglogical_alter_node_drop_interface(PG_FUNCTION_ARGS)
{
	char	   *node_name = NameStr(*PG_GETARG_NAME(0));
	char	   *if_name = NameStr(*PG_GETARG_NAME(1));
	PGLogicalNode	   *node;
	PGlogicalInterface *oldif;

	node = get_node_by_name(node_name, false);
	if (node == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("node \"%s\" not found", node_name)));

	oldif = get_node_interface_by_name(node->id, if_name, true);
	if (oldif == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("interface \"%s\" for node node \"%s\" not found",
				 if_name, node_name)));

	drop_node_interface(oldif->id);

	PG_RETURN_BOOL(true);
}


/*
 * Connect two existing nodes.
 */
Datum
pglogical_create_subscription(PG_FUNCTION_ARGS)
{
	char				   *sub_name = NameStr(*PG_GETARG_NAME(0));
	char				   *provider_dsn = text_to_cstring(PG_GETARG_TEXT_PP(1));
	ArrayType			   *rep_set_names = PG_GETARG_ARRAYTYPE_P(2);
	bool					sync_structure = PG_GETARG_BOOL(3);
	bool					sync_data = PG_GETARG_BOOL(4);
	ArrayType			   *forward_origin_names = PG_GETARG_ARRAYTYPE_P(5);
	PGconn				   *conn;
	PGLogicalSubscription	sub;
	PGLogicalSyncStatus		sync;
	PGLogicalNode			origin;
	PGLogicalNode		   *existing_origin;
	PGlogicalInterface		originif;
	PGLogicalLocalNode     *localnode;
	PGlogicalInterface		targetif;
	List				   *replication_sets;
	List				   *other_subs;
	ListCell			   *lc;
	NameData				slot_name;

	/* Check that this is actually a node. */
	localnode = get_local_node(true, false);

	/* Now, fetch info about remote node. */
	conn = pglogical_connect(provider_dsn, "create_subscription");
	pglogical_remote_node_info(conn, &origin.id, &origin.name, NULL, NULL, NULL);
	PQfinish(conn);

	/* Check that we can connect remotely also in replication mode. */
	conn = pglogical_connect_replica(provider_dsn, "create_subscription");
	PQfinish(conn);

	/* Check that local connection works. */
	conn = pglogical_connect(localnode->node_if->dsn, "create_subscription");
	PQfinish(conn);

	/*
	 * Check for existing local representation of remote node and interface
	 * and lock it if it already exists.
	 */
	existing_origin = get_node_by_name(origin.name, true);

	/*
	 * If not found, crate local representation of remote node and interface.
	 */
	if (!existing_origin)
	{
		create_node(&origin);

		originif.id = InvalidOid;
		originif.name = origin.name;
		originif.nodeid = origin.id;
		originif.dsn = provider_dsn;
		create_node_interface(&originif);
	}
	else
	{
		PGlogicalInterface *existingif;

		existingif = get_node_interface_by_name(origin.id, origin.name, false);
		if (strcmp(existingif->dsn, provider_dsn) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("dsn \"%s\" points to existing node \"%s\" with different dsn \"%s\"",
					 provider_dsn, origin.name, existingif->dsn)));

		memcpy(&originif, existingif, sizeof(PGlogicalInterface));
	}

	/*
	 * Check for overlapping replication sets.
	 *
	 * Note that we can't use exclusion constraints as we use the
	 * subscriptions table in same manner as system catalog.
	 */
	replication_sets = textarray_to_list(rep_set_names);
	other_subs = get_node_subscriptions(originif.nodeid, true);
	foreach (lc, other_subs)
	{
		PGLogicalSubscription  *esub = (PGLogicalSubscription *) lfirst(lc);
		ListCell			   *esetcell;

		foreach (esetcell, esub->replication_sets)
		{
			char	   *existingset = lfirst(esetcell);
			ListCell   *nsetcell;

			foreach (nsetcell, replication_sets)
			{
				char	   *newset = lfirst(nsetcell);

				if (strcmp(newset, existingset) == 0)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("existing subscription \"%s\" to node "
									"\"%s\" already subscribes to replication "
									"set \"%s\"", esub->name, origin.name,
									newset)));
			}
		}
	}

	/*
	 * Create the subscription.
	 *
	 * Note for now we don't care much about the target interface so we fake
	 * it here to be invalid.
	 */
	targetif.id = localnode->node_if->id;
	targetif.nodeid = localnode->node->id;
	sub.id = InvalidOid;
	sub.name = sub_name;
	sub.origin_if = &originif;
	sub.target_if = &targetif;
	sub.replication_sets = replication_sets;
	sub.forward_origins = textarray_to_list(forward_origin_names);
	sub.enabled = true;
	gen_slot_name(&slot_name, get_database_name(MyDatabaseId),
				  origin.name, sub_name);
	sub.slot_name = pstrdup(NameStr(slot_name));

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
	create_local_sync_status(&sync);

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
		PGLogicalWorker	   *apply;
		List			   *other_subs;
		PGLogicalLocalNode *node;
		RepOriginId			originid;

		node = get_local_node(true, false);

		/* First drop the status. */
		drop_subscription_sync_status(sub->id);

		/* Drop the actual subscription. */
		drop_subscription(sub->id);

		/*
		 * The rest is different depending on if we are doing this on provider
		 * or subscriber.
		 *
		 * For now on provider we just exist (there should be no records
		 * of subscribers on their provider node).
		 */
		if (sub->origin->id == node->node->id)
			PG_RETURN_BOOL(sub != NULL);

		/*
		 * If the provider node record existed only for the dropped,
		 * subscription, it should be dropped as well.
		 */
		other_subs = get_node_subscriptions(sub->origin->id, true);
		if (list_length(other_subs) == 0)
		{
			drop_node_interfaces(sub->origin->id);
			drop_node(sub->origin->id);
		}

		/* Kill the apply to unlock the resources. */
		LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);
		apply = pglogical_apply_find(MyDatabaseId, sub->id);
		pglogical_worker_kill(apply);
		LWLockRelease(PGLogicalCtx->lock);

		/* Wait for the apply to die. */
		for (;;)
		{
			LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);
			apply = pglogical_apply_find(MyDatabaseId, sub->id);
			if (!pglogical_worker_running(apply))
			{
				LWLockRelease(PGLogicalCtx->lock);
				break;
			}
			LWLockRelease(PGLogicalCtx->lock);

			CHECK_FOR_INTERRUPTS();

			(void) WaitLatch(&MyProc->procLatch,
							 WL_LATCH_SET | WL_TIMEOUT, 1000L);

			ResetLatch(&MyProc->procLatch);
		}

		/*
		 * Drop the slot on remote side.
		 *
		 * Note, we can't fail here since we can't assume that the remote node
		 * is still reachable or even alive.
		 */
		PG_TRY();
		{
			PGconn *origin_conn = pglogical_connect(sub->origin_if->dsn, "cleanup");
			pglogical_drop_remote_slot(origin_conn, sub->slot_name);
			PQfinish(origin_conn);
		}
		PG_CATCH();
			elog(WARNING, "could not drop slot \"%s\" on provider, you will probably have to drop it manually",
				 sub->slot_name);
		PG_END_TRY();

		/* Drop the origin tracking locally. */
		originid = replorigin_by_name(sub->slot_name, true);
		if (originid != InvalidRepOriginId)
			replorigin_drop(originid);
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
	PGLogicalLocalNode *node;

	/* XXX: Only used for locking purposes. */
	node = get_local_node(true, false);

	sub->enabled = false;

	alter_subscription(sub);

	if (immediate)
	{
		PGLogicalWorker		   *apply;

		if ((IsTransactionBlock() || IsSubTransaction()))
			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("alter_subscription_disable with immediate = true "
							"cannot be run inside a transaction block")));

		LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);
		apply = pglogical_apply_find(MyDatabaseId, sub->id);
		pglogical_worker_kill(apply);
		LWLockRelease(PGLogicalCtx->lock);
	}

	PG_RETURN_BOOL(true);
}

/*
 * Enable subscription.
 */
Datum
pglogical_alter_subscription_enable(PG_FUNCTION_ARGS)
{
	char				   *sub_name = NameStr(*PG_GETARG_NAME(0));
	bool					immediate = PG_GETARG_BOOL(1);
	PGLogicalSubscription  *sub = get_subscription_by_name(sub_name, false);
	PGLogicalLocalNode *node;

	/* XXX: Only used for locking purposes. */
	node = get_local_node(true, false);

	sub->enabled = true;

	alter_subscription(sub);

	/*
	 * There is nothing more to immediate here than running it outside of
	 * transaction.
	 */
	if (immediate && (IsTransactionBlock() || IsSubTransaction()))
	{
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("alter_subscription_enable with immediate = true "
						"cannot be run inside a transaction block")));
	}

	PG_RETURN_BOOL(true);
}

/*
 * Switch interface the subscription is using.
 */
Datum
pglogical_alter_subscription_interface(PG_FUNCTION_ARGS)
{
	char				   *sub_name = NameStr(*PG_GETARG_NAME(0));
	char				   *if_name = NameStr(*PG_GETARG_NAME(1));
	PGLogicalSubscription  *sub = get_subscription_by_name(sub_name, false);
	PGlogicalInterface	   *new_if;
	PGLogicalLocalNode *node;

	/* XXX: Only used for locking purposes. */
	node = get_local_node(true, false);

	new_if = get_node_interface_by_name(sub->origin->id, if_name, false);

	if (new_if->id == sub->origin_if->id)
		PG_RETURN_BOOL(false);

	sub->origin_if = new_if;
	alter_subscription(sub);

	PG_RETURN_BOOL(true);
}

/*
 * Add replication set to subscription.
 */
Datum
pglogical_alter_subscription_add_replication_set(PG_FUNCTION_ARGS)
{
	char				   *sub_name = NameStr(*PG_GETARG_NAME(0));
	char				   *repset_name = NameStr(*PG_GETARG_NAME(1));
	PGLogicalSubscription  *sub = get_subscription_by_name(sub_name, false);
	ListCell			   *lc;

	foreach (lc, sub->replication_sets)
	{
		char	   *rs = (char *) lfirst(lc);

		if (strcmp(rs, repset_name) == 0)
			PG_RETURN_BOOL(false);
	}

	sub->replication_sets = lappend(sub->replication_sets, repset_name);
	alter_subscription(sub);

	PG_RETURN_BOOL(true);
}

/*
 * Remove replication set to subscription.
 */
Datum
pglogical_alter_subscription_remove_replication_set(PG_FUNCTION_ARGS)
{
	char				   *sub_name = NameStr(*PG_GETARG_NAME(0));
	char				   *repset_name = NameStr(*PG_GETARG_NAME(1));
	PGLogicalSubscription  *sub = get_subscription_by_name(sub_name, false);
	ListCell			   *lc,
						   *next,
						   *prev;

	prev = NULL;
	for (lc = list_head(sub->replication_sets); lc; lc = next)
	{
		char	   *rs = (char *) lfirst(lc);

		/* We might delete the cell so advance it now. */
		next = lnext(lc);

		if (strcmp(rs, repset_name) == 0)
		{
			sub->replication_sets = list_delete_cell(sub->replication_sets,
													 lc, prev);
			alter_subscription(sub);

			PG_RETURN_BOOL(true);
		}

		prev = lc;
	}

	PG_RETURN_BOOL(false);
}

/*
 * Synchronize all the missing tables.
 */
Datum
pglogical_alter_subscription_synchronize(PG_FUNCTION_ARGS)
{
	char				   *sub_name = NameStr(*PG_GETARG_NAME(0));
	bool					truncate = PG_GETARG_BOOL(1);
	PGLogicalSubscription  *sub = get_subscription_by_name(sub_name, false);
	PGconn				   *conn;
	List				   *tables;
	ListCell			   *lc;
	PGLogicalWorker		   *apply;

	/* Read table list from provider. */
	conn = pglogical_connect(sub->origin_if->dsn, "synchronize_subscription");
	tables = pg_logical_get_remote_repset_tables(conn, sub->replication_sets);
	PQfinish(conn);

	/* Compare with sync status on subscription. And add missing ones. */
	foreach (lc, tables)
	{
		RangeVar	   *rv = (RangeVar *) lfirst(lc);
		PGLogicalSyncStatus	   *oldsync;

		oldsync = get_table_sync_status(sub->id, rv->schemaname, rv->relname,
										true);
		if (!oldsync)
		{
			PGLogicalSyncStatus	   newsync;

			newsync.kind = SYNC_KIND_DATA;
			newsync.subid = sub->id;
			newsync.nspname = rv->schemaname;
			newsync.relname = rv->relname;
			newsync.status = SYNC_STATUS_INIT;
			create_local_sync_status(&newsync);

			if (truncate)
				truncate_table(rv->schemaname, rv->relname);
		}
	}

	/* Tell apply to re-read sync statuses. */
	LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);
	apply = pglogical_apply_find(MyDatabaseId, sub->id);
	if (pglogical_worker_running(apply))
		apply->worker.apply.sync_pending = true;
	else
		pglogical_subscription_changed(sub->id);
	LWLockRelease(PGLogicalCtx->lock);

	PG_RETURN_BOOL(true);
}

/*
 * Resyncrhonize one existing table.
 */
Datum
pglogical_alter_subscription_resynchronize_table(PG_FUNCTION_ARGS)
{
	char				   *sub_name = NameStr(*PG_GETARG_NAME(0));
	Oid						reloid = PG_GETARG_OID(1);
	PGLogicalSubscription  *sub = get_subscription_by_name(sub_name, false);
	PGLogicalSyncStatus	   *oldsync;
	PGLogicalWorker		   *apply;
	Relation				rel;
	char				   *nspname,
						   *relname;

	rel = heap_open(reloid, AccessShareLock);

	nspname = get_namespace_name(RelationGetNamespace(rel));
	relname = RelationGetRelationName(rel);

	/* Reset sync status of the table. */
	oldsync = get_table_sync_status(sub->id, nspname, relname, true);
	if (oldsync)
	{
		if (oldsync->status != SYNC_STATUS_READY &&
			oldsync->status != SYNC_STATUS_NONE)
			elog(ERROR, "table %s.%s is already being synchronized",
				 nspname, relname);

		set_table_sync_status(sub->id, nspname, relname, SYNC_STATUS_INIT);
	}
	else
	{
		PGLogicalSyncStatus	   newsync;

		newsync.kind = SYNC_KIND_DATA;
		newsync.subid = sub->id;
		newsync.nspname = nspname;
		newsync.relname = relname;
		newsync.status = SYNC_STATUS_INIT;
		create_local_sync_status(&newsync);
	}

	heap_close(rel, NoLock);

	truncate_table(nspname, relname);

	/* Tell apply to re-read sync statuses. */
	LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);
	apply = pglogical_apply_find(MyDatabaseId, sub->id);
	if (pglogical_worker_running(apply))
		apply->worker.apply.sync_pending = true;
	else
		pglogical_subscription_changed(sub->id);
	LWLockRelease(PGLogicalCtx->lock);

	PG_RETURN_BOOL(true);
}

static char *
sync_status_to_string(char status)
{
	switch (status)
	{
		case SYNC_STATUS_INIT:
			return "sync_init";
		case SYNC_STATUS_STRUCTURE:
			return "sync_structure";
		case SYNC_STATUS_DATA:
			return "sync_data";
		case SYNC_STATUS_CONSTAINTS:
			return "sync_constraints";
		case SYNC_STATUS_SYNCWAIT:
			return "sync_waiting";
		case SYNC_STATUS_CATCHUP:
			return "catchup";
		case SYNC_STATUS_READY:
			return "syncronized";
		default:
			return "unknown";
	}
}

/*
 * Show info about one table.
 */
Datum
pglogical_show_subscription_table(PG_FUNCTION_ARGS)
{
	char				   *sub_name = NameStr(*PG_GETARG_NAME(0));
	Oid						reloid = PG_GETARG_OID(1);
	PGLogicalSubscription  *sub = get_subscription_by_name(sub_name, false);
	char				   *nspname;
	char				   *relname;
	PGLogicalSyncStatus	   *sync;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	Datum		values[3];
	bool		nulls[3];

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	nspname = get_namespace_name(get_rel_namespace(reloid));
	relname = get_rel_name(reloid);

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	values[0] = CStringGetTextDatum(nspname);
	values[1] = CStringGetTextDatum(relname);

	/* Reset sync status of the table. */
	sync = get_table_sync_status(sub->id, nspname, relname, true);
	if (sync)
		values[2] = CStringGetTextDatum(sync_status_to_string(sync->status));
	else
		values[2] = CStringGetTextDatum("unknown");

	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	tuplestore_donestoring(tupstore);

	PG_RETURN_VOID();
}

/*
 * Show info about subscribtion.
 */
Datum
pglogical_show_subscription_status(PG_FUNCTION_ARGS)
{
	List			   *subscriptions;
	ListCell		   *lc;
	ReturnSetInfo	   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc			tupdesc;
	Tuplestorestate	   *tupstore;
	PGLogicalLocalNode *node;
	MemoryContext		per_query_ctx;
	MemoryContext		oldcontext;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	node = get_local_node(false, true);
	if (!node)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("current database is not configured as pglogical node"),
				 errhint("create pglogical node first")));

	if (PG_ARGISNULL(0))
	{
		subscriptions = get_node_subscriptions(node->node->id, false);
	}
	else
	{
		PGLogicalSubscription  *sub;
		sub = get_subscription_by_name(NameStr(*PG_GETARG_NAME(0)), false);
		subscriptions = list_make1(sub);
	}

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	foreach (lc, subscriptions)
	{
		PGLogicalSubscription  *sub = lfirst(lc);
		PGLogicalWorker		   *apply;
		Datum	values[7];
		bool	nulls[7];
		char   *status;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);
		apply = pglogical_apply_find(MyDatabaseId, sub->id);
		if (pglogical_worker_running(apply))
		{
			PGLogicalSyncStatus	   *sync;
			sync = get_subscription_sync_status(sub->id, true);

			if (!sync)
				status = "unknown";
			else if (sync->status == SYNC_STATUS_READY)
				status = "replicating";
			else
				status = "initializing";
		}
		else if (!sub->enabled)
			status = "disabled";
		else
			status = "down";
		LWLockRelease(PGLogicalCtx->lock);

		values[0] = CStringGetTextDatum(sub->name);
		values[1] = CStringGetTextDatum(status);
		values[2] = CStringGetTextDatum(sub->origin->name);
		values[3] = CStringGetTextDatum(sub->origin_if->dsn);
		values[4] = CStringGetTextDatum(sub->slot_name);
		if (sub->replication_sets)
			values[5] =
				PointerGetDatum(strlist_to_textarray(sub->replication_sets));
		else
			nulls[5] = true;
		if (sub->forward_origins)
			values[6] =
				PointerGetDatum(strlist_to_textarray(sub->forward_origins));
		else
			nulls[6] = true;

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	tuplestore_donestoring(tupstore);

	PG_RETURN_VOID();
}

/*
 * Create new replication set.
 */
Datum
pglogical_create_replication_set(PG_FUNCTION_ARGS)
{
	PGLogicalRepSet		repset;
	PGLogicalLocalNode *node;

	node = get_local_node(true, true);
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

	node = get_local_node(true, true);
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

	node = get_local_node(true, true);
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
 * Common function for adding replication set / relation mapping.
 */
static Datum
pglogical_replication_set_add_relation(Name repset_name, Oid reloid,
									   bool synchronize, char relkind)
{
	PGLogicalRepSet    *repset;
	Relation			rel;
	PGLogicalLocalNode *node;
	char			   *nspname;
	char			   *relname;
	StringInfoData		json;
	char				cmdtype;

	node = get_local_node(true, true);
	if (!node)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("current database is not configured as pglogical node"),
				 errhint("create pglogical node first")));

	/* Find the replication set. */
	repset = get_replication_set_by_name(node->node->id,
										 NameStr(*repset_name), false);

	/* Make sure the relation exists. */
	rel = heap_open(reloid, AccessShareLock);

	replication_set_add_relation(repset->id, reloid);

	if (synchronize)
	{
		nspname = get_namespace_name(RelationGetNamespace(rel));
		relname = RelationGetRelationName(rel);

		/* It's easier to construct json manually than via Jsonb API... */
		initStringInfo(&json);
		appendStringInfo(&json, "{\"schema_name\": ");
		escape_json(&json, nspname);
		switch (relkind)
		{
			case RELKIND_RELATION:
				appendStringInfo(&json, ",\"table_name\": ");
				escape_json(&json, relname);
				cmdtype = QUEUE_COMMAND_TYPE_TABLESYNC;
				break;
			case RELKIND_SEQUENCE:
				appendStringInfo(&json, ",\"sequence_name\": ");
				escape_json(&json, relname);
                appendStringInfo(&json, ",\"last_value\": \""INT64_FORMAT"\"",
								 sequence_get_last_value(reloid));
				cmdtype = QUEUE_COMMAND_TYPE_SEQUENCE;
				break;
			default:
				elog(ERROR, "unsupported relkind '%c'", relkind);
		}
		appendStringInfo(&json, "}");

		/* Queue the truncate for replication. */
		queue_message(list_make1(repset->name), GetUserId(), cmdtype,
					  json.data);
	}

	/* Cleanup. */
	heap_close(rel, NoLock);

	PG_RETURN_BOOL(true);
}


/*
 * Add replication set / table mapping.
 */
Datum
pglogical_replication_set_add_table(PG_FUNCTION_ARGS)
{
	Name		repset_name = PG_GETARG_NAME(0);
	Oid			reloid = PG_GETARG_OID(1);
	bool		synchronize = PG_GETARG_BOOL(2);

	return pglogical_replication_set_add_relation(repset_name, reloid,
												  synchronize,
												  RELKIND_RELATION);
}

/*
 * Add replication set / sequence mapping.
 */
Datum
pglogical_replication_set_add_sequence(PG_FUNCTION_ARGS)
{
	Name		repset_name = PG_GETARG_NAME(0);
	Oid			reloid = PG_GETARG_OID(1);
	bool		synchronize = PG_GETARG_BOOL(2);

	return pglogical_replication_set_add_relation(repset_name, reloid,
												  synchronize,
												  RELKIND_SEQUENCE);
}

/*
 * Common function for adding replication set / relation mapping based on
 * schemas.
 */
static Datum
pglogical_replication_set_add_all_relations(Name repset_name,
											ArrayType *nsp_names,
											bool synchronize, char relkind)
{
	PGLogicalRepSet    *repset;
	Relation			rel;
	PGLogicalLocalNode *node;
	StringInfoData		json;
	ListCell		   *lc;

	node = get_local_node(true, true);
	if (!node)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("current database is not configured as pglogical node"),
				 errhint("create pglogical node first")));

	/* Find the replication set. */
	repset = get_replication_set_by_name(node->node->id,
										 NameStr(*repset_name), false);

	rel = heap_open(RelationRelationId, RowExclusiveLock);

	foreach (lc, textarray_to_list(nsp_names))
	{
		char	   *nspname = lfirst(lc);
		Oid			nspoid = LookupExplicitNamespace(nspname, false);
		ScanKeyData skey[1];
		SysScanDesc sysscan;
		HeapTuple	tuple;

		ScanKeyInit(&skey[0],
					Anum_pg_class_relnamespace,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(nspoid));

		sysscan = systable_beginscan(rel, ClassNameNspIndexId, true,
									 NULL, 1, skey);

		while (HeapTupleIsValid(tuple = systable_getnext(sysscan)))
		{
			Oid				reloid = HeapTupleGetOid(tuple);
			Form_pg_class	reltup = (Form_pg_class) GETSTRUCT(tuple);

			/*
			 * Only add logged relations which are not system relations
			 * (catalog, toast).
			 */
			if (reltup->relkind != relkind ||
				reltup->relpersistence != RELPERSISTENCE_PERMANENT ||
				IsSystemClass(reloid, reltup))
				continue;

			if (!replication_set_has_relation(repset->id, reloid))
				replication_set_add_relation(repset->id, reloid);

			// MODOS TODO
			if (synchronize && relkind == RELKIND_RELATION)
			{
				/* It's easier to construct json manually than via Jsonb API... */
				initStringInfo(&json);
				appendStringInfo(&json, "{\"schema_name\": ");
				escape_json(&json, nspname);
				appendStringInfo(&json, ",\"table_name\": ");
				escape_json(&json, NameStr(reltup->relname));
				appendStringInfo(&json, "}");

				/* Queue the truncate for replication. */
				queue_message(list_make1(repset->name), GetUserId(),
							  QUEUE_COMMAND_TYPE_TABLESYNC, json.data);
			}
		}

		systable_endscan(sysscan);
	}

	heap_close(rel, RowExclusiveLock);

	PG_RETURN_BOOL(true);
}

/*
 * Add replication set / table mapping based on schemas.
 */
Datum
pglogical_replication_set_add_all_tables(PG_FUNCTION_ARGS)
{
	Name		repset_name = PG_GETARG_NAME(0);
	ArrayType  *nsp_names = PG_GETARG_ARRAYTYPE_P(1);
	bool		synchronize = PG_GETARG_BOOL(2);

	return pglogical_replication_set_add_all_relations(repset_name, nsp_names,
													   synchronize,
													   RELKIND_RELATION);
}

/*
 * Add replication set / table mapping based on schemas.
 */
Datum
pglogical_replication_set_add_all_sequences(PG_FUNCTION_ARGS)
{
	Name		repset_name = PG_GETARG_NAME(0);
	ArrayType  *nsp_names = PG_GETARG_ARRAYTYPE_P(1);
	bool		synchronize = PG_GETARG_BOOL(2);

	return pglogical_replication_set_add_all_relations(repset_name, nsp_names,
													   synchronize,
													   RELKIND_SEQUENCE);
}

/*
 * Remove replication set / table mapping.
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

	node = get_local_node(true, true);
	if (!node)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("current database is not configured as pglogical node"),
				 errhint("create pglogical node first")));

	/* Find the replication set. */
	repset = get_replication_set_by_name(node->node->id,
										 NameStr(*PG_GETARG_NAME(0)), false);

	replication_set_remove_relation(repset->id, reloid, false);

	PG_RETURN_BOOL(true);
}

/*
 * Remove replication set / sequence mapping.
 */
Datum
pglogical_replication_set_remove_sequence(PG_FUNCTION_ARGS)
{
	/* Sequences can be handled same way as tables. */
	return pglogical_replication_set_remove_table(fcinfo);
}

/*
 * pglogical_replicate_ddl_command
 *
 * Queues the input SQL for replication.
 */
Datum
pglogical_replicate_ddl_command(PG_FUNCTION_ARGS)
{
	text	   *command = PG_GETARG_TEXT_PP(0);
	char	   *query = text_to_cstring(command);
	int			save_nestlevel;
	List	   *replication_sets;
	ListCell   *lc;
	PGLogicalLocalNode *node;
	StringInfoData		cmd;

	node = get_local_node(false, true);
	if (!node)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("current database is not configured as pglogical node"),
				 errhint("create pglogical node first")));


	/* XXX: This is here for backwards compatibility with pre 1.1 extension. */
	if (PG_NARGS() < 2)
	{
		replication_sets = list_make1(DDL_SQL_REPSET_NAME);
	}
	else
	{
		ArrayType  *rep_set_names = PG_GETARG_ARRAYTYPE_P(1);
		replication_sets = textarray_to_list(rep_set_names);
	}

	/* Validate replication sets. */
	foreach(lc, replication_sets)
	{
		char   *setname = lfirst(lc);

		(void) get_replication_set_by_name(node->node->id, setname, false);
	}

	save_nestlevel = NewGUCNestLevel();

	/* Force everything in the query to be fully qualified. */
	(void) set_config_option("search_path", "",
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SAVE, true, 0
#if PG_VERSION_NUM >= 90500
							 , false
#endif
							 );

	/* Convert the query to json string. */
	initStringInfo(&cmd);
	escape_json(&cmd, query);

	/* Queue the query for replication. */
	queue_message(replication_sets, GetUserId(),
				  QUEUE_COMMAND_TYPE_SQL, cmd.data);

	/* Execute the query locally. */
	pglogical_execute_sql_command(query, GetUserNameFromId(GetUserId()
#if PG_VERSION_NUM >= 90500
														   , false
#endif
														   ),
								  false);

	/*
	 * Restore the GUC variables we set above.
	 */
	AtEOXact_GUC(true, save_nestlevel);

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
	List		   *repsets;
	List		   *repset_names;
	ListCell	   *lc;
	StringInfoData	json;
	PGLogicalLocalNode *local_node;

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

	/* If this is not pglogical node, don't do anything. */
	local_node = get_local_node(false, true);
	if (!local_node)
		PG_RETURN_VOID();

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

	repsets = get_relation_replication_sets(local_node->node->id,
											RelationGetRelid(trigdata->tg_relation));

	repset_names = NIL;
	foreach (lc, repsets)
	{
		PGLogicalRepSet	    *repset = (PGLogicalRepSet *) lfirst(lc);
		repset_names = lappend(repset_names, pstrdup(repset->name));
	}

	/* Queue the truncate for replication. */
	queue_message(repset_names, GetUserId(), QUEUE_COMMAND_TYPE_TRUNCATE,
				  json.data);

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
	node = get_local_node(false, true);
	if (!node)
		PG_RETURN_VOID();

	stmt = (DropStmt *)trigdata->parsetree;
	initStringInfo(&logdetail);

	SPI_connect();

	res = SPI_execute("SELECT objid, schema_name, object_name, object_type "
					  "FROM pg_event_trigger_dropped_objects() "
					  "WHERE object_type IN ('table', 'sequence')",
					  false, 0);
	if (res != SPI_OK_SELECT)
		elog(ERROR, "SPI query failed: %d", res);

	for (i = 0; i < SPI_processed; i++)
	{
		Oid		reloid;
		char   *schema_name;
		char   *object_name;
		char   *object_type;
		bool	isnull;
		List   *repsets;

		reloid = (Oid) SPI_getbinval(SPI_tuptable->vals[i],
									 SPI_tuptable->tupdesc, 1, &isnull);
		Assert(!isnull);
		schema_name = SPI_getvalue(SPI_tuptable->vals[i],
								   SPI_tuptable->tupdesc, 2);
		object_name = SPI_getvalue(SPI_tuptable->vals[i],
								   SPI_tuptable->tupdesc, 3);
		object_type = SPI_getvalue(SPI_tuptable->vals[i],
								   SPI_tuptable->tupdesc, 4);

		repsets = get_relation_replication_sets(node->node->id, reloid);

		if (list_length(repsets))
		{
			ListCell	   *lc;

			foreach (lc, repsets)
			{
				PGLogicalRepSet	   *repset = (PGLogicalRepSet *) lfirst(lc);

				if (numDependentObjects++)
					appendStringInfoString(&logdetail, "\n");
				appendStringInfo(&logdetail, "%s %s in replication set %s",
								 object_type,
								 quote_qualified_identifier(schema_name,
															object_name),
								 repset->name);

				/* We always drop on replica. */
				if (stmt->behavior == DROP_CASCADE ||
					SessionReplicationRole == SESSION_REPLICATION_ROLE_REPLICA)
					replication_set_remove_relation(repset->id, reloid, true);
			}
		}

		drop_table_sync_status(schema_name, object_name);
	}

	SPI_finish();

	if (numDependentObjects)
	{
		if (stmt->behavior != DROP_CASCADE &&
			SessionReplicationRole != SESSION_REPLICATION_ROLE_REPLICA)
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

	node = get_local_node(false, false);

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
	char	   *dbname = NameStr(*PG_GETARG_NAME(0));
	char	   *provider_node_name = NameStr(*PG_GETARG_NAME(1));
	char	   *subscription_name = NameStr(*PG_GETARG_NAME(2));
	Name		slot_name;

	slot_name = (Name) palloc0(NAMEDATALEN);

	gen_slot_name(slot_name, dbname, provider_node_name,
				  subscription_name);

	PG_RETURN_NAME(slot_name);
}


/*
 * Generate slot name (used also for origin identifier)
 *
 * The current format is:
 * pgl_<subscriber database name>_<provider node name>_<subscription name>
 *
 * Note that we want to leave enough free space for 8 bytes of suffix
 * which in practice means 9 bytes including the underscore.
 */
static void
gen_slot_name(Name slot_name, char *dbname, const char *provider_node,
			  const char *subscription_name)
{
	char *cp;

	memset(NameStr(*slot_name), 0, NAMEDATALEN);
	snprintf(NameStr(*slot_name), NAMEDATALEN,
			 "pgl_%s_%s_%s",
			 shorten_hash(dbname, 16),
			 shorten_hash(provider_node, 16),
			 shorten_hash(subscription_name, 16));
	NameStr(*slot_name)[NAMEDATALEN-1] = '\0';

	/* Replace all the invalid characters in slot name with underscore. */
	for (cp = NameStr(*slot_name); *cp; cp++)
	{
		if (!((*cp >= 'a' && *cp <= 'z')
			  || (*cp >= '0' && *cp <= '9')
			  || (*cp == '_')))
		{
			*cp = '_';
		}
	}
}

Datum
pglogical_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(PGLOGICAL_VERSION));
}

Datum
pglogical_version_num(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(PGLOGICAL_VERSION_NUM);
}

Datum
pglogical_max_proto_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(PGLOGICAL_MAX_PROTO_VERSION_NUM);
}

Datum
pglogical_min_proto_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(PGLOGICAL_MIN_PROTO_VERSION_NUM);
}
