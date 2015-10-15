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
#include "catalog/pg_type.h"

#include "miscadmin.h"

#include "nodes/makefuncs.h"

#include "replication/reorderbuffer.h"

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

#include "pglogical.h"

/* Filter hooks for output plugin. */
PG_FUNCTION_INFO_V1(pglogical_origin_filter);
PG_FUNCTION_INFO_V1(pglogical_table_filter);

/* Node management. */
PG_FUNCTION_INFO_V1(pglogical_create_node);
PG_FUNCTION_INFO_V1(pglogical_drop_node);
PG_FUNCTION_INFO_V1(pglogical_create_connection);
PG_FUNCTION_INFO_V1(pglogical_drop_connection);

/* Replication set manipulation. */
PG_FUNCTION_INFO_V1(pglogical_create_replication_set);
PG_FUNCTION_INFO_V1(pglogical_drop_replication_set);
PG_FUNCTION_INFO_V1(pglogical_replication_set_add_table);
PG_FUNCTION_INFO_V1(pglogical_replication_set_remove_table);

/* DDL */
PG_FUNCTION_INFO_V1(pglogical_replicate_ddl_command);

/*
 * Filter based on origin, currently we only support all or nothing only.
 */
Datum
pglogical_origin_filter(PG_FUNCTION_ARGS)
{
	const char *forward_origin = TextDatumGetCString(PG_GETARG_DATUM(0));

	PG_RETURN_BOOL(strcmp(forward_origin, REPLICATION_ORIGIN_ALL) != 0);
}

/*
 * Filter based on change_type on a relation.
 */
Datum
pglogical_table_filter(PG_FUNCTION_ARGS)
{
	const char *remote_node_name = TextDatumGetCString(PG_GETARG_DATUM(0));
	Oid			relid = PG_GETARG_OID(1);
	char		change_type = PG_GETARG_CHAR(2);
	PGLogicalNode  *remote_node = get_node_by_name(remote_node_name, false);
	PGLogicalNode  *local_node = get_local_node();
	PGLogicalConnection *conn = find_node_connection(local_node->id,
													 remote_node->id,
													 false);
	Relation		rel;
	enum ReorderBufferChangeType action;
	bool			res;

	if (relid == get_queue_table_oid())
		PG_RETURN_BOOL(false);

	switch (change_type)
	{
		case 'I':
			action = REORDER_BUFFER_CHANGE_INSERT;
			break;
		case 'U':
			action = REORDER_BUFFER_CHANGE_UPDATE;
			break;
		case 'D':
			action = REORDER_BUFFER_CHANGE_DELETE;
			break;
		default:
			elog(ERROR, "unknown change type %c", change_type);
			action = 0;      /* silence compiler */
	}

	rel = relation_open(relid, NoLock);
	res = relation_is_replicated(rel, conn, action);
	relation_close(rel, NoLock);

	PG_RETURN_BOOL(!res);
}

/*
 * Create new node record and insert it into catalog.
 */
Datum
pglogical_create_node(PG_FUNCTION_ARGS)
{
	PGLogicalNode	node;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("node name cannot be null")));

	if (PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("node role cannot be null")));

	if (PG_ARGISNULL(2))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("node dsn cannot be null")));

	node.name = NameStr(*PG_GETARG_NAME(0));
	node.role = PG_GETARG_CHAR(1);
	node.dsn = TextDatumGetCString(PG_GETARG_DATUM(2));

	if (PG_ARGISNULL(3))
		node.init_dsn = NULL;
	else
		node.init_dsn = TextDatumGetCString(PG_GETARG_DATUM(3));

	node.id = InvalidOid;
	node.status = NODE_STATUS_INIT;

	create_node(&node);

	/* TODO: run the init. */

	PG_RETURN_INT32(node.id);
}

/*
 * Drop the named node.
 */
Datum
pglogical_drop_node(PG_FUNCTION_ARGS)
{
	PGLogicalNode  *node;

	node = get_node_by_name(NameStr(*PG_GETARG_NAME(0)), false);

	drop_node(node->id);

	/* TODO: notify the workers. */

	PG_RETURN_VOID();
}

/*
 * Connect two existing nodes.
 */
Datum
pglogical_create_connection(PG_FUNCTION_ARGS)
{
	PGLogicalNode  *origin;
	PGLogicalNode  *target;
	List		   *replication_sets;
	int				connid;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("origin name cannot be null")));

	if (PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("target name cannot be null")));

	origin = get_node_by_name(NameStr(*PG_GETARG_NAME(0)), false);
	target = get_node_by_name(NameStr(*PG_GETARG_NAME(1)), false);

	if (PG_ARGISNULL(2))
		replication_sets = NIL;
	else
		replication_sets = textarray_to_list(PG_GETARG_ARRAYTYPE_P(2));

	connid = create_node_connection(origin->id, target->id, replication_sets);

	/* TODO: notify the workers. */

	PG_RETURN_INT32(connid);
}

/*
 * Remove connection between two nodes.
 */
Datum
pglogical_drop_connection(PG_FUNCTION_ARGS)
{
	PGLogicalNode  *origin;
	PGLogicalNode  *target;
	PGLogicalConnection *conn;

	origin = get_node_by_name(NameStr(*PG_GETARG_NAME(0)), false);
	target = get_node_by_name(NameStr(*PG_GETARG_NAME(1)), false);

	conn = find_node_connection(origin->id, target->id, false);

	drop_node_connection(conn->id);

	/* TODO: notify the workers. */

	PG_RETURN_VOID();
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

	repset.replicate_inserts = PG_GETARG_BOOL(1);
	repset.replicate_updates = PG_GETARG_BOOL(2);
	repset.replicate_deletes = PG_GETARG_BOOL(3);

	create_replication_set(&repset);

	PG_RETURN_INT32(repset.id);
}

/*
 * Drop existing replication set.
 */
Datum
pglogical_drop_replication_set(PG_FUNCTION_ARGS)
{
	PGLogicalRepSet    *repset;

	repset = get_replication_set_by_name(NameStr(*PG_GETARG_NAME(0)), false);

	drop_replication_set(repset->id);

	PG_RETURN_VOID();
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

	PG_RETURN_VOID();
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

	replication_set_remove_table(repset->id, reloid);

	PG_RETURN_VOID();
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
	queue_command(GetUserId(), QUEUE_COMMAND_TYPE_SQL, cmd.data);

	/* Execute the query locally. */
	pglogical_execute_sql_command(query, GetUserNameFromId(GetUserId(), false),
								  false);

	PG_RETURN_VOID();
}

