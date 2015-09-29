/*-------------------------------------------------------------------------
 *
 * pglogical_hooks.c
 *		pglogical hooks for output plugin
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pglogical_hooks.c
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

#include "nodes/makefuncs.h"

#include "replication/reorderbuffer.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "pglogical_node.h"
#include "pglogical_repset.h"
#include "pglogical.h"

PGDLLEXPORT Datum pglogical_origin_filter(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pglogical_origin_filter);

PGDLLEXPORT Datum pglogical_table_filter(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pglogical_table_filter);

Datum
pglogical_origin_filter(PG_FUNCTION_ARGS)
{
	const char *forward_origin = TextDatumGetCString(PG_GETARG_DATUM(0));

	PG_RETURN_BOOL(strcmp(forward_origin, REPLICATION_ORIGIN_ALL) != 0);
}

/*
 * Filter change_type on a relation.
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
