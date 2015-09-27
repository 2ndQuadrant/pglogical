/*-------------------------------------------------------------------------
 *
 * pglogical_node.c
 *		pglogical node and connection catalog manipulation functions
 *
 * TODO: caching
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pglogical_node.c
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

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "pglogical_node.h"
#include "pglogical_repset.h"
#include "pglogical.h"

#define CATALOG_LOCAL_NODE	"local_node"
#define CATALOG_NODES		"nodes"
#define CATALOG_CONNECTIONS	"connections"

typedef struct NodeTuple
{
	int32		node_id;
	NameData	node_name;
	char		node_role;
	char		node_status;
	text		node_dsn;
} NodeTuple;

#define Anum_nodes_id		1
#define Anum_nodes_name		2
#define Anum_nodes_role		3
#define Anum_nodes_status	4
#define Anum_nodes_dsn		5

#define Anum_connections_id			1
#define Anum_connections_origin_id	2
#define Anum_connections_target_id	3
#define Anum_connections_replication_sets	4

#define Anum_connections_local_node	1


void create_node(PGLogicalNode *node);
void alter_node(PGLogicalNode *node);
void drop_node(int nodeid);

PGLogicalNode **
get_nodes(void)
{
	return NULL;
}

/*
 * Load the info for specific node.
 */
PGLogicalNode *
get_node(int nodeid)
{
	PGLogicalNode  *node;
	NodeTuple	   *nodetup;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	TupleDesc		desc;
	ScanKeyData		key[1];
	bool			isnull;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_NODES, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				Anum_nodes_id,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(nodeid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "node %d not found", nodeid);

	desc = RelationGetDescr(rel);
	nodetup = (NodeTuple *) GETSTRUCT(tuple);

	/* Create and fill the node struct. */
	node = (PGLogicalNode *) palloc(sizeof(PGLogicalNode));
	node->id = nodetup->node_id;
	node->name = pstrdup(NameStr(nodetup->node_name));
	node->role = nodetup->node_role;
	node->status = nodetup->node_status;
	node->dsn = pstrdup(TextDatumGetCString(fastgetattr(tuple, Anum_nodes_dsn,
														desc, &isnull)));
	node->valid = true;

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return node;
}

/*
 * Load the local node information.
 */
PGLogicalNode *
get_local_node(void)
{
	int				nodeid;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	TupleDesc		desc;
	bool			isnull;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_LOCAL_NODE, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	/* Find the local node tuple. */
	scan = systable_beginscan(rel, 0, true, NULL, 0, NULL);
	tuple = systable_getnext(scan);

	/* No local node record found. */
	if (!HeapTupleIsValid(tuple))
		return NULL;

	desc = RelationGetDescr(rel);

	nodeid = DatumGetInt32(fastgetattr(tuple, Anum_nodes_id, desc, &isnull));

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	/* Find and return the node. */
	return get_node(nodeid);
}

/*
 * Update the status of a node.
 */
void
set_node_status(int nodeid, char status)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];
	bool			tx_started = false;

	if (!IsTransactionState())
	{
		tx_started = true;
		StartTransactionCommand();
	}

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_NODES, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	/* Find the node tuple */
	ScanKeyInit(&key[0],
				Anum_nodes_id,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(nodeid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	/* If found update otherwise throw error. */
	if (HeapTupleIsValid(tuple))
	{
		HeapTuple	newtuple;
		Datum	   *values;
		bool	   *nulls;
		TupleDesc	tupDesc;

		tupDesc = RelationGetDescr(rel);

		values = (Datum *) palloc(tupDesc->natts * sizeof(Datum));
		nulls = (bool *) palloc(tupDesc->natts * sizeof(bool));

		heap_deform_tuple(tuple, tupDesc, values, nulls);

		values[Anum_nodes_status - 1] = CharGetDatum(status);

		newtuple = heap_form_tuple(RelationGetDescr(rel),
								   values, nulls);
		simple_heap_update(rel, &tuple->t_self, newtuple);
		CatalogUpdateIndexes(rel, newtuple);
	}
	else
		elog(ERROR, "node %d not found.", nodeid);

	systable_endscan(scan);

	/* Make the change visible to our tx. */
	CommandCounterIncrement();

	/* Release the lock */
	heap_close(rel, RowExclusiveLock);

	if (tx_started)
		CommitTransactionCommand();
}

/*
 * Convert text array to list of strings.
 *
 * Note: the resulting list points to the memory of the input array.
 */
static List *
textarray_to_list(ArrayType *textarray)
{
	Datum		   *elems;
	int				nelems, i;
	List		   *res = NIL;

	deconstruct_array(textarray,
					  TEXTOID, -1, false, 'i',
					  &elems, NULL, &nelems);

	if (nelems == 0)
		return NIL;

	for (i = 0; i < nelems; i++)
		res = lappend(res, TextDatumGetCString(elems[i]));

	return res;
}

static List *
get_node_connections(int32 nodeid, bool is_origin)
{
	List		   *res = NIL;
	PGLogicalNode  *node;
	RangeVar	   *rv,
				   *noderv;
	Relation		rel,
					noderel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	TupleDesc		desc;
	ScanKeyData		key[1];
	bool			isnull;
	int				searchcolumn,
					othercolumn;

	node = get_node(nodeid);

	if (node == NULL)
		return NULL;

	searchcolumn = is_origin ? Anum_connections_origin_id :
		Anum_connections_target_id;
	othercolumn = is_origin ? Anum_connections_target_id :
		Anum_connections_origin_id;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_CONNECTIONS, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	desc = RelationGetDescr(rel);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				searchcolumn,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(nodeid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);

	noderv = makeRangeVar(EXTENSION_NAME, CATALOG_NODES, -1);
	noderel = heap_openrv(noderv, RowExclusiveLock);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		PGLogicalNode  *cnode;
		NodeTuple	   *cnodetup;
		PGLogicalConnection *conn;
		SysScanDesc		nodescan;
		HeapTuple		nodetuple;
		ScanKeyData		nodekey[1];
		int				other_node_id;
		Datum			d;
		List		   *repset_names;

		other_node_id = DatumGetInt32(fastgetattr(tuple, othercolumn,
												  desc, &isnull));

		/* Search for node record. */
		ScanKeyInit(&nodekey[0],
					Anum_nodes_id,
					BTEqualStrategyNumber, F_INT4EQ,
					Int32GetDatum(other_node_id));

		nodescan = systable_beginscan(noderel, 0, true, NULL, 1, nodekey);
		nodetuple = systable_getnext(nodescan);

		if (!HeapTupleIsValid(nodetuple))
			return NULL;

		conn = (PGLogicalConnection *) palloc(sizeof(PGLogicalConnection));

		/* Get the connection id. */
		conn->id = Int32GetDatum(heap_getattr(tuple, Anum_connections_id,
											  desc, &isnull));

		/* Create and fill the node struct. */
		cnodetup = (NodeTuple *) GETSTRUCT(nodetuple);
		cnode = (PGLogicalNode *) palloc(sizeof(PGLogicalNode));
		node->id = cnodetup->node_id;
		node->name = pstrdup(NameStr(cnodetup->node_name));
		node->role = cnodetup->node_role;
		node->status = cnodetup->node_status;

		d = fastgetattr(nodetuple, Anum_nodes_dsn, desc, &isnull);
		Assert(!isnull);
		node->dsn = pstrdup(TextDatumGetCString(d));

		cnode->valid = true;

		if (is_origin)
		{
			conn->origin = node;
			conn->target = cnode;
		}
		else
		{
			conn->origin = cnode;
			conn->target = node;
		}

		/* Get replication sets */
		d = heap_getattr(tuple, Anum_connections_replication_sets, desc,
						 &isnull);
		repset_names = textarray_to_list(DatumGetArrayTypeP(d));
		conn->replication_sets = get_replication_sets(repset_names);

		/* Put the connection object to the list. */
		res = lappend(res, conn);

		systable_endscan(nodescan);
	}

	heap_close(noderel, RowExclusiveLock);

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return res;
}

List *
get_node_subscribers(int nodeid)
{
	return get_node_connections(nodeid, true);
}

List *
get_node_publishers(int nodeid)
{
	return get_node_connections(nodeid, false);
}

PGLogicalConnection *
get_node_connection(int originid, int targetid, bool missing_ok)
{
	PGLogicalConnection *conn;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	TupleDesc		desc;
	ScanKeyData		key[2];
	bool			isnull;
	Datum			d;
	List		   *repset_names;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_CONNECTIONS, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	desc = RelationGetDescr(rel);

	/* Search for connection record. */
	ScanKeyInit(&key[0],
				Anum_connections_origin_id,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(originid));

	ScanKeyInit(&key[0],
				Anum_connections_target_id,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(targetid));

	scan = systable_beginscan(rel, 0, true, NULL, 2, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
	{
		if (missing_ok)
			return NULL;

		elog(ERROR, "connection between node %d and %d not found", originid,
			 targetid);
	}

	conn = (PGLogicalConnection *) palloc(sizeof(PGLogicalConnection));

	/* Get node id. */
	d = heap_getattr(tuple, Anum_connections_id, desc, &isnull);
	conn->id = DatumGetInt32(d);

	/* Get origin and target nodes. */
	conn->origin = get_node(originid);
	conn->target = get_node(targetid);

	/* Get replication sets. */
	d = heap_getattr(tuple, Anum_connections_replication_sets, desc, &isnull);
	repset_names = textarray_to_list(DatumGetArrayTypeP(d));
	conn->replication_sets = get_replication_sets(repset_names);

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return conn;
}

PGLogicalConnection *
get_node_connection_by_id(int connid)
{
	PGLogicalConnection *conn;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	TupleDesc		desc;
	ScanKeyData		key[1];
	bool			isnull;
	Datum			d;
	List		   *repset_names;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_CONNECTIONS, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	desc = RelationGetDescr(rel);

	/* Search for connection record. */
	ScanKeyInit(&key[0],
				Anum_connections_id,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(connid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "connection %d not found", connid);

	conn = (PGLogicalConnection *) palloc(sizeof(PGLogicalConnection));

	conn->id = connid;

	/* Get origin node */
	d = heap_getattr(tuple, Anum_connections_origin_id, desc, &isnull);
	conn->origin = get_node(DatumGetInt32(d));

	/* Get target node */
	d = heap_getattr(tuple, Anum_connections_target_id, desc, &isnull);
	conn->target = get_node(DatumGetInt32(d));

	/* Get replication sets */
	d = heap_getattr(tuple, Anum_connections_replication_sets, desc, &isnull);
	repset_names = textarray_to_list(DatumGetArrayTypeP(d));
	conn->replication_sets = get_replication_sets(repset_names);

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return conn;
}

void create_node_connection(int originid, int targetid);
void drop_node_connection(int connid);
