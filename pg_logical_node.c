/*-------------------------------------------------------------------------
 *
 * pg_logical_node.c
 *		pg_logical node and connection catalog manipulation functions
 *
 * TODO: caching
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pg_logical_node.c
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

#include "pg_logical_node.h"

#define CATALOG_SCHEMA "pglogical"
#define CATALOG_LOCAL_NODE "local_node"
#define CATALOG_NODES "nodes"
#define CATALOG_CONNECTIONS "connections"

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
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	TupleDesc		desc;
	ScanKeyData		key[1];
	bool			isnull;

	rv = makeRangeVar(CATALOG_SCHEMA, CATALOG_NODES, -1);
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

	/* Create and fill the node struct. */
	node = (PGLogicalNode *) palloc(sizeof(PGLogicalNode));
	node->id = nodeid;
	node->name = pstrdup(TextDatumGetCString(fastgetattr(tuple,
														 Anum_nodes_name,
														 desc, &isnull)));
	node->role = DatumGetChar(fastgetattr(tuple, Anum_nodes_role, desc,
										  &isnull));
	node->status = DatumGetChar(fastgetattr(tuple, Anum_nodes_status, desc,
											&isnull));
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

	rv = makeRangeVar(CATALOG_SCHEMA, CATALOG_LOCAL_NODE, -1);
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

	rv = makeRangeVar(CATALOG_SCHEMA, CATALOG_NODES, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	/* Find the node tuple */
	ScanKeyInit(&key[0],
				Anum_nodes_name,
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
}

/*
 * Returns palloc string of quoted identifiers frpm the passed text[].
 */
static char *
textarray_to_identifierlist(ArrayType *textarray)
{
	Datum		   *elems;
	int				nelems, i;
	StringInfoData	si;

	deconstruct_array(textarray,
					  TEXTOID, -1, false, 'i',
					  &elems, NULL, &nelems);

	if (nelems == 0)
		return pstrdup("");

	initStringInfo(&si);

	appendStringInfoString(&si,
		quote_identifier(TextDatumGetCString(elems[0])));
	for (i = 1; i < nelems; i++)
	{
		appendStringInfoString(&si, ",");
		appendStringInfoString(&si,
			quote_identifier(TextDatumGetCString(elems[i])));
	}

	/*
	 * The stringinfo is on the stack, but its data element is palloc'd
	 * in the caller's context and can be returned safely.
	 */
	return si.data;

}

static List *
get_node_connections(int32 nodeid, bool is_origin)
{
	List		   *res = NIL;
	PGLogicalNode  *node;
	RangeVar	   *rv,
				   *nrv;
	Relation		rel,
					nrel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	TupleDesc		desc,
					ndesc;
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

	rv = makeRangeVar(CATALOG_SCHEMA, CATALOG_CONNECTIONS, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	desc = RelationGetDescr(rel);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				searchcolumn,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(nodeid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);

	nrv = makeRangeVar(CATALOG_SCHEMA, CATALOG_NODES, -1);
	nrel = heap_openrv(nrv, RowExclusiveLock);
	ndesc = RelationGetDescr(nrel);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		PGLogicalNode  *cnode;
		PGLogicalConnection *conn;
		SysScanDesc		nscan;
		HeapTuple		ntuple;
		ScanKeyData		nkey[1];
		int				other_node_id;
		Datum			replication_sets;

		other_node_id = DatumGetInt32(fastgetattr(tuple, othercolumn,
												  desc, &isnull));

		/* Search for node record. */
		ScanKeyInit(&nkey[0],
					Anum_nodes_id,
					BTEqualStrategyNumber, F_INT4EQ,
					Int32GetDatum(other_node_id));

		nscan = systable_beginscan(nrel, 0, true, NULL, 1, nkey);
		ntuple = systable_getnext(nscan);

		if (!HeapTupleIsValid(ntuple))
			return NULL;

		conn = (PGLogicalConnection *) palloc(sizeof(PGLogicalConnection));

		/* Get the connection id. */
		conn->id = Int32GetDatum(heap_getattr(tuple, Anum_connections_id,
											  desc, &isnull));

		/* Create and fill the node struct. */
		cnode = (PGLogicalNode *) palloc(sizeof(PGLogicalNode));
		cnode->id = other_node_id;
		cnode->name = pstrdup(TextDatumGetCString(fastgetattr(ntuple,
															  Anum_nodes_name,
															  ndesc,
															  &isnull)));
		cnode->role = DatumGetChar(fastgetattr(ntuple, Anum_nodes_role, ndesc,
											   &isnull));
		cnode->status = DatumGetChar(fastgetattr(ntuple, Anum_nodes_status,
												 ndesc, &isnull));
		cnode->dsn = pstrdup(TextDatumGetCString(fastgetattr(ntuple,
															 Anum_nodes_dsn,
															 ndesc, &isnull)));
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
		replication_sets = heap_getattr(tuple,
										Anum_connections_replication_sets,
										desc, &isnull);

		conn->replication_sets =
			textarray_to_identifierlist(DatumGetArrayTypeP(replication_sets));

		/* Put the connection object to the list. */
		res = lappend(res, conn);

		systable_endscan(nscan);
	}

	heap_close(nrel, RowExclusiveLock);

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

int
get_node_connectionid(int originid, int targetid);

PGLogicalConnection *
get_node_connection_by_id(int connid)
{
	PGLogicalConnection *conn;
	int				originid,
					targetid;
	Datum			replication_sets;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	TupleDesc		desc;
	ScanKeyData		key[1];
	bool			isnull;

	rv = makeRangeVar(CATALOG_SCHEMA, CATALOG_CONNECTIONS, -1);
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

	/* Get node */
	originid = DatumGetInt32(heap_getattr(tuple, Anum_connections_origin_id,
										  desc, &isnull));
	conn->origin = get_node(originid);
	targetid = DatumGetInt32(heap_getattr(tuple, Anum_connections_target_id,
										  desc, &isnull));
	conn->target = get_node(targetid);

	/* Get replication sets */
	replication_sets = heap_getattr(tuple, Anum_connections_replication_sets,
									desc, &isnull);

	conn->replication_sets =
		textarray_to_identifierlist(DatumGetArrayTypeP(replication_sets));

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return conn;
}

void create_node_connection(int originid, int targetid);
void drop_node_connection(int connid);
