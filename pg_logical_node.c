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

#define CATALOG_SCHEMA "pg_logical"
#define CATALOG_LOCAL_NODE "local_node"
#define CATALOG_NODES "node"
#define CATALOG_CONNECTIONS "connections"


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
				1,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(nodeid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		return NULL;

	desc = RelationGetDescr(rel);

	/* Create and fill the node struct. */
	node = palloc0(sizeof(PGLogicalNode));
	node->id = nodeid;
	node->name = pstrdup(TextDatumGetCString(fastgetattr(tuple, 1, desc,
														 &isnull)));
	node->role = DatumGetChar(fastgetattr(tuple, 3, desc, &isnull));
	node->status = DatumGetChar(fastgetattr(tuple, 4, desc, &isnull));
	node->dsn = pstrdup(TextDatumGetCString(fastgetattr(tuple, 5, desc,
														&isnull)));
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

	nodeid = DatumGetInt32(fastgetattr(tuple, 1, desc, &isnull));

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
				1,
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
		AttrNumber	attnum = get_attnum(rel->rd_id, "node_status");

		tupDesc = RelationGetDescr(rel);

		values = (Datum *) palloc(tupDesc->natts * sizeof(Datum));
		nulls = (bool *) palloc(tupDesc->natts * sizeof(bool));

		heap_deform_tuple(tuple, tupDesc, values, nulls);

		values[attnum - 1] = CharGetDatum(status);

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

	node = get_node(nodeid);

	if (node == NULL)
		return NULL;

	rv = makeRangeVar(CATALOG_SCHEMA, CATALOG_CONNECTIONS, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	desc = RelationGetDescr(rel);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				is_origin ? 2 : 3,
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

		other_node_id = DatumGetInt32(fastgetattr(tuple, 2, desc, &isnull));

		/* Search for node record. */
		ScanKeyInit(&nkey[0],
					1,
					BTEqualStrategyNumber, F_INT4EQ,
					Int32GetDatum(other_node_id));

		nscan = systable_beginscan(nrel, 0, true, NULL, 1, nkey);
		ntuple = systable_getnext(nscan);

		if (!HeapTupleIsValid(ntuple))
			return NULL;

		conn = palloc0(sizeof(PGLogicalConnection));

		/* Get the connection id. */
		conn->id = Int32GetDatum(heap_getattr(tuple, 1, desc, &isnull));

		/* Create and fill the node struct. */
		cnode = palloc0(sizeof(PGLogicalNode));
		cnode->id = other_node_id;
		cnode->name = pstrdup(TextDatumGetCString(fastgetattr(tuple, 2, ndesc,
															  &isnull)));
		cnode->role = DatumGetChar(fastgetattr(tuple, 3, ndesc, &isnull));
		cnode->status = DatumGetChar(fastgetattr(tuple, 4, ndesc, &isnull));
		cnode->dsn = pstrdup(TextDatumGetCString(fastgetattr(tuple, 5, ndesc,
															 &isnull)));
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
		replication_sets = heap_getattr(tuple, 4, desc, &isnull);

		conn->replication_sets =
			textarray_to_identifierlist(DatumGetArrayTypeP(replication_sets));

		lappend(res, conn);

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
				1,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(connid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		return NULL;

	conn = palloc0(sizeof(PGLogicalConnection));

	conn->id = connid;

	/* Get node */
	originid = DatumGetInt32(heap_getattr(tuple, 2, desc, &isnull));
	conn->origin = get_node(originid);
	targetid = DatumGetInt32(heap_getattr(tuple, 3, desc, &isnull));
	conn->target = get_node(targetid);

	/* Get replication sets */
	replication_sets = heap_getattr(tuple, 4, desc, &isnull);

	conn->replication_sets =
		textarray_to_identifierlist(DatumGetArrayTypeP(replication_sets));

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return conn;
}

void create_node_connection(int originid, int targetid);
void drop_node_connection(int connid);
