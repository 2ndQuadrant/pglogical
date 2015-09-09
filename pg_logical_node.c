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
void drop_node(const char *nodename);

PGLogicalNode **
get_nodes()
{
}

/*
 * Load the info for specific node.
 */
static PGLogicalNode *
get_node(const char *node_name)
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
				BTEqualStrategyNumber, F_TEXTEQ,
				CStringGetTextDatum(node_name));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		return NULL;

	desc = RelationGetDescr(rel);

	/* Create and fill the node struct. */
	node = palloc0(sizeof(PGLogicalNode));
	node->name = pstrdup(node_name);
	node->role = DatumGetChar(fastgetattr(tuple, 2, desc, &isnull));
	node->status = DatumGetChar(fastgetattr(tuple, 3, desc, &isnull));
	node->dsn = TextDatumGetCString(fastgetattr(tuple, 4, desc, &isnull));
	node->valid = true;

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return node;
}

/*
 * Load the local node information.
 */
PGLogicalNode *
get_local_node()
{
	char		   *node_name;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	TupleDesc		desc;
	ScanKeyData		key[1];
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

	node_name = TextDatumGetCString(fastgetattr(tuple, 0, desc, &isnull));

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	/* Find and return the node. */
	return get_node(node_name);
}

/*
 * Update the status of a node.
 */
void
set_node_status(const char *node_name, char status)
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

	/* Find the node tuple */
	ScanKeyInit(&key[0],
				1,
				BTEqualStrategyNumber, F_TEXTEQ,
				CStringGetTextDatum(node_name));

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
		elog(ERROR, "node %s not found.", node_name);

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
get_node_connections(const char *node_name, bool is_origin)
{
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

	node = get_node(node_name);

	if (node == NULL)
		return NULL;

	rv = makeRangeVar(CATALOG_SCHEMA, CATALOG_CONNECTIONS, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	desc = RelationGetDescr(rel);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				is_origin ? 1 : 2,
				BTEqualStrategyNumber, F_TEXTEQ,
				CStringGetTextDatum(node_name));

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
		char		   *origin_node_name;
		Datum			replication_sets;

		origin_node_name = TextDatumGetCString(fastgetattr(tuple, 1, desc,
														   &isnull));

		/* Search for node record. */
		ScanKeyInit(&nkey[0],
					1,
					BTEqualStrategyNumber, F_TEXTEQ,
					CStringGetTextDatum(origin_node_name));

		nscan = systable_beginscan(nrel, 0, true, NULL, 1, nkey);
		ntuple = systable_getnext(nscan);

		if (!HeapTupleIsValid(ntuple))
			return NULL;

		conn = palloc0(sizeof(PGLogicalConnection));

		/* Create and fill the node struct. */
		cnode = palloc0(sizeof(PGLogicalNode));
		cnode->name = pstrdup(node_name);
		cnode->role = DatumGetChar(fastgetattr(ntuple, 2, ndesc, &isnull));
		cnode->status = DatumGetChar(fastgetattr(ntuple, 3, ndesc, &isnull));
		cnode->dsn = TextDatumGetCString(fastgetattr(ntuple, 4, ndesc,
													 &isnull));
		cnode->valid = true;

		conn->node = cnode;

		/* Get replication sets */
		replication_sets = heap_getattr(tuple, 3, desc, &isnull);

		conn->replication_sets =
			textarray_to_identifierlist(DatumGetArrayTypeP(replication_sets));

		systable_endscan(nscan);
	}

	heap_close(nrel, RowExclusiveLock);

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);
}

List *
get_node_subscribers(const char *node_name)
{
	return get_node_connections(node_name, true);
}

List *
get_node_publishers(const char *node_name)
{
	return get_node_connections(node_name, false);
}

void create_node_connection(const char *origin_name, const char *target_name);
void drop_node_connection(const char *origin_name, const char *target_name);
