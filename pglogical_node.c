/*-------------------------------------------------------------------------
 *
 * pglogical_node.c
 *		pglogical node and subscription catalog manipulation functions
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
#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"

#include "catalog/indexing.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_type.h"

#include "commands/dbcommands.h"

#include "miscadmin.h"

#include "nodes/makefuncs.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "pglogical_node.h"
#include "pglogical_repset.h"
#include "pglogical_worker.h"
#include "pglogical.h"

#define CATALOG_NODE			"node"
#define CATALOG_LOCAL_NODE		"local_node"
#define CATALOG_NODE_INTERFACE	"node_interface"
#define CATALOG_SUBSCRIPTION	"subscription"

typedef struct NodeTuple
{
	Oid			node_id;
	NameData	node_name;
} NodeTuple;

#define Natts_node			4
#define Anum_node_id		1
#define Anum_node_name		2

#define Natts_local_node			2
#define Anum_node_local_id			1
#define Anum_node_local_node_if		2

typedef struct NodeInterfaceTuple
{
	Oid			if_id;
	NameData	if_name;
	Oid			if_nodeid;
	text		if_dsn;
} NodeInterfaceTuple;

#define Natts_node_inteface	4
#define Anum_if_id			1
#define Anum_if_name		2
#define Anum_if_nodeid		3
#define Anum_if_dsn			4

typedef struct SubscriptionTuple
{
	Oid			sub_id;
	NameData	sub_name;
	Oid			sub_origin;
	Oid			sub_target;
    Oid			sub_origin_if;
	Oid			sub_target_if;
	bool		sub_enabled;
	NameData	sub_slot_name;
} SubscriptionTuple;

#define Natts_subscription			12
#define Anum_sub_id					1
#define Anum_sub_name				2
#define Anum_sub_origin				3
#define Anum_sub_target				4
#define Anum_sub_origin_if			5
#define Anum_sub_target_if			6
#define Anum_sub_enabled			7
#define Anum_sub_slot_name			8
#define Anum_sub_replication_sets	9
#define Anum_sub_forward_origins	10
#define Anum_sub_apply_delay		11
#define Anum_sub_force_text_transfer 12

/*
 * We impose same validation rules as replication slot name validation does.
 */
static void
validate_subscription_name(const char *name)
{
	const char *cp;

	if (strlen(name) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("subscription  name \"%s\" is too short", name)));

	if (strlen(name) >= NAMEDATALEN)
		ereport(ERROR,
				(errcode(ERRCODE_NAME_TOO_LONG),
				 errmsg("subscription name \"%s\" is too long", name)));

	for (cp = name; *cp; cp++)
	{
		if (!((*cp >= 'a' && *cp <= 'z')
			  || (*cp >= '0' && *cp <= '9')
			  || (*cp == '_')))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_NAME),
					 errmsg("subscription name \"%s\" contains invalid character",
							name),
					 errhint("Subscription names may only contain lower case "
							 "letters, numbers, and the underscore character.")));
		}
	}
}

/*
 * Add new node to catalog.
 */
void
create_node(PGLogicalNode *node)
{
	RangeVar   *rv;
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_node];
	bool		nulls[Natts_node];
	NameData	node_name;

	if (get_node_by_name(node->name, true) != NULL)
		elog(ERROR, "node %s already exists", node->name);

	/* Generate new id unless one was already specified. */
	if (node->id == InvalidOid)
		node->id =
			DatumGetUInt32(hash_any((const unsigned char *) node->name,
									strlen(node->name)));

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_NODE, -1);
	rel = table_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* Form a tuple. */
	memset(nulls, false, sizeof(nulls));

	values[Anum_node_id - 1] = ObjectIdGetDatum(node->id);
	namestrcpy(&node_name, node->name);
	values[Anum_node_name - 1] = NameGetDatum(&node_name);

	tup = heap_form_tuple(tupDesc, values, nulls);

	/* Insert the tuple to the catalog. */
	CatalogTupleInsert(rel, tup);

	/* Cleanup. */
	heap_freetuple(tup);
	table_close(rel, NoLock);

	CommandCounterIncrement();

	pglogical_subscription_changed(InvalidOid, false);
}

/*
 * Delete node from the catalog.
 */
void
drop_node(Oid nodeid)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_NODE, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				Anum_node_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(nodeid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "node %u not found", nodeid);

	/* Remove the tuple. */
	simple_heap_delete(rel, &tuple->t_self);

	/* Cleanup. */
	systable_endscan(scan);
	table_close(rel, NoLock);

	CommandCounterIncrement();

	pglogical_subscription_changed(InvalidOid, false);
}

static PGLogicalNode *
node_fromtuple(HeapTuple tuple)
{
	NodeTuple *nodetup = (NodeTuple *) GETSTRUCT(tuple);
	PGLogicalNode *node
		= (PGLogicalNode *) palloc(sizeof(PGLogicalNode));
	node->id = nodetup->node_id;
	node->name = pstrdup(NameStr(nodetup->node_name));
	return node;
}

/*
 * Load the info for specific node.
 */
PGLogicalNode *
get_node(Oid nodeid)
{
	PGLogicalNode  *node;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_NODE, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				Anum_node_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(nodeid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "node %u not found", nodeid);

	node = node_fromtuple(tuple);

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	return node;
}

/*
 * Load the info for specific node.
 */
PGLogicalNode *
get_node_by_name(const char *name, bool missing_ok)
{
	PGLogicalNode  *node;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_NODE, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				Anum_node_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(name));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
	{
		if (missing_ok)
		{
			systable_endscan(scan);
			table_close(rel, RowExclusiveLock);
			return NULL;
		}

		elog(ERROR, "node %s not found", name);
	}

	node = node_fromtuple(tuple);

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	return node;
}

/*
 * Add local node record to catalog.
 */
void
create_local_node(Oid nodeid, Oid ifid)
{
	RangeVar   *rv;
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_local_node];
	bool		nulls[Natts_local_node];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_LOCAL_NODE, -1);
	rel = table_openrv(rv, AccessExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* TODO: better error message */
	if (get_local_node(false, true))
		elog(ERROR, "current database is already configured as pglogical node");

	/* Form a tuple. */
	memset(nulls, false, sizeof(nulls));

	values[Anum_node_local_id - 1] = ObjectIdGetDatum(nodeid);
	values[Anum_node_local_node_if - 1] = ObjectIdGetDatum(ifid);

	tup = heap_form_tuple(tupDesc, values, nulls);

	/* Insert the tuple to the catalog. */
	CatalogTupleInsert(rel, tup);

	/* Cleanup. */
	heap_freetuple(tup);
	table_close(rel, AccessExclusiveLock);

	CommandCounterIncrement();
}

/*
 * Drop local node record from catalog.
 */
void
drop_local_node(void)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_LOCAL_NODE, -1);
	rel = table_openrv(rv, AccessExclusiveLock);

	/* Find the local node tuple. */
	scan = systable_beginscan(rel, 0, true, NULL, 0, NULL);
	tuple = systable_getnext(scan);

	/* No local node record found. */
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("local node not found")));

	/* Remove the tuple. */
	simple_heap_delete(rel, &tuple->t_self);

	/* Cleanup. */
	systable_endscan(scan);
	table_close(rel, NoLock);

	CommandCounterIncrement();
}

/*
 * Return local node.
 */
PGLogicalLocalNode *
get_local_node(bool for_update, bool missing_ok)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	TupleDesc		desc;
	Oid				nodeid;
	Oid				nodeifid;
	bool			isnull;
	PGLogicalLocalNode *res;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_LOCAL_NODE, -1);
	rel = table_openrv_extended(rv, for_update ?
							   ShareUpdateExclusiveLock : RowExclusiveLock,
							   true);

	if (!rel)
	{
		if (missing_ok)
			return NULL;

		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("local pglogical node not found")));
	}

	/* Find the local node tuple. */
	scan = systable_beginscan(rel, 0, true, NULL, 0, NULL);
	tuple = systable_getnext(scan);

	/* No local node record found. */
	if (!HeapTupleIsValid(tuple))
	{
		if (missing_ok)
		{
			systable_endscan(scan);
			table_close(rel, for_update ?
					   NoLock : RowExclusiveLock);
			return NULL;
		}

		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("local pglogical node not found")));
	}

	desc = RelationGetDescr(rel);

	nodeid = DatumGetObjectId(fastgetattr(tuple, Anum_node_local_id, desc,
										  &isnull));
	nodeifid = DatumGetObjectId(fastgetattr(tuple, Anum_node_local_node_if,
											desc, &isnull));

	systable_endscan(scan);
	table_close(rel, for_update ? NoLock : RowExclusiveLock);

	res = (PGLogicalLocalNode *) palloc(sizeof(PGLogicalLocalNode));
	res->node = get_node(nodeid);
	res->node_if = get_node_interface(nodeifid);

	return res;
}

/*
 * Add new node interface to catalog.
 */
void
create_node_interface(PGlogicalInterface *nodeif)
{
	RangeVar   *rv;
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_node_inteface];
	bool		nulls[Natts_node_inteface];
	NameData	nodeif_name;

	/* Generate new id unless one was already specified. */
	if (nodeif->id == InvalidOid)
	{
		uint32	hashinput[2];

		hashinput[0] = nodeif->nodeid;
		hashinput[1] = DatumGetUInt32(hash_any((const unsigned char *) nodeif->name,
											   strlen(nodeif->name)));

		nodeif->id = DatumGetUInt32(hash_any((const unsigned char *) hashinput,
											 (int) sizeof(hashinput)));
	}

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_NODE_INTERFACE, -1);
	rel = table_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* Form a tuple. */
	memset(nulls, false, sizeof(nulls));

	values[Anum_if_id - 1] = ObjectIdGetDatum(nodeif->id);
	namestrcpy(&nodeif_name, nodeif->name);
	values[Anum_if_name - 1] = NameGetDatum(&nodeif_name);
	values[Anum_if_nodeid - 1] = ObjectIdGetDatum(nodeif->nodeid);
	values[Anum_if_dsn - 1] = CStringGetTextDatum(nodeif->dsn);

	tup = heap_form_tuple(tupDesc, values, nulls);

	/* Insert the tuple to the catalog. */
	CatalogTupleInsert(rel, tup);

	/* Cleanup. */
	heap_freetuple(tup);
	table_close(rel, RowExclusiveLock);

	CommandCounterIncrement();
}

/*
 * Delete node interface from the catalog.
 */
void
drop_node_interface(Oid ifid)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_NODE_INTERFACE, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				Anum_if_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ifid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "node interface %u not found", ifid);

	/* Remove the tuple. */
	simple_heap_delete(rel, &tuple->t_self);

	/* Cleanup. */
	systable_endscan(scan);
	table_close(rel, NoLock);

	CommandCounterIncrement();
}

/*
 * Delete all node interfaces from the catalog.
 */
void
drop_node_interfaces(Oid nodeid)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_NODE_INTERFACE, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				Anum_if_nodeid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(nodeid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);

	/* Remove tuples. */
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
		simple_heap_delete(rel, &tuple->t_self);

	/* Cleanup. */
	systable_endscan(scan);
	table_close(rel, NoLock);

	CommandCounterIncrement();
}

/*
 * Get the node interface from the catalog.
 */
PGlogicalInterface *
get_node_interface(Oid ifid)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];
	NodeInterfaceTuple *iftup;
	PGlogicalInterface *nodeif;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_NODE_INTERFACE, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				Anum_if_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ifid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "node interface %u not found", ifid);

	iftup = (NodeInterfaceTuple *) GETSTRUCT(tuple);
	nodeif = (PGlogicalInterface *) palloc(sizeof(PGlogicalInterface));
	nodeif->id = iftup->if_id;
	nodeif->name = pstrdup(NameStr(iftup->if_name));
	nodeif->nodeid = iftup->if_nodeid;
	nodeif->dsn = pstrdup(text_to_cstring(&iftup->if_dsn));

	/* Cleanup. */
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	return nodeif;
}

/*
 * Get the node interface by name.
 */
PGlogicalInterface *
get_node_interface_by_name(Oid nodeid, const char *name, bool missing_ok)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[2];
	NodeInterfaceTuple *iftup;
	PGlogicalInterface *nodeif;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_NODE_INTERFACE, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Search for interface record. */
	ScanKeyInit(&key[0],
				Anum_if_nodeid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(nodeid));
	ScanKeyInit(&key[1],
				Anum_if_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(name));

	scan = systable_beginscan(rel, 0, true, NULL, 2, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
	{
		if (missing_ok)
		{
			systable_endscan(scan);
			table_close(rel, RowExclusiveLock);

			return NULL;
		}
		else
			elog(ERROR, "node interface \"%s\" not found for nod %u",
				 name, nodeid);
	}

	iftup = (NodeInterfaceTuple *) GETSTRUCT(tuple);
	nodeif = (PGlogicalInterface *) palloc(sizeof(PGlogicalInterface));
	nodeif->id = iftup->if_id;
	nodeif->name = pstrdup(NameStr(iftup->if_name));
	nodeif->nodeid = iftup->if_nodeid;
	nodeif->dsn = pstrdup(text_to_cstring(&iftup->if_dsn));

	/* Cleanup. */
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	return nodeif;
}

/*
 * Add new subscription to catalog.
 */
void
create_subscription(PGLogicalSubscription *sub)
{
	RangeVar   *rv;
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_subscription];
	bool		nulls[Natts_subscription];
	NameData	sub_name;
	NameData	sub_slot_name;

	/* Validate the new subscription name. */
	validate_subscription_name(sub->name);

	if (get_subscription_by_name(sub->name, true) != NULL)
		elog(ERROR, "subscription %s already exists", sub->name);

	/* Generate new id unless one was already specified. */
	if (sub->id == InvalidOid)
		sub->id =
			DatumGetObjectId(hash_any((const unsigned char *) sub->name,
									  strlen(sub->name)));

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_SUBSCRIPTION, -1);
	rel = table_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* Form a tuple. */
	memset(nulls, false, sizeof(nulls));

	values[Anum_sub_id - 1] = ObjectIdGetDatum(sub->id);
	namestrcpy(&sub_name, sub->name);
	values[Anum_sub_name - 1] = NameGetDatum(&sub_name);
	values[Anum_sub_origin - 1] = ObjectIdGetDatum(sub->origin_if->nodeid);
	values[Anum_sub_target - 1] = ObjectIdGetDatum(sub->target_if->nodeid);
	values[Anum_sub_origin_if - 1] = ObjectIdGetDatum(sub->origin_if->id);
	values[Anum_sub_target_if - 1] = ObjectIdGetDatum(sub->target_if->id);
	values[Anum_sub_enabled - 1] = BoolGetDatum(sub->enabled);
	namestrcpy(&sub_slot_name, sub->slot_name);
	values[Anum_sub_slot_name - 1] = NameGetDatum(&sub_slot_name);

	if (list_length(sub->replication_sets) > 0)
		values[Anum_sub_replication_sets - 1] =
			PointerGetDatum(strlist_to_textarray(sub->replication_sets));
	else
		nulls[Anum_sub_replication_sets - 1] = true;

	if (list_length(sub->forward_origins) > 0)
		values[Anum_sub_forward_origins - 1] =
			PointerGetDatum(strlist_to_textarray(sub->forward_origins));
	else
		nulls[Anum_sub_forward_origins - 1] = true;

	if (sub->apply_delay)
		values[Anum_sub_apply_delay - 1] = IntervalPGetDatum(sub->apply_delay);
	else
		nulls[Anum_sub_apply_delay - 1] = true;

	values[Anum_sub_force_text_transfer - 1] = BoolGetDatum(sub->force_text_transfer);

	tup = heap_form_tuple(tupDesc, values, nulls);

	/* Insert the tuple to the catalog. */
	CatalogTupleInsert(rel, tup);

	/* Cleanup. */
	heap_freetuple(tup);
	table_close(rel, RowExclusiveLock);

	CommandCounterIncrement();

	pglogical_subscription_changed(sub->id, true);
}

/*
 * Change the subscription tuple.
 */
void
alter_subscription(PGLogicalSubscription *sub)
{
	RangeVar   *rv;
	Relation	rel;
	TupleDesc	tupDesc;
	SysScanDesc	scan;
	SubscriptionTuple	*oldsub;
	HeapTuple	oldtup,
				newtup;
	ScanKeyData	key[1];
	Datum		values[Natts_subscription];
	bool		nulls[Natts_subscription];
	bool		replaces[Natts_subscription];
	NameData	sub_slot_name;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_SUBSCRIPTION, -1);
	rel = table_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				Anum_sub_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(sub->id));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	oldtup = systable_getnext(scan);

	if (!HeapTupleIsValid(oldtup))
		elog(ERROR, "subscription %u not found", sub->id);

	oldsub = (SubscriptionTuple *) GETSTRUCT(oldtup);
	if (strcmp(NameStr(oldsub->sub_name), sub->name) != 0)
		ereport(LOG,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("subscription name change is not supported")));

	/* Form a tuple. */
	memset(nulls, false, sizeof(nulls));
	memset(replaces, true, sizeof(replaces));

	replaces[Anum_sub_id - 1] = false;
	replaces[Anum_sub_name - 1] = false;

	values[Anum_sub_origin - 1] = ObjectIdGetDatum(sub->origin_if->nodeid);
	values[Anum_sub_target - 1] = ObjectIdGetDatum(sub->target_if->nodeid);
	values[Anum_sub_origin_if - 1] = ObjectIdGetDatum(sub->origin_if->id);
	values[Anum_sub_target_if - 1] = ObjectIdGetDatum(sub->target_if->id);
	values[Anum_sub_enabled - 1] = BoolGetDatum(sub->enabled);
	namestrcpy(&sub_slot_name, sub->slot_name);
	values[Anum_sub_slot_name - 1] = NameGetDatum(&sub_slot_name);

	if (list_length(sub->replication_sets) > 0)
		values[Anum_sub_replication_sets - 1] =
			PointerGetDatum(strlist_to_textarray(sub->replication_sets));
	else
		nulls[Anum_sub_replication_sets - 1] = true;

	if (list_length(sub->forward_origins) > 0)
		values[Anum_sub_forward_origins - 1] =
			PointerGetDatum(strlist_to_textarray(sub->forward_origins));
	else
		nulls[Anum_sub_forward_origins - 1] = true;

	values[Anum_sub_apply_delay - 1] = IntervalPGetDatum(sub->apply_delay);
	values[Anum_sub_force_text_transfer - 1] = BoolGetDatum(sub->force_text_transfer);

	newtup = heap_modify_tuple(oldtup, tupDesc, values, nulls, replaces);

	/* Update the tuple in catalog. */
	CatalogTupleUpdate(rel, &oldtup->t_self, newtup);

	/* Cleanup. */
	heap_freetuple(newtup);
	systable_endscan(scan);
	table_close(rel, NoLock);

	CommandCounterIncrement();

	pglogical_subscription_changed(sub->id, true);
}

/*
 * Delete the tuple from subsription catalog.
 */
void
drop_subscription(Oid subid)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_SUBSCRIPTION, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				Anum_sub_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(subid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "subscription %u not found", subid);

	/* Remove the tuple. */
	simple_heap_delete(rel, &tuple->t_self);

	/* Cleanup. */
	systable_endscan(scan);
	table_close(rel, NoLock);

	CommandCounterIncrement();

	pglogical_subscription_changed(subid, true);
}

static PGLogicalSubscription*
subscription_fromtuple(HeapTuple tuple, TupleDesc desc)
{
	SubscriptionTuple *subtup = (SubscriptionTuple *) GETSTRUCT(tuple);
	Datum		d;
	bool		isnull;

	PGLogicalSubscription *sub =
		(PGLogicalSubscription *) palloc(sizeof(PGLogicalSubscription));
	sub->id = subtup->sub_id;
	sub->name = pstrdup(NameStr(subtup->sub_name));
	sub->enabled = subtup->sub_enabled;
	sub->slot_name = pstrdup(NameStr(subtup->sub_slot_name));

	sub->origin = get_node(subtup->sub_origin);
	sub->target = get_node(subtup->sub_target);
	sub->origin_if = get_node_interface(subtup->sub_origin_if);
	sub->target_if = get_node_interface(subtup->sub_target_if);

	/* Get replication sets. */
	d = heap_getattr(tuple, Anum_sub_replication_sets, desc, &isnull);
	if (isnull)
		sub->replication_sets = NIL;
	else
	{
		List		   *repset_names;
		repset_names = textarray_to_list(DatumGetArrayTypeP(d));
		sub->replication_sets = repset_names;
	}

	/* Get origin forwarding. */
	d = heap_getattr(tuple, Anum_sub_forward_origins, desc, &isnull);
	if (isnull)
		sub->forward_origins = NIL;
	else
	{
		List		   *forward_origin_names;
		forward_origin_names = textarray_to_list(DatumGetArrayTypeP(d));
		sub->forward_origins = forward_origin_names;
	}

	/* Get apply_delay. */
	d = heap_getattr(tuple, Anum_sub_apply_delay, desc, &isnull);
	if (isnull)
		sub->apply_delay = NULL;
	else
		sub->apply_delay = DatumGetIntervalP(d);

	/* Get force_text_transfer. */
	d = heap_getattr(tuple, Anum_sub_force_text_transfer, desc, &isnull);
	if (isnull)
		sub->force_text_transfer = false;
	else
		sub->force_text_transfer = DatumGetBool(d);

	return sub;
}

/*
 * Load the info for specific subscriber.
 */
PGLogicalSubscription *
get_subscription(Oid subid)
{
	PGLogicalSubscription    *sub;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	TupleDesc		desc;
	ScanKeyData		key[1];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_SUBSCRIPTION, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				Anum_sub_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(subid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "subscription %u not found", subid);

	desc = RelationGetDescr(rel);
	sub = subscription_fromtuple(tuple, desc);

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	return sub;
}

/*
 * Load the info for specific subscriber.
 */
PGLogicalSubscription *
get_subscription_by_name(const char *name, bool missing_ok)
{
	PGLogicalSubscription    *sub;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	TupleDesc		desc;
	ScanKeyData		key[1];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_SUBSCRIPTION, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				Anum_sub_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(name));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
	{
		if (missing_ok)
		{
			systable_endscan(scan);
			table_close(rel, RowExclusiveLock);
			return NULL;
		}

		elog(ERROR, "subscriber %s not found", name);
	}

	desc = RelationGetDescr(rel);
	sub = subscription_fromtuple(tuple, desc);

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	return sub;
}

/*
 * Return all target node subscriptions.
 */
List *
get_node_subscriptions(Oid nodeid, bool origin)
{
	PGLogicalSubscription    *sub;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	TupleDesc		desc;
	ScanKeyData		key[1];
	List		   *res = NIL;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_SUBSCRIPTION, -1);
	rel = table_openrv(rv, RowExclusiveLock);
	desc = RelationGetDescr(rel);

	ScanKeyInit(&key[0],
				origin ? Anum_sub_origin : Anum_sub_target,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(nodeid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		sub = subscription_fromtuple(tuple, desc);

		res = lappend(res, sub);
	}

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	return res;
}
