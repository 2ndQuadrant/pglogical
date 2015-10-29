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
#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"

#include "catalog/indexing.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_type.h"

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

#define CATALOG_PROVIDER	"provider"
#define CATALOG_SUBSCRIBER	"subscriber"

typedef struct SubscriberTuple
{
	Oid			subscriber_id;
	NameData	subscriber_name;
	char		subscriber_status;
	NameData	subscriber_provider_name;
	text		subscriber_provider_dsn;
	text		subscriber_local_dsn;
} SubscriberTuple;

#define Natts_subscriber					7
#define Anum_subscriber_id					1
#define Anum_subscriber_name				2
#define Anum_subscriber_status				3
#define Anum_subscriber_provider_name		4
#define Anum_subscriber_provider_dsn		5
#define Anum_subscriber_local_dsn			6
#define Anum_subscriber_replication_sets	7

typedef struct ProviderTuple
{
	Oid			provider_id;
	NameData	provider_name;
} ProviderTuple;

#define	Natts_provider		2
#define Anum_provider_id	1
#define Anum_provider_name	2

/*
 * Add new tuple to the provider catalog.
 */
void
create_provider(PGLogicalProvider *provider)
{
	RangeVar   *rv;
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_provider];
	bool		nulls[Natts_provider];
	NameData	provider_name;

	if (get_provider_by_name(provider->name, true) != NULL)
		elog(ERROR, "provider %s already exists", provider->name);

	/* Generate new id unless one was already specified. */
	if (provider->id == InvalidOid)
		provider->id =
			DatumGetUInt32(hash_any((const unsigned char *) provider->name,
									strlen(provider->name)));

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_PROVIDER, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* Form a tuple. */
	memset(nulls, false, sizeof(nulls));

	values[Anum_provider_id - 1] = ObjectIdGetDatum(provider->id);
	namestrcpy(&provider_name, provider->name);
	values[Anum_provider_name - 1] = NameGetDatum(&provider_name);

	tup = heap_form_tuple(tupDesc, values, nulls);

	/* Insert the tuple to the catalog. */
	simple_heap_insert(rel, tup);

	/* Update the indexes. */
	//CatalogUpdateIndexes(rel, tup);

	/* Cleanup. */
	heap_freetuple(tup);
	heap_close(rel, RowExclusiveLock);

	pglogical_connections_changed();
}

/*
 * Delete the tuple from provider catalog.
 */
void
drop_provider(Oid providerid)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_PROVIDER, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				Anum_provider_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(providerid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "provider %u not found", providerid);

	/* Remove the tuple. */
	simple_heap_delete(rel, &tuple->t_self);

	/* Cleanup. */
	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	pglogical_connections_changed();
}

static PGLogicalProvider *
provider_fromtuple(HeapTuple tuple)
{
	ProviderTuple *providertup = (ProviderTuple *) GETSTRUCT(tuple);
	PGLogicalProvider *provider
		= (PGLogicalProvider *) palloc(sizeof(PGLogicalProvider));
	provider->id = providertup->provider_id;
	provider->name = pstrdup(NameStr(providertup->provider_name));
	return provider;
}

/*
 * Load the info for specific provider.
 */
PGLogicalProvider *
get_provider(Oid providerid)
{
	PGLogicalProvider  *provider;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_PROVIDER, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				Anum_provider_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(providerid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "provider %u not found", providerid);

	provider = provider_fromtuple(tuple);

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return provider;
}

/*
 * Load the info for specific provider.
 */
PGLogicalProvider *
get_provider_by_name(const char *name, bool missing_ok)
{
	PGLogicalProvider  *provider;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_PROVIDER, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				Anum_provider_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(name));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
	{
		if (missing_ok)
		{
			systable_endscan(scan);
			heap_close(rel, RowExclusiveLock);
			return NULL;
		}

		elog(ERROR, "provider %s not found", name);
	}

	provider = provider_fromtuple(tuple);

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return provider;
}


/*
 * Add new tuple to the subsriber catalog.
 */
void
create_subscriber(PGLogicalSubscriber *subscriber)
{
	RangeVar   *rv;
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_subscriber];
	bool		nulls[Natts_subscriber];
	NameData	subscriber_name;
	NameData	subscriber_provider_name;

	if (get_subscriber_by_name(subscriber->name, true) != NULL)
		elog(ERROR, "subscriber %s already exists", subscriber->name);

	/* Generate new id unless one was already specified. */
	if (subscriber->id == InvalidOid)
		subscriber->id =
			DatumGetUInt32(hash_any((const unsigned char *) subscriber->name,
									strlen(subscriber->name)));

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_SUBSCRIBER, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* Form a tuple. */
	memset(nulls, false, sizeof(nulls));

	values[Anum_subscriber_id - 1] = ObjectIdGetDatum(subscriber->id);
	namestrcpy(&subscriber_name, subscriber->name);
	values[Anum_subscriber_name - 1] = NameGetDatum(&subscriber_name);
	values[Anum_subscriber_status - 1] = CharGetDatum(subscriber->status);
	namestrcpy(&subscriber_provider_name, subscriber->provider_name);
	values[Anum_subscriber_provider_name - 1] =
		NameGetDatum(&subscriber_provider_name);
	values[Anum_subscriber_provider_dsn - 1] =
		CStringGetTextDatum(subscriber->provider_dsn);
	values[Anum_subscriber_local_dsn - 1] =
		CStringGetTextDatum(subscriber->local_dsn);

	if (list_length(subscriber->replication_sets) > 0)
		values[Anum_subscriber_replication_sets - 1] =
			PointerGetDatum(strlist_to_textarray(subscriber->replication_sets));
	else
		nulls[Anum_subscriber_replication_sets - 1] = true;

	tup = heap_form_tuple(tupDesc, values, nulls);

	/* Insert the tuple to the catalog. */
	simple_heap_insert(rel, tup);

	/* Update the indexes. */
	CatalogUpdateIndexes(rel, tup);

	/* Cleanup. */
	heap_freetuple(tup);
	heap_close(rel, RowExclusiveLock);

	pglogical_connections_changed();
}

/*
 * Delete the tuple from subsriber catalog.
 */
void
drop_subscriber(Oid subscriberid)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_SUBSCRIBER, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				Anum_subscriber_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(subscriberid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "subscriber %u not found", subscriberid);

	/* Remove the tuple. */
	simple_heap_delete(rel, &tuple->t_self);

	/* Cleanup. */
	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	pglogical_connections_changed();
}


static PGLogicalSubscriber*
subscriber_fromtuple(HeapTuple tuple, TupleDesc desc)
{
	SubscriberTuple *subtup = (SubscriberTuple *) GETSTRUCT(tuple);
	Datum d;
	bool isnull;

	PGLogicalSubscriber *sub =
		(PGLogicalSubscriber *) palloc(sizeof(PGLogicalSubscriber));
	sub->id = subtup->subscriber_id;
	sub->name = pstrdup(NameStr(subtup->subscriber_name));
	sub->status = subtup->subscriber_status;
	sub->provider_name = pstrdup(NameStr(subtup->subscriber_provider_name));

	d = heap_getattr(tuple, Anum_subscriber_provider_dsn, desc, &isnull);
	Assert(!isnull);
	sub->provider_dsn = pstrdup(TextDatumGetCString(d));

	d = heap_getattr(tuple, Anum_subscriber_local_dsn, desc, &isnull);
	Assert(!isnull);
	sub->local_dsn = pstrdup(TextDatumGetCString(d));

	/* Get replication sets. */
	d = heap_getattr(tuple, Anum_subscriber_replication_sets, desc, &isnull);
	if (isnull)
		sub->replication_sets = NIL;
	else
	{
		List		   *repset_names;
		repset_names = textarray_to_list(DatumGetArrayTypeP(d));
		sub->replication_sets = get_replication_sets(repset_names, true);
	}

	return sub;
}

/*
 * Load the info for specific subscriber.
 */
PGLogicalSubscriber *
get_subscriber(Oid subscriberid)
{
	PGLogicalSubscriber    *sub;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	TupleDesc		desc;
	ScanKeyData		key[1];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_SUBSCRIBER, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				Anum_subscriber_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(subscriberid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "subscriber %u not found", subscriberid);

	desc = RelationGetDescr(rel);
	sub = subscriber_fromtuple(tuple, desc);

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return sub;
}

/*
 * Load the info for specific subscriber.
 */
PGLogicalSubscriber *
get_subscriber_by_name(const char *name, bool missing_ok)
{
	PGLogicalSubscriber    *sub;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	TupleDesc		desc;
	ScanKeyData		key[1];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_SUBSCRIBER, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	/* Search for node record. */
	ScanKeyInit(&key[0],
				Anum_subscriber_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(name));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
	{
		if (missing_ok)
		{
			systable_endscan(scan);
			heap_close(rel, RowExclusiveLock);
			return NULL;
		}

		elog(ERROR, "subscriber %s not found", name);
	}

	desc = RelationGetDescr(rel);
	sub = subscriber_fromtuple(tuple, desc);

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return sub;
}

/*
 * Return all local subscribers.
 */
List *
get_subscribers(void)
{
	PGLogicalSubscriber    *sub;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	TupleDesc		desc;
	List		   *res = NIL;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_SUBSCRIBER, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	desc = RelationGetDescr(rel);

	scan = systable_beginscan(rel, 0, true, NULL, 0, NULL);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		sub = subscriber_fromtuple(tuple, desc);

		res = lappend(res, sub);
	}

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return res;
}


/*
 * Update the status of a subscriber.
 */
void
set_subscriber_status(int subscriberid, char status)
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

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_SUBSCRIBER, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	/* Find the node tuple */
	ScanKeyInit(&key[0],
				Anum_subscriber_id,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(subscriberid));

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

		values[Anum_subscriber_status - 1] = CharGetDatum(status);

		newtuple = heap_form_tuple(RelationGetDescr(rel),
								   values, nulls);
		simple_heap_update(rel, &tuple->t_self, newtuple);
		CatalogUpdateIndexes(rel, newtuple);
	}
	else
		elog(ERROR, "subscriber %d not found.", subscriberid);

	systable_endscan(scan);

	/* Make the change visible to our tx. */
	CommandCounterIncrement();

	/* Release the lock */
	heap_close(rel, RowExclusiveLock);

	if (tx_started)
		CommitTransactionCommand();
}
