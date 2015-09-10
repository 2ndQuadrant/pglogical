/*-------------------------------------------------------------------------
 *
 * pg_logical_manager.c
 * 		pg_logical worker for managing apply workers in a database
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pg_logical_manager.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq-fe.h"

#include "miscadmin.h"

#include "access/heapam.h"
#include "access/htup_details.h"

#include "catalog/pg_database.h"

#include "commands/extension.h"

#include "postmaster/bgworker.h"

#include "storage/ipc.h"

#include "pg_logical_proto.h"
#include "pg_logical_relcache.h"
#include "pg_logical_node.h"

#define EXTENSION_NAME "pg_logical"


/*
 * Start apply workers for each of the publisher node.
 */
static void
register_apply_workers(PGLogicalNode *local_node)
{
	List	   *conns;
	ListCell   *lc;

	conns = get_node_publishers(local_node->id);

	foreach (lc, conns)
	{
		PGLogicalConnection *conn = (PGLogicalConnection *) lfirst(lc);
		BackgroundWorker	bgw;

		bgw.bgw_flags =	BGWORKER_BACKEND_DATABASE_CONNECTION;
		bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
		bgw.bgw_main = NULL;
		snprintf(bgw.bgw_library_name, BGW_MAXLEN,
				 "pg_logical");
		snprintf(bgw.bgw_function_name, BGW_MAXLEN,
				 "pg_logical_apply_main");
		bgw.bgw_restart_time = 1;
		bgw.bgw_notify_pid = 0;
		snprintf(bgw.bgw_name, BGW_MAXLEN,
				 "bdr supervisor");
		bgw.bgw_main_arg = UInt32GetDatum(conn->id);

		RegisterBackgroundWorker(&bgw);
	}
}

/*
 * Entry point for manager worker.
 */
void
pg_logical_manager_main(Datum main_arg)
{
	Oid		dbid = DatumGetObjectId(main_arg);
	Oid		extoid;
	PGLogicalNode *node;

	BackgroundWorkerInitializeConnectionByOid(dbid, InvalidOid);

	/* If the extension is not installed in this DB, exit. */
	extoid = get_extension_oid(EXTENSION_NAME, true);
	if (!OidIsValid(extoid))
		proc_exit(0);

	/* Fetch local node info. */
	node = get_local_node();

	/* No local node. */
	if (!node)
		proc_exit(0);

	register_apply_workers(node);
}

/*
 * Register the manager bgworker for the given DB. The manager worker will then
 * start the apply workers.
 *
 * Called in postmaster context from _PG_init, and under backend from node join
 * funcions.
 */
static void
pg_logical_manager_register(Oid dboid)
{
	BackgroundWorker bgw;

	bgw.bgw_flags =	BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_main = NULL;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN,
			 "pg_logical");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN,
			 "pg_logical_manager_main");
	bgw.bgw_restart_time = 1;
	bgw.bgw_notify_pid = 0;
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "bdr supervisor");
	bgw.bgw_main_arg = ObjectIdGetDatum(dboid);

	RegisterBackgroundWorker(&bgw);
}

/*
 * Entry point for this module.
 */
void
_PG_init(void)
{
	Relation	rel;
	HeapScanDesc scan;
	HeapTuple	tup;

	if (IsBinaryUpgrade)
		return;

	/* Run manager worker for every connectable database. */
	rel = heap_open(DatabaseRelationId, AccessShareLock);
	scan = heap_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database pgdatabase = (Form_pg_database) GETSTRUCT(tup);

		if (pgdatabase->datallowconn)
			pg_logical_manager_register(HeapTupleGetOid(tup));
	}

	heap_endscan(scan);
	heap_close(rel, AccessShareLock);
}
