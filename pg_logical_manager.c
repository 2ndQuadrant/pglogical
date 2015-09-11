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
#include "access/xact.h"

#include "catalog/pg_database.h"

#include "commands/extension.h"

#include "postmaster/bgworker.h"

#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/shm_toc.h"
#include "storage/spin.h"

#include "utils/memutils.h"
#include "utils/resowner.h"

#include "pglogical.h"
#include "pg_logical_proto.h"
#include "pg_logical_relcache.h"
#include "pg_logical_node.h"

PG_MODULE_MAGIC;

static volatile sig_atomic_t got_SIGTERM = false;

void
handle_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGTERM = true;

	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}


/*
 * Start apply workers for each of the publisher node.
 */
static void
register_apply_workers(dsm_segment *seg, int cnt)
{
	int		i;

	for (i = 0; i < cnt; i++)
	{
		BackgroundWorker	bgw;
		BackgroundWorkerHandle *bgw_handle;

		bgw.bgw_flags =	BGWORKER_SHMEM_ACCESS |
			BGWORKER_BACKEND_DATABASE_CONNECTION;
		bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
		bgw.bgw_main = NULL;
		snprintf(bgw.bgw_library_name, BGW_MAXLEN,
				 EXTENSION_NAME);
		snprintf(bgw.bgw_function_name, BGW_MAXLEN,
				 "pg_logical_apply_main");
		bgw.bgw_restart_time = 1;
		bgw.bgw_notify_pid = 0;
		snprintf(bgw.bgw_name, BGW_MAXLEN,
				 "pglogical apply");
		bgw.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg));

		elog(WARNING, "starting apply %u", i);
		if (!RegisterDynamicBackgroundWorker(&bgw, &bgw_handle))
		{
			ereport(ERROR,
					(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
					 errmsg("Registering worker failed, check prior log messages for details")));
		}
	}
}

static dsm_segment*
setup_dynamic_shared_memory(List *conns)
{
	shm_toc_estimator	estimator;
	dsm_segment	   *seg;
	shm_toc		   *toc;
	Size			segsize;
	ListCell	   *lc;
	int				i;
	PGLogicalDBState	   *state;
	PGLogicalApplyWorker   *apply;

	shm_toc_initialize_estimator(&estimator);
	shm_toc_estimate_chunk(&estimator, sizeof(PGLogicalDBState));
	shm_toc_estimate_chunk(&estimator,
						   conns->length * sizeof(PGLogicalApplyWorker));
	shm_toc_estimate_keys(&estimator, 2);

	segsize = shm_toc_estimate(&estimator);

	seg = dsm_create(segsize, 0);
	toc = shm_toc_create(PGLOGICAL_MASTER_TOC_MAGIC, dsm_segment_address(seg),
						 segsize);

	state = shm_toc_allocate(toc, sizeof(PGLogicalDBState));
	SpinLockInit(&state->mutex);
	state->apply_total = conns->length;
	state->apply_attached = 0;
	shm_toc_insert(toc, PGLOGICAL_MASTER_TOC_STATE, state);

	apply = shm_toc_allocate(toc,
							 conns->length * sizeof(PGLogicalApplyWorker));
	shm_toc_insert(toc, PGLOGICAL_MASTER_TOC_APPLY, apply);

	i = 0;
	foreach (lc, conns)
	{
		PGLogicalConnection *c = (PGLogicalConnection *) lfirst(lc);

		apply[i++].connid = c->id;
	}

	return seg;
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
	BackgroundWorkerHandle *bgw_handle;

	bgw.bgw_flags =	BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_main = NULL;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN,
			 EXTENSION_NAME);
	snprintf(bgw.bgw_function_name, BGW_MAXLEN,
			 "pg_logical_manager_main");
	bgw.bgw_restart_time = 0;
	bgw.bgw_notify_pid = 0;
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "pglogical manager");
	bgw.bgw_main_arg = ObjectIdGetDatum(dboid);

	if (!RegisterDynamicBackgroundWorker(&bgw, &bgw_handle))
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("Registering worker failed, check prior log messages for details")));
	}
}

/*
 * Entry point for manager worker.
 */
void
pg_logical_manager_main(Datum main_arg)
{
	Oid			dbid = DatumGetObjectId(main_arg);
	Oid			extoid;
	List	   *conns;
	PGLogicalNode  *node;
	dsm_segment	   *seg;
	MemoryContext	saved_ctx;

	/* Establish signal handlers. */
	pqsignal(SIGTERM, handle_sigterm);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnectionByOid(dbid, InvalidOid);

	StartTransactionCommand();

	/* If the extension is not installed in this DB, exit. */
	extoid = get_extension_oid(EXTENSION_NAME, true);
	if (!OidIsValid(extoid))
		proc_exit(0);

	/* Fetch local node info. */
	saved_ctx = MemoryContextSwitchTo(TopMemoryContext);
	node = get_local_node();

	/* No local node, exit. */
	if (!node)
		proc_exit(0);

	conns = get_node_publishers(node->id);
	MemoryContextSwitchTo(saved_ctx);

	CommitTransactionCommand();

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "pglogical apply");

	seg = setup_dynamic_shared_memory(conns);
	register_apply_workers(seg, conns->length);

	while (!got_SIGTERM)
    {
       int rc = WaitLatch(&MyProc->procLatch,
						  WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
						  180000L);

        ResetLatch(&MyProc->procLatch);

        /* emergency bailout if postmaster has died */
        if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}

	/* Clean up. */
	dsm_detach(seg);

	proc_exit(0);
}

/*
 * Static bgworker used for initialization.
 */
void
pg_logical_manager_init(Datum main_arg)
{
	Relation	rel;
	HeapScanDesc scan;
	HeapTuple	tup;

	BackgroundWorkerInitializeConnection(NULL, NULL);

	StartTransactionCommand();

	/* Run manager worker for every connectable database. */
	rel = heap_open(DatabaseRelationId, AccessShareLock);
	scan = heap_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database pgdatabase = (Form_pg_database) GETSTRUCT(tup);

		if (pgdatabase->datallowconn)
		{
			elog(DEBUG1, "registering pglogical manager process for database %s",
				 NameStr(pgdatabase->datname));
			pg_logical_manager_register(HeapTupleGetOid(tup));
		}
	}

	heap_endscan(scan);
	heap_close(rel, AccessShareLock);

	CommitTransactionCommand();

	proc_exit(0);
}

/*
 * Entry point for this module.
 */
void
_PG_init(void)
{
	BackgroundWorker bgw;

	if (IsBinaryUpgrade)
		return;

	bgw.bgw_flags =	BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_main = NULL;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN,
			 EXTENSION_NAME);
	snprintf(bgw.bgw_function_name, BGW_MAXLEN,
			 "pg_logical_manager_init");
	bgw.bgw_restart_time = 1;
	bgw.bgw_notify_pid = 0;
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "pglogical init");
	bgw.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&bgw);
}
