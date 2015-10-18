/*-------------------------------------------------------------------------
 *
 * pglogical_manager.c
 * 		pglogical worker for managing apply workers in a database
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_manager.c
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
#include "storage/procsignal.h"
#include "storage/shm_toc.h"
#include "storage/spin.h"

#include "utils/memutils.h"
#include "utils/resowner.h"

#include "pglogical_proto.h"
#include "pglogical_relcache.h"
#include "pglogical_node.h"
#include "pglogical_init_replica.h"
#include "pglogical.h"

static PGLogicalApplyWorker	   *apply_workers = NULL;
static PGLogicalDBState		   *manager_state = NULL;

void pglogical_manager_main(Datum main_arg);

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

		/* Already assigned handle? Check if still running. */
		if (apply_workers[i].bgwhandle != NULL)
		{
			pid_t	pid;
			BgwHandleStatus status;

			status = GetBackgroundWorkerPid(apply_workers[i].bgwhandle, &pid);
			if (status == BGWH_STOPPED || status == BGWH_POSTMASTER_DIED)
			{
				pfree(apply_workers[i].bgwhandle);
				apply_workers[i].bgwhandle = NULL;
				SpinLockAcquire(&manager_state->mutex);
				manager_state->apply_attached--;
				SpinLockRelease(&manager_state->mutex);
			}
			else
				continue;
		}

		bgw.bgw_flags =	BGWORKER_SHMEM_ACCESS |
			BGWORKER_BACKEND_DATABASE_CONNECTION;
		bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
		bgw.bgw_main = NULL;
		snprintf(bgw.bgw_library_name, BGW_MAXLEN,
				 EXTENSION_NAME);
		snprintf(bgw.bgw_function_name, BGW_MAXLEN,
				 "pglogical_apply_main");
		bgw.bgw_restart_time = BGW_NEVER_RESTART;
		bgw.bgw_notify_pid = MyProcPid;
		snprintf(bgw.bgw_name, BGW_MAXLEN,
				 "pglogical apply");
		bgw.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg));

		if (!RegisterDynamicBackgroundWorker(&bgw, &apply_workers[i].bgwhandle))
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

	shm_toc_initialize_estimator(&estimator);
	shm_toc_estimate_chunk(&estimator, sizeof(PGLogicalDBState));
	shm_toc_estimate_chunk(&estimator,
						   list_length(conns) * sizeof(PGLogicalApplyWorker));
	shm_toc_estimate_keys(&estimator, 2);

	segsize = shm_toc_estimate(&estimator);

	seg = dsm_create(segsize, 0);
	toc = shm_toc_create(PGLOGICAL_MASTER_TOC_MAGIC, dsm_segment_address(seg),
						 segsize);

	manager_state = shm_toc_allocate(toc, sizeof(PGLogicalDBState));
	SpinLockInit(&manager_state->mutex);
	manager_state->apply_total = list_length(conns);
	manager_state->apply_attached = 0;
	shm_toc_insert(toc, PGLOGICAL_MASTER_TOC_STATE, manager_state);

	apply_workers = shm_toc_allocate(toc,
							list_length(conns) * sizeof(PGLogicalApplyWorker));
	shm_toc_insert(toc, PGLOGICAL_MASTER_TOC_APPLY, apply_workers);

	i = 0;
	foreach (lc, conns)
	{
		PGLogicalConnection *c = (PGLogicalConnection *) lfirst(lc);

		apply_workers[i].dboid = MyDatabaseId;
		apply_workers[i].bgwhandle = NULL;
		apply_workers[i++].connid = c->id;
	}

	return seg;
}

/*
 * Cleanup function.
 *
 * Called on process exit.
 */
static void
pglogical_manager_on_exit(int code, Datum arg)
{
	pglogical_manager_detach(code != 0);
}

/*
 * Entry point for manager worker.
 */
void
pglogical_manager_main(Datum main_arg)
{
	Oid			dboid = DatumGetObjectId(main_arg);
	Oid			extoid;
	List	   *conns;
	PGLogicalNode  *node;
	dsm_segment	   *seg;
	MemoryContext	saved_ctx;

	/* Setup shmem. */
	before_shmem_exit(pglogical_manager_on_exit, main_arg);
	pglogical_manager_attach(dboid);

	/* Establish signal handlers. */
	pqsignal(SIGTERM, handle_sigterm);
	BackgroundWorkerUnblockSignals();

	/* Connect to db. */
	BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid);

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

	/* No connections (this should probably be error). */
	if (list_length(conns) == 0)
		proc_exit(0);

	MemoryContextSwitchTo(saved_ctx);
	CommitTransactionCommand();

	/* If this is newly created node, initialize it. */
	if (node->status != NODE_STATUS_READY)
	{
		PGLogicalConnection *c = linitial(conns);
		pglogical_init_replica(c);
	}

	/* Setup shared state between the manager and apply processes. */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "pglogical apply");
	seg = setup_dynamic_shared_memory(conns);

	/* Main wait loop. */
	while (!got_SIGTERM)
    {
		int rc;

		/* Launch the apply workers. */
		register_apply_workers(seg, list_length(conns));

		rc = WaitLatch(&MyProc->procLatch,
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
