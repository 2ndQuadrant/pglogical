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

#include "access/xact.h"

#include "commands/extension.h"

#include "storage/ipc.h"
#include "storage/proc.h"

#include "utils/memutils.h"
#include "utils/resowner.h"

#include "pglogical_node.h"
#include "pglogical_worker.h"
#include "pglogical.h"

void pglogical_manager_main(Datum main_arg);

/*
 * Manage the apply workers - start new ones, kill old ones.
 */
static void
manage_apply_workers(void)
{
	List	   *subscribers;
	List	   *workers;
	ListCell   *slc,
			   *wlc;

	/* Get list of existing workers. */
	LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);
	workers = pglogical_apply_find_all(MyPGLogicalWorker->dboid);
	LWLockRelease(PGLogicalCtx->lock);

	/* Get list of subscribers. */
	StartTransactionCommand();
	subscribers = get_subscribers();

	/* Register apply worker for each subscriber. */
	foreach (slc, subscribers)
	{
		PGLogicalSubscriber	   *sub = (PGLogicalSubscriber *) lfirst(slc);
		PGLogicalWorker			apply;
		ListCell			   *next,
							   *prev;
		bool					found = false;

		/*
		 * Skip if subscriber not enabled.
		 * This must be called before the following search loop because
		 * we want to kill any workers for disabled subscribers.
		 */
		if (!sub->enabled)
			continue;

		/* Check if the subscriber already has registered worker. */
		prev = NULL;
		for (wlc = list_head(workers); wlc; wlc = next)
		{
			PGLogicalWorker *worker = (PGLogicalWorker *) lfirst(wlc);

			/* We might delete the cell so advance it now. */
			next = lnext(wlc);

			if (worker->worker.apply.subscriberid == sub->id)
			{
				workers = list_delete_cell(workers, wlc, prev);
				found = true;
				break;
			}
			else
				prev = wlc;
		}

		/* Skip if the worker was alrady registered. */
		if (found)
			continue;

		memset(&apply, 0, sizeof(PGLogicalWorker));
		apply.worker_type = PGLOGICAL_WORKER_APPLY;
		apply.dboid = MyPGLogicalWorker->dboid;
		apply.worker.apply.subscriberid = sub->id;

		pglogical_worker_register(&apply);
	}

	CommitTransactionCommand();

	/* Kill any remaining workers. */
	foreach (wlc, workers)
	{
		PGLogicalWorker *worker = (PGLogicalWorker *) lfirst(wlc);
		kill(worker->proc->pid, SIGTERM);
	}

	/* No subscribers, exit. */
	if (list_length(subscribers) == 0)
		proc_exit(0);
}

/*
 * Entry point for manager worker.
 */
void
pglogical_manager_main(Datum main_arg)
{
	int			slot = DatumGetInt32(main_arg);
	Oid			extoid;
	List	   *subscribers;
	MemoryContext	saved_ctx;

	/* Setup shmem. */
	pglogical_worker_attach(slot);

	/* Establish signal handlers. */
	pqsignal(SIGTERM, handle_sigterm);
	BackgroundWorkerUnblockSignals();

	/* Connect to db. */
	BackgroundWorkerInitializeConnectionByOid(MyPGLogicalWorker->dboid,
											  InvalidOid);

	StartTransactionCommand();

	/* If the extension is not installed in this DB, exit. */
	extoid = get_extension_oid(EXTENSION_NAME, true);
	if (!OidIsValid(extoid))
		proc_exit(0);

	saved_ctx = MemoryContextSwitchTo(TopMemoryContext);

	subscribers = get_subscribers();

	/* No subscribers, exit. */
	if (list_length(subscribers) == 0)
		proc_exit(0);

	MemoryContextSwitchTo(saved_ctx);
	CommitTransactionCommand();

	/* TODO: check that there is only one subscriber with node status != 'r' */

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "pglogical manager");

	/* Main wait loop. */
	while (!got_SIGTERM)
    {
		int rc;

		/* Launch the apply workers. */
		manage_apply_workers();

		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   180000L);

        ResetLatch(&MyProc->procLatch);

        /* emergency bailout if postmaster has died */
        if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}

	proc_exit(0);
}
