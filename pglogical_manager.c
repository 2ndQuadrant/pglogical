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
 *
 * TODO: check workers which need to be killed/disabled and kill/disable them.
 */
static void
manage_apply_workers(void)
{
	List	   *subscribers;
	ListCell   *lc;

	StartTransactionCommand();

	subscribers = get_subscribers();

	/* No subscribers, exit. */
	if (list_length(subscribers) == 0)
		proc_exit(0);

	/* Register apply worker for each subscriber. */
	foreach (lc, subscribers)
	{
		PGLogicalSubscriber	   *sub = (PGLogicalSubscriber *) lfirst(lc);
		PGLogicalWorker			worker;

		if (!sub->enabled)
			continue;

		/* Skip already registered workers. */
		LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);
		if (pglogical_apply_find(MyPGLogicalWorker->dboid, sub->id))
		{
			LWLockRelease(PGLogicalCtx->lock);
			continue;
		}
		LWLockRelease(PGLogicalCtx->lock);

		memset(&worker, 0, sizeof(PGLogicalWorker));
		worker.worker_type = PGLOGICAL_WORKER_APPLY;
		worker.dboid = MyPGLogicalWorker->dboid;
		worker.worker.apply.subscriberid = sub->id;

		pglogical_worker_register(&worker);
	}


	CommitTransactionCommand();
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
