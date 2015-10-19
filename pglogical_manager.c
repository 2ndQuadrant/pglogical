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
#include "pglogical_init_replica.h"
#include "pglogical.h"

void pglogical_manager_main(Datum main_arg);

/*
 * Manage the apply workers - start new ones, kill old ones.
 *
 * TODO: check workers which need to be killed and kill them.
 */
static void
manage_apply_workers(void)
{
	PGLogicalNode  *node;
	List	   *conns;
	ListCell   *lc;

	StartTransactionCommand();

	node = get_local_node(true);

	/* No local node, exit. */
	if (!node)
		proc_exit(0);

	conns = get_node_publishers(node->id);

	/* No connections (this should probably be error). */
	if (list_length(conns) == 0)
		proc_exit(0);

	/* Register apply worker for each connection. */
	foreach (lc, conns)
	{
		PGLogicalConnection	   *conn = (PGLogicalConnection *) lfirst(lc);
		PGLogicalWorker			worker;

		/* Skip already registered workers. */
		LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);
		if (pglogical_apply_find(MyPGLogicalWorker->dboid, conn->id))
		{
			LWLockRelease(PGLogicalCtx->lock);
			continue;
		}
		LWLockRelease(PGLogicalCtx->lock);

		memset(&worker, 0, sizeof(PGLogicalWorker));
		worker.worker_type = PGLOGICAL_WORKER_APPLY;
		worker.dboid = MyPGLogicalWorker->dboid;
		worker.worker.apply.connid = conn->id;

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
	List	   *conns;
	PGLogicalNode  *node;
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

	/* Fetch local node info. */
	node = get_local_node(true);

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
