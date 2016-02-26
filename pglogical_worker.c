/*-------------------------------------------------------------------------
 *
 * pglogical.c
 * 		pglogical initialization and common functionality
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"

#include "access/xact.h"

#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/procarray.h"

#include "utils/guc.h"

#include "pglogical_sync.h"
#include "pglogical_worker.h"
#include "pglogical.h"


volatile sig_atomic_t	got_SIGTERM = false;

PGLogicalContext	   *PGLogicalCtx = NULL;
PGLogicalWorker		   *MyPGLogicalWorker = NULL;

static bool xacthook_signal_workers = false;


static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static void pglogical_worker_detach(bool crash);
static void wait_for_worker_startup(PGLogicalWorker *worker,
									BackgroundWorkerHandle *handle);
static void signal_worker_xact_callback(XactEvent event, void *arg);


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
 * Find unused worker slot.
 *
 * The caller is responsible for locking.
 */
static int
find_empty_worker_slot(void)
{
	int	i;

	Assert(LWLockHeldByMe(PGLogicalCtx->lock));

	for (i = 0; i < PGLogicalCtx->total_workers; i++)
	{
		if (PGLogicalCtx->workers[i].worker_type == PGLOGICAL_WORKER_NONE ||
			PGLogicalCtx->workers[i].crashed_at != 0)
			return i;
	}

	return -1;
}

/*
 * Register the pglogical worker proccess.
 *
 * Return the assigned slot number.
 */
int
pglogical_worker_register(PGLogicalWorker *worker)
{
	BackgroundWorker	bgw;
	BackgroundWorkerHandle *bgw_handle;
	int					slot;

	LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);

	slot = find_empty_worker_slot();
	if (slot == -1)
	{
		LWLockRelease(PGLogicalCtx->lock);
		elog(ERROR, "could not register pglogical worker: all background worker slots are already used");
	}

	memcpy(&PGLogicalCtx->workers[slot], worker, sizeof(PGLogicalWorker));
	PGLogicalCtx->workers[slot].crashed_at = 0;

	LWLockRelease(PGLogicalCtx->lock);

	bgw.bgw_flags =	BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_main = NULL;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN,
			 EXTENSION_NAME);
	if (worker->worker_type == PGLOGICAL_WORKER_MANAGER)
	{
		snprintf(bgw.bgw_function_name, BGW_MAXLEN,
				 "pglogical_manager_main");
		snprintf(bgw.bgw_name, BGW_MAXLEN,
				 "pglogical manager %u", worker->dboid);
	}
	else if (worker->worker_type == PGLOGICAL_WORKER_SYNC)
	{
		snprintf(bgw.bgw_function_name, BGW_MAXLEN,
				 "pglogical_sync_main");
		snprintf(bgw.bgw_name, BGW_MAXLEN,
				 "pglogical sync %s %u:%u",
				 NameStr(worker->worker.sync.relname),
				 worker->dboid, worker->worker.sync.apply.subid);
	}
	else
	{
		snprintf(bgw.bgw_function_name, BGW_MAXLEN,
				 "pglogical_apply_main");
		snprintf(bgw.bgw_name, BGW_MAXLEN,
				 "pglogical apply %u:%u", worker->dboid,
				 worker->worker.apply.subid);
	}

	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	bgw.bgw_notify_pid = MyProcPid;
	bgw.bgw_main_arg = ObjectIdGetDatum(slot);

	if (!RegisterDynamicBackgroundWorker(&bgw, &bgw_handle))
	{
		PGLogicalCtx->workers[slot].crashed_at = GetCurrentTimestamp();
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("worker registration failed, you might want to increase max_worker_processes setting")));
	}

	wait_for_worker_startup(&PGLogicalCtx->workers[slot], bgw_handle);

	return slot;
}

/*
 * This is our own version of WaitForBackgroundWorkerStartup where we wait
 * until worker actually attaches to our shmem.
 */
static void
wait_for_worker_startup(PGLogicalWorker *worker,
						BackgroundWorkerHandle *handle)
{
	BgwHandleStatus status;
	int			rc;

	for (;;)
	{
		pid_t		pid;

		CHECK_FOR_INTERRUPTS();

		status = GetBackgroundWorkerPid(handle, &pid);
		if (status == BGWH_STARTED && pglogical_worker_running(worker))
		{
			break;
		}
		if (status == BGWH_STOPPED)
		{
			worker->crashed_at = GetCurrentTimestamp();
			break;
		}

		Assert(status == BGWH_NOT_YET_STARTED || status == BGWH_STARTED);

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 1000L);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		ResetLatch(MyLatch);
	}
}

/*
 * Cleanup function.
 *
 * Called on process exit.
 */
static void
pglogical_worker_on_exit(int code, Datum arg)
{
	pglogical_worker_detach(code != 0);
}

/*
 * Attach the current master process to the PGLogicalCtx.
 *
 * Called during by master worker startup.
 */
void
pglogical_worker_attach(int slot)
{
	Assert(slot < PGLogicalCtx->total_workers);

#if PG_VERSION_NUM < 90600
	set_latch_on_sigusr1 = true;
#endif

	LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);

	before_shmem_exit(pglogical_worker_on_exit, (Datum) 0);

	MyPGLogicalWorker = &PGLogicalCtx->workers[slot];
	MyPGLogicalWorker->proc = MyProc;

	LWLockRelease(PGLogicalCtx->lock);

	/* Make it easy to identify our processes. */
	SetConfigOption("application_name", MyBgworkerEntry->bgw_name,
					PGC_USERSET, PGC_S_SESSION);
}

/*
 * Detach the current master process from the PGLogicalCtx.
 *
 * Called during master worker exit.
 */
static void
pglogical_worker_detach(bool crash)
{
	/* Nothing to detach. */
	if (MyPGLogicalWorker == NULL)
		return;

	LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);

	Assert(MyPGLogicalWorker->proc = MyProc);
	MyPGLogicalWorker->proc = NULL;

	/*
	 * If we crashed we need to report it.
	 *
	 * The crash logic only works because all of the workers are attached
	 * to shmem and the serious crashes that we can't catch here cause
	 * postmaster to restart whole server killing all our workers and cleaning
	 * shmem so we start from clean state in that scenario.
	 */
	if (crash)
	{
		MyPGLogicalWorker->crashed_at = GetCurrentTimestamp();

		/* Signal the supervisor process. */
		if (PGLogicalCtx->supervisor)
			SetLatch(&PGLogicalCtx->supervisor->procLatch);
	}
	else
	{
		/* Worker has finished work, clean up its state from shmem. */
		MyPGLogicalWorker->worker_type = PGLOGICAL_WORKER_NONE;
		MyPGLogicalWorker->dboid = InvalidOid;
	}

	MyPGLogicalWorker = NULL;

	LWLockRelease(PGLogicalCtx->lock);
}

/*
 * Find the manager worker for given database.
 */
PGLogicalWorker *
pglogical_manager_find(Oid dboid)
{
	int i;

	Assert(LWLockHeldByMe(PGLogicalCtx->lock));

	for (i = 0; i < PGLogicalCtx->total_workers; i++)
	{
		if (PGLogicalCtx->workers[i].worker_type == PGLOGICAL_WORKER_MANAGER &&
			dboid == PGLogicalCtx->workers[i].dboid)
			return &PGLogicalCtx->workers[i];
	}

	return NULL;
}

/*
 * Find the apply worker for given subscription.
 */
PGLogicalWorker *
pglogical_apply_find(Oid dboid, Oid subscriberid)
{
	int i;

	Assert(LWLockHeldByMe(PGLogicalCtx->lock));

	for (i = 0; i < PGLogicalCtx->total_workers; i++)
	{
		if (PGLogicalCtx->workers[i].worker_type == PGLOGICAL_WORKER_APPLY &&
			dboid == PGLogicalCtx->workers[i].dboid &&
			subscriberid == PGLogicalCtx->workers[i].worker.apply.subid)
			return &PGLogicalCtx->workers[i];
	}

	return NULL;
}

/*
 * Find all apply worker for given database.
 */
List *
pglogical_apply_find_all(Oid dboid)
{
	int			i;
	List	   *res = NIL;

	Assert(LWLockHeldByMe(PGLogicalCtx->lock));

	for (i = 0; i < PGLogicalCtx->total_workers; i++)
	{
		if (PGLogicalCtx->workers[i].worker_type == PGLOGICAL_WORKER_APPLY &&
			dboid == PGLogicalCtx->workers[i].dboid)
			res = lappend(res, &PGLogicalCtx->workers[i]);
	}

	return res;
}

/*
 * Find the sync worker for given subscription and table
 */
PGLogicalWorker *
pglogical_sync_find(Oid dboid, Oid subscriberid, char *nspname, char *relname)
{
	int i;

	Assert(LWLockHeldByMe(PGLogicalCtx->lock));

	for (i = 0; i < PGLogicalCtx->total_workers; i++)
	{
		PGLogicalWorker *w = &PGLogicalCtx->workers[i];
		if (w->worker_type == PGLOGICAL_WORKER_SYNC && dboid == w->dboid &&
			subscriberid == w->worker.apply.subid &&
			strcmp(NameStr(w->worker.sync.nspname), nspname) == 0 &&
			strcmp(NameStr(w->worker.sync.relname), relname) == 0)
			return w;
	}

	return NULL;
}


/*
 * Find the sync worker for given subscription
 */
List *
pglogical_sync_find_all(Oid dboid, Oid subscriberid)
{
	int			i;
	List	   *res = NIL;

	Assert(LWLockHeldByMe(PGLogicalCtx->lock));

	for (i = 0; i < PGLogicalCtx->total_workers; i++)
	{
		PGLogicalWorker *w = &PGLogicalCtx->workers[i];
		if (w->worker_type == PGLOGICAL_WORKER_SYNC && dboid == w->dboid &&
			subscriberid == w->worker.apply.subid)
			res = lappend(res, w);
	}

	return res;
}

/*
 * Get worker based on slot
 */
PGLogicalWorker *
pglogical_get_worker(int slot)
{
	Assert(LWLockHeldByMe(PGLogicalCtx->lock));
	return &PGLogicalCtx->workers[slot];
}

/*
 * Is the worker running?
 */
bool
pglogical_worker_running(PGLogicalWorker *worker)
{
	return worker && worker->proc;
}

void
pglogical_worker_kill(PGLogicalWorker *worker)
{
	Assert(LWLockHeldByMe(PGLogicalCtx->lock));
	if (pglogical_worker_running(worker))
		kill(worker->proc->pid, SIGTERM);
}

static void
signal_worker_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
			if (xacthook_signal_workers)
			{
				PGLogicalWorker	   *w;

				xacthook_signal_workers = false;

				LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);

				PGLogicalCtx->connections_changed = true;

				w = pglogical_manager_find(MyDatabaseId);

				if (pglogical_worker_running(w))
				{
					/* Signal the manager worker. */
					SetLatch(&w->proc->procLatch);
				}
				else if (PGLogicalCtx->supervisor)
				{
					/* Signal the supervisor process. */
					SetLatch(&PGLogicalCtx->supervisor->procLatch);
				}

				LWLockRelease(PGLogicalCtx->lock);
			}
			break;
		default:
			/* We're not interested in other tx events */
			break;
	}
}

/*
 * Enqueue singal for supervisor/manager at COMMIT.
 */
void
pglogical_connections_changed(void)
{
	if (!xacthook_signal_workers)
	{
		RegisterXactCallback(signal_worker_xact_callback, NULL);
		xacthook_signal_workers = true;
	}
}

static size_t
worker_shmem_size(void)
{
	return offsetof(PGLogicalContext, workers) +
		sizeof(PGLogicalWorker) * max_worker_processes;
}

/*
 * Init shmem needed for workers.
 */
static void
pglogical_worker_shmem_startup(void)
{
	bool        found;

	if (prev_shmem_startup_hook != NULL)
		prev_shmem_startup_hook();

	/* Init signaling context for supervisor proccess. */
	PGLogicalCtx = ShmemInitStruct("pglogical_context", worker_shmem_size(),
								   &found);

	if (!found)
	{
		PGLogicalCtx->lock = &(GetNamedLWLockTranche("pglogical"))->lock;
		PGLogicalCtx->supervisor = NULL;
		PGLogicalCtx->total_workers = max_worker_processes;
		memset(PGLogicalCtx->workers, 0,
			   sizeof(PGLogicalWorker) * PGLogicalCtx->total_workers);
	}
}

/*
 * Request shmem resources for our worker management.
 */
void
pglogical_worker_shmem_init(void)
{
	Assert(process_shared_preload_libraries_in_progress);

	/* Allocate enough shmem for the worker limit ... */
	RequestAddinShmemSpace(worker_shmem_size());

	/*
	 * We'll need to be able to take exclusive locks so only one per-db backend
	 * tries to allocate or free blocks from this array at once.  There won't
	 * be enough contention to make anything fancier worth doing.
	 */
	RequestNamedLWLockTranche("pglogical", 1);

	/*
	 * Whether this is a first startup or crash recovery, we'll be re-initing
	 * the bgworkers.
	 */
	PGLogicalCtx = NULL;
	MyPGLogicalWorker = NULL;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pglogical_worker_shmem_startup;
}
