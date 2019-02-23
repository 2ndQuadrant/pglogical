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

#include "libpq/libpq-be.h"

#include "access/xact.h"

#include "commands/dbcommands.h"

#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/procarray.h"

#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include "pgstat.h"

#include "pglogical_sync.h"
#include "pglogical_worker.h"


typedef struct signal_worker_item
{
	Oid		subid;
	bool	kill;
} signal_worker_item;
static	List *signal_workers = NIL;

volatile sig_atomic_t	got_SIGTERM = false;

PGLogicalContext	   *PGLogicalCtx = NULL;
PGLogicalWorker		   *MyPGLogicalWorker = NULL;
static uint16			MyPGLogicalWorkerGeneration;

static bool xacthook_signal_workers = false;
static bool xact_cb_installed = false;


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
find_empty_worker_slot(Oid dboid)
{
	int	i;

	Assert(LWLockHeldByMe(PGLogicalCtx->lock));

	for (i = 0; i < PGLogicalCtx->total_workers; i++)
	{
		if (PGLogicalCtx->workers[i].worker_type == PGLOGICAL_WORKER_NONE
		    || (PGLogicalCtx->workers[i].crashed_at != 0 
                && (PGLogicalCtx->workers[i].dboid == dboid
                    || PGLogicalCtx->workers[i].dboid == InvalidOid)))
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
	PGLogicalWorker		*worker_shm;
	BackgroundWorkerHandle *bgw_handle;
	int					slot;
	int					next_generation;

	Assert(worker->worker_type != PGLOGICAL_WORKER_NONE);

	LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);

	slot = find_empty_worker_slot(worker->dboid);
	if (slot == -1)
	{
		LWLockRelease(PGLogicalCtx->lock);
		elog(ERROR, "could not register pglogical worker: all background worker slots are already used");
	}

	worker_shm = &PGLogicalCtx->workers[slot];

	/*
	 * Maintain a generation counter for worker registrations; see
	 * wait_for_worker_startup(...). The counter wraps around.
	 */
	if (worker_shm->generation == PG_UINT16_MAX)
		next_generation = 0;
	else
		next_generation = worker_shm->generation + 1;

	memcpy(worker_shm, worker, sizeof(PGLogicalWorker));
	worker_shm->generation = next_generation;
	worker_shm->crashed_at = 0;
	worker_shm->proc = NULL;
	worker_shm->worker_type = worker->worker_type;

	LWLockRelease(PGLogicalCtx->lock);

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags =	BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
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
				 "pglogical sync %*s %u:%u", NAMEDATALEN - 37,
				 shorten_hash(NameStr(worker->worker.sync.relname), NAMEDATALEN - 37),
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
		worker_shm->crashed_at = GetCurrentTimestamp();
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("worker registration failed, you might want to increase max_worker_processes setting")));
	}

	wait_for_worker_startup(worker_shm, bgw_handle);

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
	uint16		generation = worker->generation;

	Assert(handle != NULL);

	for (;;)
	{
		pid_t		pid = 0;

		CHECK_FOR_INTERRUPTS();

		if (got_SIGTERM)
		{
			elog(DEBUG1, "pglogical supervisor exiting on SIGTERM");
			proc_exit(0);
		}

		status = GetBackgroundWorkerPid(handle, &pid);

		if (status == BGWH_STARTED && pglogical_worker_running(worker))
		{
			elog(DEBUG2, "%s worker at slot %zu started with pid %d and attached to shmem",
				 pglogical_worker_type_name(worker->worker_type), (worker - &PGLogicalCtx->workers[0]), pid);
			break;
		}
		if (status == BGWH_STOPPED)
		{
			/*
			 * The worker may have:
			 * - failed to launch after registration
			 * - launched then crashed/exited before attaching
			 * - launched, attached, done its work, detached cleanly and exited
			 *   before we got rescheduled
			 * - launched, attached, crashed and self-reported its crash, then
			 *   exited before we got rescheduled
			 *
			 * If it detached cleanly it will've set its worker type to
			 * PGLOGICAL_WORKER_NONE, which it can't have been at entry, so we
			 * know it must've started, attached and cleared it.
			 *
			 * However, someone else might've grabbed the slot and re-used it
			 * and not exited yet, so if the worker type is not NONE we can't
			 * tell if it's our worker that's crashed or another worker that
			 * might still be running. We use a generation counter incremented
			 * on registration to tell the difference. If the generation
			 * counter has increased we know the our worker must've exited
			 * cleanly (setting the worker type back to NONE) or self-reported
			 * a crash (setting crashed_at), then the slot re-used by another
			 * manager.
			 */
			if (worker->worker_type != PGLOGICAL_WORKER_NONE
				&& worker->generation == generation
				&& worker->crashed_at == 0)
			{
				/*
				 * The worker we launched (same generation) crashed before
				 * attaching to shmem so it didn't set crashed_at. Mark it
				 * crashed so the slot can be re-used.
				 */
				elog(DEBUG2, "%s worker at slot %zu exited prematurely",
					 pglogical_worker_type_name(worker->worker_type), (worker - &PGLogicalCtx->workers[0]));
				worker->crashed_at = GetCurrentTimestamp();
			}
			else
			{
				/*
				 * Worker exited normally or self-reported a crash and may have already been
				 * replaced. Either way, we don't care, we're only looking for crashes before
				 * shmem attach.
				 */
				elog(DEBUG2, "%s worker at slot %zu exited before we noticed it started",
					 pglogical_worker_type_name(worker->worker_type), (worker - &PGLogicalCtx->workers[0]));
			}
			break;
		}

		Assert(status == BGWH_NOT_YET_STARTED || status == BGWH_STARTED);

		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 1000L);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		ResetLatch(&MyProc->procLatch);
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
 * Called during worker startup to inform the master the worker
 * is ready and give it access to the worker's PGPROC.
 */
void
pglogical_worker_attach(int slot, PGLogicalWorkerType type)
{
	Assert(slot >= 0);
	Assert(slot < PGLogicalCtx->total_workers);

	MyProcPort = (Port *) calloc(1, sizeof(Port));

#if PG_VERSION_NUM < 90600
	set_latch_on_sigusr1 = true;
#endif

	LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);

	before_shmem_exit(pglogical_worker_on_exit, (Datum) 0);

	MyPGLogicalWorker = &PGLogicalCtx->workers[slot];
	Assert(MyPGLogicalWorker->proc == NULL);
	Assert(MyPGLogicalWorker->worker_type == type);
	MyPGLogicalWorker->proc = MyProc;
	MyPGLogicalWorkerGeneration = MyPGLogicalWorker->generation;

	elog(DEBUG2, "%s worker [%d] attaching to slot %d generation %hu",
		 pglogical_worker_type_name(type), MyProcPid, slot,
		 MyPGLogicalWorkerGeneration);

	/*
	 * So we can find workers in valgrind output, send a Valgrind client
	 * request to print to the Valgrind log.
	 */
	VALGRIND_PRINTF("PGLOGICAL: pglogical worker %s (%s)\n",
		pglogical_worker_type_name(type),
		MyBgworkerEntry->bgw_name);

	LWLockRelease(PGLogicalCtx->lock);

	/* Make it easy to identify our processes. */
	SetConfigOption("application_name", MyBgworkerEntry->bgw_name,
					PGC_USERSET, PGC_S_SESSION);

	/* Establish signal handlers. */
	BackgroundWorkerUnblockSignals();

	/* Make it easy to identify our processes. */
	SetConfigOption("application_name", MyBgworkerEntry->bgw_name,
					PGC_USERSET, PGC_S_SESSION);

	/* Connect to database if needed. */
	if (MyPGLogicalWorker->dboid != InvalidOid)
	{
		MemoryContext oldcontext;

		BackgroundWorkerInitializeConnectionByOid(MyPGLogicalWorker->dboid,
												  InvalidOid
#if PG_VERSION_NUM >= 110000
												  , 0 /* flags */
#endif
												  );


		StartTransactionCommand();
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		MyProcPort->database_name = pstrdup(get_database_name(MyPGLogicalWorker->dboid));
		MemoryContextSwitchTo(oldcontext);
		CommitTransactionCommand();
	}
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
	Assert(MyPGLogicalWorker->generation == MyPGLogicalWorkerGeneration);
	MyPGLogicalWorker->proc = NULL;

	elog(LOG, "%s worker [%d] at slot %zu generation %hu %s",
		 pglogical_worker_type_name(MyPGLogicalWorker->worker_type),
		 MyProcPid, MyPGLogicalWorker - &PGLogicalCtx->workers[0],
		 MyPGLogicalWorkerGeneration,
		 crash ? "exiting with error" : "detaching cleanly");

	VALGRIND_PRINTF("PGLOGICAL: worker detaching, unclean=%d\n",
		crash);

	/*
	 * If we crashed we need to report it.
	 *
	 * The crash logic only works because all of the workers are attached
	 * to shmem and the serious crashes that we can't catch here cause
	 * postmaster to restart whole server killing all our workers and cleaning
	 * shmem so we start from clean state in that scenario.
	 *
	 * It's vital NOT to clear or change the generation field here; see
	 * wait_for_worker_startup(...).
	 */
	if (crash)
	{
		MyPGLogicalWorker->crashed_at = GetCurrentTimestamp();

		/* Manager crash, make sure supervisor notices. */
		if (MyPGLogicalWorker->worker_type == PGLOGICAL_WORKER_MANAGER)
			PGLogicalCtx->subscriptions_changed = true;
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
		PGLogicalWorker	   *w = &PGLogicalCtx->workers[i];

		if (w->worker_type == PGLOGICAL_WORKER_APPLY &&
			dboid == w->dboid &&
			subscriberid == w->worker.apply.subid)
			return w;
	}

	return NULL;
}

/*
 * Find all apply workers for given database.
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
pglogical_sync_find(Oid dboid, Oid subscriberid, const char *nspname, const char *relname)
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
	{
		elog(DEBUG2, "killing pglogical %s worker [%d] at slot %zu",
			 pglogical_worker_type_name(worker->worker_type),
			 worker->proc->pid, (worker - &PGLogicalCtx->workers[0]));
		kill(worker->proc->pid, SIGTERM);
	}
}

static void
signal_worker_xact_callback(XactEvent event, void *arg)
{
	if (event == XACT_EVENT_COMMIT && xacthook_signal_workers)
	{
		PGLogicalWorker	   *w;
		ListCell	   *l;

		LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);

		foreach (l, signal_workers)
		{
			signal_worker_item *item = (signal_worker_item *) lfirst(l);

			w = pglogical_apply_find(MyDatabaseId, item->subid);
			if (item->kill)
				pglogical_worker_kill(w);
			else if (pglogical_worker_running(w))
			{
				w->worker.apply.sync_pending = true;
				SetLatch(&w->proc->procLatch);
			}
		}

		PGLogicalCtx->subscriptions_changed = true;

		/* Signal the manager worker, if there's one */
		w = pglogical_manager_find(MyDatabaseId);
		if (pglogical_worker_running(w))
			SetLatch(&w->proc->procLatch);

		/* and signal the supervisor, for good measure */
		if (PGLogicalCtx->supervisor)
			SetLatch(&PGLogicalCtx->supervisor->procLatch);

		LWLockRelease(PGLogicalCtx->lock);

		list_free_deep(signal_workers);
		signal_workers = NIL;

		xacthook_signal_workers = false;
	}
}

/*
 * Enqueue signal for supervisor/manager at COMMIT.
 */
void
pglogical_subscription_changed(Oid subid, bool kill)
{
	if (!xact_cb_installed)
	{
		RegisterXactCallback(signal_worker_xact_callback, NULL);
		xact_cb_installed = true;
	}

	if (OidIsValid(subid))
	{
		MemoryContext	oldcxt;
		signal_worker_item *item;

		oldcxt = MemoryContextSwitchTo(TopTransactionContext);

		item = palloc(sizeof(signal_worker_item));
		item->subid = subid;
		item->kill = kill;

		signal_workers = lappend(signal_workers, item);

		MemoryContextSwitchTo(oldcxt);
	}

	xacthook_signal_workers = true;
}

static size_t
worker_shmem_size(int nworkers)
{
	return offsetof(PGLogicalContext, workers) +
		sizeof(PGLogicalWorker) * nworkers;
}

/*
 * Init shmem needed for workers.
 */
static void
pglogical_worker_shmem_startup(void)
{
	bool        found;
	int			nworkers;

	if (prev_shmem_startup_hook != NULL)
		prev_shmem_startup_hook();

	/*
	 * This is kludge for Windows (Postgres does not define the GUC variable
	 * as PGDLLIMPORT)
	 */
	nworkers = atoi(GetConfigOptionByName("max_worker_processes", NULL,
										  false));

	/* Init signaling context for the various processes. */
	PGLogicalCtx = ShmemInitStruct("pglogical_context",
								   worker_shmem_size(nworkers), &found);

	if (!found)
	{
		PGLogicalCtx->lock = &(GetNamedLWLockTranche("pglogical"))->lock;
		PGLogicalCtx->supervisor = NULL;
		PGLogicalCtx->subscriptions_changed = false;
		PGLogicalCtx->total_workers = nworkers;
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
	int			nworkers;

	Assert(process_shared_preload_libraries_in_progress);

	/*
	 * This is cludge for Windows (Postgres des not define the GUC variable
	 * as PGDDLIMPORT)
	 */
	nworkers = atoi(GetConfigOptionByName("max_worker_processes", NULL,
										  false));

	/* Allocate enough shmem for the worker limit ... */
	RequestAddinShmemSpace(worker_shmem_size(nworkers));

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

const char *
pglogical_worker_type_name(PGLogicalWorkerType type)
{
	switch (type)
	{
		case PGLOGICAL_WORKER_NONE: return "none";
		case PGLOGICAL_WORKER_MANAGER: return "manager";
		case PGLOGICAL_WORKER_APPLY: return "apply";
		case PGLOGICAL_WORKER_SYNC: return "sync";
		default: Assert(false); return NULL;
	}
}
