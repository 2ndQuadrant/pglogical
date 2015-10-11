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

#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "access/xlog.h"

#include "catalog/pg_database.h"
#include "catalog/pg_type.h"

#include "postmaster/bgworker.h"

#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"

#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#include "pglogical_node.h"
#include "pglogical_conflict.h"
#include "pglogical.h"

PG_MODULE_MAGIC;

volatile sig_atomic_t got_SIGTERM = false;

static const struct config_enum_entry PGLogicalConflictResolvers[] = {
	{"error", PGLOGICAL_RESOLVE_ERROR, false},
	{"apply_remote", PGLOGICAL_RESOLVE_APPLY_REMOTE, false},
	{"keep_local", PGLOGICAL_RESOLVE_KEEP_LOCAL, false},
	{"last_update_wins", PGLOGICAL_RESOLVE_LAST_UPDATE_WINS, false},
	{"first_update_wins", PGLOGICAL_RESOLVE_FIRST_UPDATE_WINS, false},
	{NULL, 0, false}
};

typedef struct PGLogicalManager {
	/*
	 * We identify manager workers by dboid. Even though PGPROC has databaseId
	 * the worker might die before the databaseId is assigned.
	 */
	Oid		dboid;

	/* Pointer to proc array. */
	PGPROC *proc;
} PGLogicalManager;

static bool xacthook_signal_workers = false;

typedef struct PGLogicalContext {
	/* Write lock. */
	LWLock	   *lock;

	/* Supervisor process. */
	PGPROC	   *supervisor;

	/* Manager workers. */
	int			total_managers;
	PGLogicalManager  managers[FLEXIBLE_ARRAY_MEMBER];
} PGLogicalContext;

static PGLogicalContext *PGLogicalCtx = NULL;
static PGLogicalManager *MyPGLogicalManager = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

void _PG_init(void);
void pglogical_supervisor_main(Datum main_arg);
static void signal_worker_xact_callback(XactEvent event, void *arg);
static PGLogicalManager *pglogical_manager_find(Oid dboid);

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
 * Ensure string is not longer than maxlen.
 *
 * The way we do this is we if the string is longer we return prefix from that
 * string and hash of the string which will together be exatly maxlen.
 *
 * Maxlen can't be less than 11 because hash produces uint32 which in text form
 * can have up to 10 characters.
 */
static char *
shorten_hash(const char *str, int maxlen)
{
	char   *ret;
	int		len = strlen(str);

	Assert(maxlen > 10);

	if (len <= maxlen)
		return pstrdup(str);

	ret = (char *) palloc(maxlen + 1);
	snprintf(ret, maxlen, "%*s%u", maxlen - 10, /* uint32 max length is 10 */
			 str, DatumGetUInt32(hash_any((unsigned char *) str, len)));
	ret[maxlen] = '\0';

	return ret;
}

/*
 * Generate slot name (used also for origin identifier)
 */
void
gen_slot_name(Name slot_name, char *dbname, PGLogicalNode *origin_node,
			  PGLogicalNode *target_node)
{
	snprintf(NameStr(*slot_name), NAMEDATALEN,
			 "pgl_%s_%s_%s",
			 shorten_hash(dbname, 16),
			 shorten_hash(origin_node->name, 16),
			 shorten_hash(target_node->name, 16));
	NameStr(*slot_name)[NAMEDATALEN-1] = '\0';
}

/*
 * Generates new random id, hopefully unique enough.
 *
 * TODO: this could be improved.
 */
Oid
pglogical_generate_id(void)
{
	uint32	hashinput[3];
	uint64	sysid = GetSystemIdentifier();
	uint32	id = random(); /* XXX: this would be better as sequence. */

	do
	{
		hashinput[0] = (uint32) sysid;
		hashinput[1] = (uint32) (sysid >> 32);
		hashinput[2] = id++;

		id = DatumGetUInt32(hash_any((const unsigned char *) hashinput,
									 (int) sizeof(hashinput)));
	}
	while (id == InvalidOid); /* Protect against returning InvalidOid */

	return id;
}

/*
 * Convert text array to list of strings.
 *
 * Note: the resulting list points to the memory of the input array.
 */
List *
textarray_to_list(ArrayType *textarray)
{
	Datum		   *elems;
	int				nelems, i;
	List		   *res = NIL;

	deconstruct_array(textarray,
					  TEXTOID, -1, false, 'i',
					  &elems, NULL, &nelems);

	if (nelems == 0)
		return NIL;

	for (i = 0; i < nelems; i++)
		res = lappend(res, TextDatumGetCString(elems[i]));

	return res;
}

/*
 * Register the manager bgworker for the given DB. The manager worker will then
 * start the apply workers.
 *
 * Called in postmaster context from _PG_init, and under backend from node join
 * funcions.
 */
static void
pglogical_manager_register(Oid dboid)
{
	BackgroundWorker	bgw;
	BackgroundWorkerHandle *bgw_handle;
	pid_t				pid;

	bgw.bgw_flags =	BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_main = NULL;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN,
			 EXTENSION_NAME);
	snprintf(bgw.bgw_function_name, BGW_MAXLEN,
			 "pglogical_manager_main");
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	bgw.bgw_notify_pid = MyProcPid;
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "pglogical manager");
	bgw.bgw_main_arg = ObjectIdGetDatum(dboid);

	if (!RegisterDynamicBackgroundWorker(&bgw, &bgw_handle))
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("Registering worker failed, check prior log messages for details")));
	}

	/* TODO: handle crash? */
	WaitForBackgroundWorkerStartup(bgw_handle, &pid);
}

/*
 * Attach the current master process to the PGLogicalCtx.
 *
 * Called during by master worker startup.
 */
void
pglogical_manager_attach(Oid dboid)
{
	int i;
	int	emptyslot = -1;

	LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);

	for (i = 0; i < PGLogicalCtx->total_managers; i++)
	{
		if (dboid == PGLogicalCtx->managers[i].dboid)
		{
			LWLockRelease(PGLogicalCtx->lock);

			if (PGLogicalCtx->managers[i].proc != MyProc)
				elog(ERROR, "PGLogical state shared memory corrupted");

			return;
		}

		if (PGLogicalCtx->managers[i].dboid == InvalidOid && emptyslot < 0)
			emptyslot = i;
	}

	/* TODO hint about max_worker_processes. */
	if (emptyslot < 0)
	{
		LWLockRelease(PGLogicalCtx->lock);
		elog(ERROR, "no free slot found for PGLogical worker");
	}


	MyPGLogicalManager = &PGLogicalCtx->managers[emptyslot];

	/* Assign the slot to current proccess. */
	MyPGLogicalManager->dboid = dboid;
	MyPGLogicalManager->proc = MyProc;

	LWLockRelease(PGLogicalCtx->lock);
}

/*
 * Find the manager worker for given database.
 */
static PGLogicalManager *
pglogical_manager_find(Oid dboid)
{
	int i;

	Assert(LWLockHeldByMe(PGLogicalCtx->lock));

	for (i = 0; i < PGLogicalCtx->total_managers; i++)
	{
		if (dboid == PGLogicalCtx->managers[i].dboid)
			return &PGLogicalCtx->managers[i];
	}

	return NULL;
}

/*
 * Detach the current master process from the PGLogicalCtx.
 *
 * Called during master worker exit.
 */
void
pglogical_manager_detach(bool signal_supervisor)
{
	/* Nothing to detach. */
	if (MyPGLogicalManager == NULL)
		return;

	LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);

	Assert(MyPGLogicalManager->proc = MyProc);
	MyPGLogicalManager->dboid = InvalidOid;

	/* Signal the supervisor process. */
	if (signal_supervisor && PGLogicalCtx->supervisor)
		SetLatch(&PGLogicalCtx->supervisor->procLatch);

	LWLockRelease(PGLogicalCtx->lock);
}

/*
 * Start the manager workers for every db which has a pglogical node.
 *
 * Note that we start workers that are not necessary here. We do this because
 * we need to check every individual database to check if there is pglogical
 * node setup and it's not possible to switch connections to different
 * databases within one background worker. The workers that won't find any
 * pglogical node setup will exit immediately during startup.
 * This behavior can cause issue where we consume all the allowed workers and
 * eventually error out even though the max_worker_processes is set high enough
 * to satisfy the actual needed worker count.
 *
 * Must be run inside a transaction.
 */
static void
start_manager_workers(void)
{
	Relation	rel;
	HeapScanDesc scan;
	HeapTuple	tup;

	/* Run manager worker for every connectable database. */
	rel = heap_open(DatabaseRelationId, AccessShareLock);
	scan = heap_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database	pgdatabase = (Form_pg_database) GETSTRUCT(tup);
		Oid					dboid = HeapTupleGetOid(tup);

		CHECK_FOR_INTERRUPTS();

		/* Can't run workers on databases which don't allow connection. */
		if (!pgdatabase->datallowconn)
			continue;

		/* Worker already attached, nothing to do. */
		LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);
		if (pglogical_manager_find(dboid))
		{
			LWLockRelease(PGLogicalCtx->lock);
			continue;
		}
		LWLockRelease(PGLogicalCtx->lock);

		/* No record found, try running new worker. */
		elog(DEBUG1, "registering pglogical manager process for database %s",
			 NameStr(pgdatabase->datname));
		pglogical_manager_register(dboid);
	}

	heap_endscan(scan);
	heap_close(rel, AccessShareLock);
}

static void
signal_worker_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
			if (xacthook_signal_workers)
			{
				PGLogicalManager   *m;

				xacthook_signal_workers = false;

				LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);

				m = pglogical_manager_find(MyDatabaseId);

				if (m)
				{
					/* Signal the manager worker. */
					SetLatch(&m->proc->procLatch);
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

void
pglogical_connections_changed(void)
{
	if (!xacthook_signal_workers)
	{
		RegisterXactCallback(signal_worker_xact_callback, NULL);
		xacthook_signal_workers = true;
	}
}

/*
 * Static bgworker used for initialization and management (our main process).
 */
void
pglogical_supervisor_main(Datum main_arg)
{
	/* Establish signal handlers. */
	pqsignal(SIGTERM, handle_sigterm);
	BackgroundWorkerUnblockSignals();

	/* Assign the latch in shared memory to our process latch. */
	PGLogicalCtx->supervisor = MyProc;

	/* Setup connection to pinned catalogs (we only ever read pg_database). */
	BackgroundWorkerInitializeConnection(NULL, NULL);

	/* Main wait loop. */
	while (!got_SIGTERM)
    {
		int rc;

	   	StartTransactionCommand();

		start_manager_workers();

		CommitTransactionCommand();

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

static void
pglogical_shmem_startup(void)
{
	bool        found;

	/* Init signaling context for supervisor proccess. */
	PGLogicalCtx = ShmemInitStruct("pglogical_context",
								   offsetof(PGLogicalContext, managers) +
								   sizeof(PGLogicalManager) * max_worker_processes,
								   &found);
	if (!found)
	{
		PGLogicalCtx->lock = LWLockAssign();
		PGLogicalCtx->supervisor = NULL;
		PGLogicalCtx->total_managers = max_worker_processes;
		memset(PGLogicalCtx->managers, 0,
			   sizeof(PGLogicalManager) * max_worker_processes);
	}
}


/*
 * Entry point for this module.
 */
void
_PG_init(void)
{
	BackgroundWorker bgw;

	DefineCustomEnumVariable("pglogical.conflict_resolution",
							 gettext_noop("Sets method used for conflict resolution for resolvable conflicts."),
							 NULL,
							 &pglogical_conflict_resolver,
							 PGLOGICAL_RESOLVE_ERROR,
							 PGLogicalConflictResolvers,
							 PGC_SUSET, 0,
							 pglogical_conflict_resolver_check_hook,
							 NULL, NULL);

	if (IsBinaryUpgrade)
		return;

	/* Init hooks. */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pglogical_shmem_startup;

	/* Run the supervisor. */
	bgw.bgw_flags =	BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_main = NULL;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN,
			 EXTENSION_NAME);
	snprintf(bgw.bgw_function_name, BGW_MAXLEN,
			 "pglogical_supervisor_main");
	bgw.bgw_restart_time = 1;
	bgw.bgw_notify_pid = 0;
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "pglogical supervisor");
	bgw.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&bgw);
}
