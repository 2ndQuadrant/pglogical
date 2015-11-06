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
#include "access/htup_details.h"
#include "access/xact.h"
#include "access/xlog.h"

#include "catalog/pg_database.h"
#include "catalog/pg_type.h"

#include "storage/ipc.h"
#include "storage/proc.h"

#include "utils/builtins.h"

#include "pglogical_node.h"
#include "pglogical_conflict.h"
#include "pglogical_worker.h"
#include "pglogical.h"

PG_MODULE_MAGIC;

static const struct config_enum_entry PGLogicalConflictResolvers[] = {
	{"error", PGLOGICAL_RESOLVE_ERROR, false},
	{"apply_remote", PGLOGICAL_RESOLVE_APPLY_REMOTE, false},
	{"keep_local", PGLOGICAL_RESOLVE_KEEP_LOCAL, false},
	{"last_update_wins", PGLOGICAL_RESOLVE_LAST_UPDATE_WINS, false},
	{"first_update_wins", PGLOGICAL_RESOLVE_FIRST_UPDATE_WINS, false},
	{NULL, 0, false}
};

void _PG_init(void);
void pglogical_supervisor_main(Datum main_arg);


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
gen_slot_name(Name slot_name, char *dbname, const char *provider_name,
			  const char *subscriber_name)
{
	snprintf(NameStr(*slot_name), NAMEDATALEN,
			 "pgl_%s_%s_%s",
			 shorten_hash(dbname, 16),
			 shorten_hash(provider_name, 16),
			 shorten_hash(subscriber_name, 16));
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
 * Make standard postgres connection, ERROR on failure.
 */
PGconn *
pglogical_connect(const char *connstring, const char *connname)
{
	PGconn		   *conn;
	StringInfoData	dsn;

	initStringInfo(&dsn);
	appendStringInfo(&dsn,
					"%s fallback_application_name='%s'",
					connstring, connname);

	conn = PQconnectdb(dsn.data);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		ereport(FATAL,
				(errmsg("could not connect to the postgresql server: %s",
						PQerrorMessage(conn)),
				 errdetail("dsn was: %s", dsn.data)));
	}

	return conn;
}

/*
 * Make replication connection, ERROR on failure.
 */
PGconn *
pglogical_connect_replica(const char *connstring, const char *connname)
{
	PGconn		   *conn;
	StringInfoData	dsn;

	initStringInfo(&dsn);
	appendStringInfo(&dsn,
					"%s replication=database fallback_application_name='%s'",
					connstring, connname);

	conn = PQconnectdb(dsn.data);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		ereport(FATAL,
				(errmsg("could not connect to the postgresql server in replication mode: %s",
						PQerrorMessage(conn)),
				 errdetail("dsn was: %s", dsn.data)));
	}

	return conn;
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
		PGLogicalWorker		worker;

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

		memset(&worker, 0, sizeof(PGLogicalWorker));
		worker.worker_type = PGLOGICAL_WORKER_MANAGER;
		worker.dboid = dboid;
		pglogical_worker_register(&worker);
	}

	heap_endscan(scan);
	heap_close(rel, AccessShareLock);
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
							 PGLOGICAL_RESOLVE_APPLY_REMOTE,
							 PGLogicalConflictResolvers,
							 PGC_SUSET, 0,
							 pglogical_conflict_resolver_check_hook,
							 NULL, NULL);

	if (IsBinaryUpgrade)
		return;

	if (!process_shared_preload_libraries_in_progress)
		elog(ERROR, "pglogical is not in shared_preload_libraries");

	/* Init workers. */
	pglogical_worker_shmem_init();

	/* Run the supervisor. */
	bgw.bgw_flags =	BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_main = NULL;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN,
			 EXTENSION_NAME);
	snprintf(bgw.bgw_function_name, BGW_MAXLEN,
			 "pglogical_supervisor_main");
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "pglogical supervisor");
	bgw.bgw_restart_time = 1;
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&bgw);
}
