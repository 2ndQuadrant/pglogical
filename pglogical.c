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

#include "catalog/namespace.h"
#include "catalog/pg_database.h"
#include "catalog/pg_type.h"

#include "mb/pg_wchar.h"

#include "storage/ipc.h"
#include "storage/proc.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"

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

bool pglogical_synchronous_commit = false;

void _PG_init(void);
void pglogical_supervisor_main(Datum main_arg);


/*
 * Ensure string is not longer than maxlen.
 *
 * The way we do this is we if the string is longer we return prefix from that
 * string and hash of the string which will together be exatly maxlen.
 *
 * Maxlen can't be less than 8 because hash produces uint32 which in hex form
 * can have up to 8 characters.
 */
char *
shorten_hash(const char *str, int maxlen)
{
	char   *ret;
	int		len = strlen(str);

	Assert(maxlen >= 8);

	if (len <= maxlen)
		return pstrdup(str);

	ret = (char *) palloc(maxlen + 1);
	snprintf(ret, maxlen, "%.*s%08x", maxlen - 8,
			 str, DatumGetUInt32(hash_any((unsigned char *) str, len)));
	ret[maxlen] = '\0';

	return ret;
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
 * Get oid of our queue table.
 */
inline Oid
get_pglogical_table_oid(const char *table)
{
	Oid			nspoid;
	Oid			reloid;

	nspoid = get_namespace_oid(EXTENSION_NAME, false);

	reloid = get_relname_relid(table, nspoid);

	if (reloid == InvalidOid)
		elog(ERROR, "cache lookup failed for relation %s.%s",
			 EXTENSION_NAME, table);

	return reloid;
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
		ereport(ERROR,
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
		ereport(ERROR,
				(errmsg("could not connect to the postgresql server in replication mode: %s",
						PQerrorMessage(conn)),
				 errdetail("dsn was: %s", dsn.data)));
	}

	return conn;
}

void
pglogical_start_replication(PGconn *streamConn, const char *slot_name,
							XLogRecPtr start_pos, const char *forward_origins,
							const char *replication_sets,
							const char *replicate_only_table)
{
	StringInfoData	command;
	PGresult	   *res;
	char		   *sqlstate;

	initStringInfo(&command);
	appendStringInfo(&command, "START_REPLICATION SLOT \"%s\" LOGICAL %X/%X (",
					 slot_name,
					 (uint32) (start_pos >> 32),
					 (uint32) start_pos);

	/* Basic protocol info. */
	appendStringInfo(&command, "expected_encoding '%s'",
					 GetDatabaseEncodingName());
	appendStringInfo(&command, ", min_proto_version '%d'", PGLOGICAL_MIN_PROTO_VERSION_NUM);
	appendStringInfo(&command, ", max_proto_version '%d'", PGLOGICAL_MAX_PROTO_VERSION_NUM);
	appendStringInfo(&command, ", startup_params_format '1'");

	/* Binary protocol compatibility. */
	appendStringInfo(&command, ", \"binary.want_internal_basetypes\" '1'");
	appendStringInfo(&command, ", \"binary.want_binary_basetypes\" '1'");
	appendStringInfo(&command, ", \"binary.basetypes_major_version\" '%u'",
					 PG_VERSION_NUM/100);
	appendStringInfo(&command, ", \"binary.sizeof_datum\" '%zu'",
					 sizeof(Datum));
	appendStringInfo(&command, ", \"binary.sizeof_int\" '%zu'", sizeof(int));
	appendStringInfo(&command, ", \"binary.sizeof_long\" '%zu'", sizeof(long));
	appendStringInfo(&command, ", \"binary.bigendian\" '%d'",
#ifdef WORDS_BIGENDIAN
					 true
#else
					 false
#endif
					 );
	appendStringInfo(&command, ", \"binary.float4_byval\" '%d'",
#ifdef USE_FLOAT4_BYVAL
					 true
#else
					 false
#endif
					 );
	appendStringInfo(&command, ", \"binary.float8_byval\" '%d'",
#ifdef USE_FLOAT8_BYVAL
					 true
#else
					 false
#endif
					 );
	appendStringInfo(&command, ", \"binary.integer_datetimes\" '%d'",
#ifdef USE_INTEGER_DATETIMES
					 true
#else
					 false
#endif
					 );

	appendStringInfoString(&command,
						   ", \"hooks.setup_function\" 'pglogical.pglogical_hooks_setup'");

	if (forward_origins)
		appendStringInfo(&command, ", \"pglogical.forward_origins\" %s",
					 quote_literal_cstr(forward_origins));

	if (replicate_only_table)
	{
		/* Send the table name we want to the upstream */
		appendStringInfoString(&command, ", \"pglogical.replicate_only_table\" ");
		appendStringInfoString(&command, quote_literal_cstr(replicate_only_table));
	}

	if (replication_sets)
	{
		/* Send the replication set names we want to the upstream */
		appendStringInfoString(&command, ", \"pglogical.replication_set_names\" ");
		appendStringInfoString(&command, quote_literal_cstr(replication_sets));
	}

	/* Tell the upstream that we want unbounded metadata cache size */
	appendStringInfoString(&command, ", \"relmeta_cache_size\" '-1'");

	/* general info about the downstream */
	appendStringInfo(&command, ", pg_version '%u'", PG_VERSION_NUM);
	appendStringInfo(&command, ", pglogical_version '%s'", PGLOGICAL_VERSION);
	appendStringInfo(&command, ", pglogical_version_num '%d'", PGLOGICAL_VERSION_NUM);

	appendStringInfoChar(&command, ')');

	res = PQexec(streamConn, command.data);
	sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
	if (PQresultStatus(res) != PGRES_COPY_BOTH)
		elog(FATAL, "could not send replication command \"%s\": %s\n, sqlstate: %s",
			 command.data, PQresultErrorMessage(res), sqlstate);
	PQclear(res);
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
	PGLogicalCtx->connections_changed = true;

	/* Setup connection to pinned catalogs (we only ever read pg_database). */
#if PG_VERSION_NUM >= 90500
	BackgroundWorkerInitializeConnection(NULL, NULL);
#else
	BackgroundWorkerInitializeConnection("postgres", NULL);
#endif

	/* Main wait loop. */
	while (!got_SIGTERM)
    {
		int rc;

		if (PGLogicalCtx->connections_changed)
		{
			PGLogicalCtx->connections_changed = false;
			StartTransactionCommand();
			start_manager_workers();
			CommitTransactionCommand();
		}

		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   180000L);

        ResetLatch(&MyProc->procLatch);

        /* emergency bailout if postmaster has died */
        if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}

	proc_exit(1);
}


/*
 * Entry point for this module.
 */
void
_PG_init(void)
{
	BackgroundWorker bgw;

	if (!process_shared_preload_libraries_in_progress)
		elog(ERROR, "pglogical is not in shared_preload_libraries");

	DefineCustomEnumVariable("pglogical.conflict_resolution",
							 gettext_noop("Sets method used for conflict resolution for resolvable conflicts."),
							 NULL,
							 &pglogical_conflict_resolver,
							 PGLOGICAL_RESOLVE_APPLY_REMOTE,
							 PGLogicalConflictResolvers,
							 PGC_SUSET, 0,
							 pglogical_conflict_resolver_check_hook,
							 NULL, NULL);

	DefineCustomBoolVariable("pglogical.synchronous_commit",
							 "pglogical specific synchronous commit value",
							 NULL,
							 &pglogical_synchronous_commit,
							 false, PGC_POSTMASTER,
							 0,
							 NULL, NULL, NULL);

	if (IsBinaryUpgrade)
		return;

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
	bgw.bgw_restart_time = 5;
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&bgw);
}
