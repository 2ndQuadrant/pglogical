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

#include "catalog/pg_extension.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_database.h"
#include "catalog/pg_type.h"

#include "commands/extension.h"
#include "commands/trigger.h"

#include "executor/executor.h"

#include "mb/pg_wchar.h"

#include "nodes/nodeFuncs.h"

#include "optimizer/planner.h"

#include "parser/parse_coerce.h"

#include "replication/reorderbuffer.h"

#include "storage/ipc.h"
#include "storage/proc.h"

#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "pgstat.h"

#include "pglogical_executor.h"
#include "pglogical_node.h"
#include "pglogical_conflict.h"
#include "pglogical_worker.h"
#include "pglogical.h"

PG_MODULE_MAGIC;

static const struct config_enum_entry PGLogicalConflictResolvers[] = {
	{"error", PGLOGICAL_RESOLVE_ERROR, false},
#ifndef XCP
	{"apply_remote", PGLOGICAL_RESOLVE_APPLY_REMOTE, false},
	{"keep_local", PGLOGICAL_RESOLVE_KEEP_LOCAL, false},
	{"last_update_wins", PGLOGICAL_RESOLVE_LAST_UPDATE_WINS, false},
	{"first_update_wins", PGLOGICAL_RESOLVE_FIRST_UPDATE_WINS, false},
#endif
	{NULL, 0, false}
};

/* copied fom guc.c */
static const struct config_enum_entry server_message_level_options[] = {
	{"debug", DEBUG2, true},
	{"debug5", DEBUG5, false},
	{"debug4", DEBUG4, false},
	{"debug3", DEBUG3, false},
	{"debug2", DEBUG2, false},
	{"debug1", DEBUG1, false},
	{"info", INFO, false},
	{"notice", NOTICE, false},
	{"warning", WARNING, false},
	{"error", ERROR, false},
	{"log", LOG, false},
	{"fatal", FATAL, false},
	{"panic", PANIC, false},
	{NULL, 0, false}
};

bool	pglogical_synchronous_commit = false;
char   *pglogical_temp_directory = "";
bool	pglogical_use_spi = false;
bool	pglogical_batch_inserts = true;
static char *pglogical_temp_directory_config;

void _PG_init(void);
void pglogical_supervisor_main(Datum main_arg);
char *pglogical_extra_connection_options;

static PGconn * pglogical_connect_base(const char *connstr,
									   const char *appname,
									   const char *suffix,
									   bool replication);


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
 * Deconstruct the text representation of a 1-dimensional Postgres array
 * into individual items.
 *
 * On success, returns true and sets *itemarray and *nitems to describe
 * an array of individual strings.  On parse failure, returns false;
 * *itemarray may exist or be NULL.
 *
 * NOTE: free'ing itemarray is sufficient to deallocate the working storage.
 */
bool
parsePGArray(const char *atext, char ***itemarray, int *nitems)
{
	int			inputlen;
	char	  **items;
	char	   *strings;
	int			curitem;

	/*
	 * We expect input in the form of "{item,item,item}" where any item is
	 * either raw data, or surrounded by double quotes (in which case embedded
	 * characters including backslashes and quotes are backslashed).
	 *
	 * We build the result as an array of pointers followed by the actual
	 * string data, all in one malloc block for convenience of deallocation.
	 * The worst-case storage need is not more than one pointer and one
	 * character for each input character (consider "{,,,,,,,,,,}").
	 */
	*itemarray = NULL;
	*nitems = 0;
	inputlen = strlen(atext);
	if (inputlen < 2 || atext[0] != '{' || atext[inputlen - 1] != '}')
		return false;			/* bad input */
	items = (char **) malloc(inputlen * (sizeof(char *) + sizeof(char)));
	if (items == NULL)
		return false;			/* out of memory */
	*itemarray = items;
	strings = (char *) (items + inputlen);

	atext++;					/* advance over initial '{' */
	curitem = 0;
	while (*atext != '}')
	{
		if (*atext == '\0')
			return false;		/* premature end of string */
		items[curitem] = strings;
		while (*atext != '}' && *atext != ',')
		{
			if (*atext == '\0')
				return false;	/* premature end of string */
			if (*atext != '"')
				*strings++ = *atext++;	/* copy unquoted data */
			else
			{
				/* process quoted substring */
				atext++;
				while (*atext != '"')
				{
					if (*atext == '\0')
						return false;	/* premature end of string */
					if (*atext == '\\')
					{
						atext++;
						if (*atext == '\0')
							return false;		/* premature end of string */
					}
					*strings++ = *atext++;		/* copy quoted data */
				}
				atext++;
			}
		}
		*strings++ = '\0';
		if (*atext == ',')
			atext++;
		curitem++;
	}
	if (atext[1] != '\0')
		return false;			/* bogus syntax (embedded '}') */
	*nitems = curitem;
	return true;
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

#define CONN_PARAM_ARRAY_SIZE 9

static PGconn *
pglogical_connect_base(const char *connstr, const char *appname,
					   const char *suffix, bool replication)
{
	int				i=0;
	PGconn		   *conn;
	const char	   *keys[CONN_PARAM_ARRAY_SIZE];
	const char	   *vals[CONN_PARAM_ARRAY_SIZE];
	StringInfoData s;

	initStringInfo(&s);
	appendStringInfoString(&s, pglogical_extra_connection_options);
	appendStringInfoChar(&s, ' ');
	appendStringInfoString(&s, connstr);

	keys[i] = "dbname";
	vals[i] = connstr;
	i++;
	keys[i] = "application_name";
	if (suffix)
	{
		char	s[NAMEDATALEN];
		snprintf(s, NAMEDATALEN,
			 "%s_%s",
			 shorten_hash(appname, NAMEDATALEN - strlen(suffix) - 2),
			 suffix);
		vals[i] = s;
	}
	else
		vals[i] = appname;
	i++;
	keys[i] = "connect_timeout";
	vals[i] = "30";
	i++;
	keys[i] = "keepalives";
	vals[i] = "1";
	i++;
	keys[i] = "keepalives_idle";
	vals[i] = "20";
	i++;
	keys[i] = "keepalives_interval";
	vals[i] = "20";
	i++;
	keys[i] = "keepalives_count";
	vals[i] = "5";
	i++;
	keys[i] = "replication";
	vals[i] = replication ? "database" : NULL;
	i++;
	keys[i] = NULL;
	vals[i] = NULL;

	Assert(i <= CONN_PARAM_ARRAY_SIZE);

	/*
	 * We use the expand_dbname parameter to process the connection string
	 * (or URI), and pass some extra options.
	 */
	conn = PQconnectdbParams(keys, vals, /* expand_dbname = */ true);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		ereport(ERROR,
				(errmsg("could not connect to the postgresql server%s: %s",
						replication ? " in replication mode" : "",
						PQerrorMessage(conn)),
				 errdetail("dsn was: %s", s.data)));
	}

	resetStringInfo(&s);

	return conn;
}


/*
 * Make standard postgres connection, ERROR on failure.
 */
PGconn *
pglogical_connect(const char *connstring, const char *connname,
				  const char *suffix)
{
	return pglogical_connect_base(connstring, connname, suffix, false);
}

/*
 * Make replication connection, ERROR on failure.
 */
PGconn *
pglogical_connect_replica(const char *connstring, const char *connname,
						  const char *suffix)
{
	return pglogical_connect_base(connstring, connname, suffix, true);
}

/*
 * Make sure the extension is up to date.
 *
 * Called by db manager.
 */
void
pglogical_manage_extension(void)
{
	Relation	extrel;
	SysScanDesc scandesc;
	HeapTuple	tuple;
	ScanKeyData key[1];

	if (RecoveryInProgress())
		return;

	PushActiveSnapshot(GetTransactionSnapshot());

	/* make sure we're operating without other pglogical workers interfering */
	extrel = table_open(ExtensionRelationId, ShareUpdateExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_pg_extension_extname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(EXTENSION_NAME));

	scandesc = systable_beginscan(extrel, ExtensionNameIndexId, true,
								  NULL, 1, key);

	tuple = systable_getnext(scandesc);

	/* No extension, nothing to update. */
	if (HeapTupleIsValid(tuple))
	{
		Datum		datum;
		bool		isnull;
		char	   *extversion;

		/* Determine extension version. */
		datum = heap_getattr(tuple, Anum_pg_extension_extversion,
							 RelationGetDescr(extrel), &isnull);
		if (isnull)
			elog(ERROR, "extversion is null");
		extversion = text_to_cstring(DatumGetTextPP(datum));

		/* Only run the alter if the versions don't match. */
		if (strcmp(extversion, PGLOGICAL_VERSION) != 0)
		{
			AlterExtensionStmt alter_stmt;

			alter_stmt.options = NIL;
			alter_stmt.extname = EXTENSION_NAME;
			ExecAlterExtensionStmt(&alter_stmt);
		}
	}

	systable_endscan(scandesc);
	table_close(extrel, NoLock);

	PopActiveSnapshot();
}

/*
 * Call IDENTIFY_SYSTEM on the connection and report its results.
 */
void
pglogical_identify_system(PGconn *streamConn, uint64* sysid,
							TimeLineID *timeline, XLogRecPtr *xlogpos,
							Name *dbname)
{
	PGresult	   *res;

	res = PQexec(streamConn, "IDENTIFY_SYSTEM");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		elog(ERROR, "could not send replication command \"%s\": %s",
			 "IDENTIFY_SYSTEM", PQerrorMessage(streamConn));
	}
	if (PQntuples(res) != 1 || PQnfields(res) < 4)
	{
		elog(ERROR, "could not identify system: got %d rows and %d fields, expected %d rows and at least %d fields\n",
			 PQntuples(res), PQnfields(res), 1, 4);
	}

	if (PQnfields(res) > 4)
	{
		elog(DEBUG2, "ignoring extra fields in IDENTIFY_SYSTEM response; expected 4, got %d",
			 PQnfields(res));
	}

	if (sysid != NULL)
	{
		const char *remote_sysid = PQgetvalue(res, 0, 0);
		if (sscanf(remote_sysid, UINT64_FORMAT, sysid) != 1)
			elog(ERROR, "could not parse remote sysid %s", remote_sysid);
	}

	if (timeline != NULL)
	{
		const char *remote_tlid = PQgetvalue(res, 0, 1);
		if (sscanf(remote_tlid, "%u", timeline) != 1)
			elog(ERROR, "could not parse remote tlid %s", remote_tlid);
	}

	if (xlogpos != NULL)
	{
		const char *remote_xlogpos = PQgetvalue(res, 0, 2);
		uint32 xlogpos_low, xlogpos_high;
		if (sscanf(remote_xlogpos, "%X/%X", &xlogpos_high, &xlogpos_low) != 2)
			elog(ERROR, "could not parse remote xlogpos %s", remote_xlogpos);
		*xlogpos = (((XLogRecPtr)xlogpos_high)<<32) + xlogpos_low;
	}

	if (dbname != NULL)
	{
		char *remote_dbname = PQgetvalue(res, 0, 3);
		strncpy(NameStr(**dbname), remote_dbname, NAMEDATALEN);
		NameStr(**dbname)[NAMEDATALEN-1] = '\0';
	}

	PQclear(res);
}

void
pglogical_start_replication(PGconn *streamConn, const char *slot_name,
							XLogRecPtr start_pos, const char *forward_origins,
							const char *replication_sets,
							const char *replicate_only_table,
							bool force_text_transfer)
{
	StringInfoData	command;
	PGresult	   *res;
	char		   *sqlstate;
	const char	   *want_binary = (force_text_transfer ? "0" : "1");

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
	appendStringInfo(&command, ", \"binary.want_internal_basetypes\" '%s'", want_binary);
	appendStringInfo(&command, ", \"binary.want_binary_basetypes\" '%s'", want_binary);
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

	/* We don't care about this anymore but pglogical 1.x expects this. */
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
	appendStringInfo(&command, ", pglogical_apply_pid '%d'", MyProcPid);

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
	TableScanDesc scan;
	HeapTuple	tup;

	/* Run manager worker for every connectable database. */
	rel = table_open(DatabaseRelationId, AccessShareLock);
	scan = table_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database	pgdatabase = (Form_pg_database) GETSTRUCT(tup);
#if PG_VERSION_NUM < 120000
		Oid					dboid = HeapTupleGetOid(tup);
#else
		Oid					dboid = pgdatabase->oid;
#endif
		PGLogicalWorker		worker;

		CHECK_FOR_INTERRUPTS();

		/* Can't run workers on databases which don't allow connection. */
		if (!pgdatabase->datallowconn)
			continue;

		/* Worker already attached, nothing to do. */
		LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);
		if (pglogical_worker_running(pglogical_manager_find(dboid)))
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

	table_endscan(scan);
	table_close(rel, AccessShareLock);
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

	/*
	 * Initialize supervisor info in shared memory.  Strictly speaking we
	 * don't need a lock here, because no other process could possibly be
	 * looking at this shared struct since they're all started by the
	 * supervisor, but let's be safe.
	 */
	LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);
	PGLogicalCtx->supervisor = MyProc;
	PGLogicalCtx->subscriptions_changed = true;
	LWLockRelease(PGLogicalCtx->lock);

	/* Make it easy to identify our processes. */
	SetConfigOption("application_name", MyBgworkerEntry->bgw_name,
					PGC_USERSET, PGC_S_SESSION);

	elog(LOG, "starting pglogical supervisor");

	VALGRIND_PRINTF("PGLOGICAL: supervisor\n");

	/* Setup connection to pinned catalogs (we only ever read pg_database). */
#if PG_VERSION_NUM >= 110000
	BackgroundWorkerInitializeConnection(NULL, NULL, 0);
#elif PG_VERSION_NUM >= 90500
	BackgroundWorkerInitializeConnection(NULL, NULL);
#else
	BackgroundWorkerInitializeConnection("postgres", NULL);
#endif

	/* Main wait loop. */
	while (!got_SIGTERM)
    {
		int rc;

		if (PGLogicalCtx->subscriptions_changed)
		{
			/*
			 * No need to lock here, since we'll take account of all sub
			 * changes up to this point, even if new ones were added between
			 * the test above and flag clear. We're just being woken up.
			 */
			PGLogicalCtx->subscriptions_changed = false;
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

	VALGRIND_PRINTF("PGLOGICAL: supervisor exit\n");
	proc_exit(0);
}

static void
pglogical_temp_directory_assing_hook(const char *newval, void *extra)
{
	if (strlen(newval))
	{
		pglogical_temp_directory = strdup(newval);
	}
	else
	{
#ifndef WIN32
		const char *tmpdir = getenv("TMPDIR");

		if (!tmpdir)
			tmpdir = "/tmp";
#else
		char		tmpdir[MAXPGPATH];
		int			ret;

		ret = GetTempPath(MAXPGPATH, tmpdir);
		if (ret == 0 || ret > MAXPGPATH)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("could not locate temporary directory: %s\n",
							!ret ? strerror(errno) : "")));
			return false;
		}
#endif

		pglogical_temp_directory = strdup(tmpdir);

	}

	if (pglogical_temp_directory == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));
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
#ifdef XCP
							 PGLOGICAL_RESOLVE_ERROR,
#else
							 PGLOGICAL_RESOLVE_APPLY_REMOTE,
#endif
							 PGLogicalConflictResolvers,
							 PGC_SUSET, 0,
							 pglogical_conflict_resolver_check_hook,
							 NULL, NULL);

	DefineCustomEnumVariable("pglogical.conflict_log_level",
							 gettext_noop("Sets log level used for logging resolved conflicts."),
							 NULL,
							 &pglogical_conflict_log_level,
							 LOG,
							 server_message_level_options,
							 PGC_SUSET, 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("pglogical.synchronous_commit",
							 "pglogical specific synchronous commit value",
							 NULL,
							 &pglogical_synchronous_commit,
							 false, PGC_POSTMASTER,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("pglogical.use_spi",
							 "Use SPI instead of low-level API for applying changes",
							 NULL,
							 &pglogical_use_spi,
#ifdef XCP
							 true,
#else
							 false,
#endif
							 PGC_POSTMASTER,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("pglogical.batch_inserts",
							 "Batch inserts if possible",
							 NULL,
							 &pglogical_batch_inserts,
							 true,
							 PGC_POSTMASTER,
							 0,
							 NULL, NULL, NULL);

	/*
	 * We can't use the temp_tablespace safely for our dumps, because Pg's
	 * crash recovery is very careful to delete only particularly formatted
	 * files. Instead for now just allow user to specify dump storage.
	 */
	DefineCustomStringVariable("pglogical.temp_directory",
							   "Directory to store dumps for local restore",
							   NULL,
							   &pglogical_temp_directory_config,
							   "", PGC_SIGHUP,
							   0,
							   NULL,
							   pglogical_temp_directory_assing_hook,
							   NULL);

	DefineCustomStringVariable("pglogical.extra_connection_options",
							   "connection options to add to all peer node connections",
							   NULL,
							   &pglogical_extra_connection_options,
							   "",
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	if (IsBinaryUpgrade)
		return;

	/* Init workers. */
	pglogical_worker_shmem_init();

	/* Init executor module */
	pglogical_executor_init();

	/* Run the supervisor. */
	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags =	BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN,
			 EXTENSION_NAME);
	snprintf(bgw.bgw_function_name, BGW_MAXLEN,
			 "pglogical_supervisor_main");
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "pglogical supervisor");
	bgw.bgw_restart_time = 5;

	RegisterBackgroundWorker(&bgw);
}
