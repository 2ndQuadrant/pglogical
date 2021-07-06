/*-------------------------------------------------------------------------
 *
 * pglogical_sync.c
 *		table synchronization functions
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *				pglogical_sync.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#ifdef WIN32
#include <process.h>
#else
#include <sys/wait.h>
#endif

#include "libpq-fe.h"

#include "miscadmin.h"

#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/skey.h"
#include "access/stratnum.h"
#include "access/xact.h"

#include "catalog/indexing.h"
#include "catalog/namespace.h"

#include "commands/dbcommands.h"
#include "commands/tablecmds.h"

#include "lib/stringinfo.h"

#include "utils/memutils.h"

#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"

#include "pgstat.h"

#include "replication/origin.h"

#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/proc.h"

#include "tcop/utility.h"

#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/resowner.h"

#include "pglogical_relcache.h"
#include "pglogical_repset.h"
#include "pglogical_rpc.h"
#include "pglogical_sync.h"
#include "pglogical_worker.h"
#include "pglogical.h"

#define CATALOG_LOCAL_SYNC_STATUS	"local_sync_status"

#if PG_VERSION_NUM < 90500
#define PGDUMP_BINARY "pglogical_dump"
#else
#define PGDUMP_BINARY "pg_dump"
#endif
#define PGRESTORE_BINARY "pg_restore"

#define Natts_local_sync_state	6
#define Anum_sync_kind			1
#define Anum_sync_subid			2
#define Anum_sync_nspname		3
#define Anum_sync_relname		4
#define Anum_sync_status		5
#define Anum_sync_statuslsn		6

void pglogical_sync_main(Datum main_arg);

static PGLogicalSyncWorker	   *MySyncWorker = NULL;

#ifdef WIN32
static int exec_cmd_win32(const char *cmd, char *cmdargv[]);
#endif


/*
 * Run a command and wait for it to exit, then return its exit code
 * in the same format as waitpid() including on Windows.
 *
 * Does not elog(ERROR).
 *
 * 'cmd' must be a full relative or absolute path to the executable to
 * start. The PATH is not searched.
 *
 * Preserves each argument in cmdargv as a discrete argument to the child
 * process. The first entry in cmdargv is passed as the child process's
 * argv[0], so the first "real" argument begins at index 1.
 *
 * Uses the current environment and working directory.
 *
 * Note that if we elog(ERROR) or elog(FATAL) here we won't kill the
 * child proc.
 */
static int
exec_cmd(const char *cmd, char *cmdargv[])
{
	pid_t		pid;
	int			stat;

	/* Fire off execv in child */
	fflush(stdout);
	fflush(stderr);

#ifndef WIN32
	if ((pid = fork()) == 0)
	{
		if (execv(cmd, cmdargv) < 0)
		{
			ereport(ERROR,
					(errmsg("could not execute \"%s\": %m", cmd)));
			/* We're already in the child process here, can't return */
			exit(1);
		}
	}

	if (waitpid(pid, &stat, 0) != pid)
		stat = -1;
#else
	stat = exec_cmd_win32(cmd, cmdargv);
#endif

	return stat;
}


static void
get_pg_executable(char *cmdname, char *cmdbuf)
{
	uint32		version;

	if (find_other_exec_version(my_exec_path, cmdname, &version, cmdbuf))
		elog(ERROR, "pglogical subscriber init failed to find %s relative to binary %s",
			 cmdname, my_exec_path);

	if (version / 100 != PG_VERSION_NUM / 100)
		elog(ERROR, "pglogical subscriber init found %s with wrong major version %d.%d, expected %d.%d",
			 cmdname, version / 100 / 100, version / 100 % 100,
			 PG_VERSION_NUM / 100 / 100, PG_VERSION_NUM / 100 % 100);
}

static void
dump_structure(PGLogicalSubscription *sub, const char *destfile,
			   const char *snapshot)
{
	char	   *dsn;
	char	   *err_msg;
	char		pg_dump[MAXPGPATH];
	char	   *cmdargv[20];
	int			cmdargc = 0;
	bool		has_pgl_origin;
	StringInfoData	s;

	dsn = pgl_get_connstr((char *) sub->origin_if->dsn, NULL, NULL, &err_msg);
	if (dsn == NULL)
		elog(ERROR, "invalid connection string \"%s\": %s",
			 sub->origin_if->dsn, err_msg);

	get_pg_executable(PGDUMP_BINARY, pg_dump);

	cmdargv[cmdargc++] = pg_dump;

	/* custom format */
	cmdargv[cmdargc++] = "-Fc";

	/* schema only */
	cmdargv[cmdargc++] = "-s";

	/* snapshot */
	initStringInfo(&s);
	appendStringInfo(&s, "--snapshot=%s", snapshot);
	cmdargv[cmdargc++] = pstrdup(s.data);
	resetStringInfo(&s);

	/* Dumping database, filter out our extension. */
	appendStringInfo(&s, "--exclude-schema=%s", EXTENSION_NAME);
	cmdargv[cmdargc++] = pstrdup(s.data);
	resetStringInfo(&s);

	/* Skip the pglogical_origin if it exists locally. */
	StartTransactionCommand();
	has_pgl_origin = OidIsValid(LookupExplicitNamespace("pglogical_origin",
														true));
	CommitTransactionCommand();
	if (has_pgl_origin)
	{
		appendStringInfo(&s, "--exclude-schema=%s", "pglogical_origin");
		cmdargv[cmdargc++] = pstrdup(s.data);
		resetStringInfo(&s);
	}

	/* destination file */
	appendStringInfo(&s, "--file=%s", destfile);
	cmdargv[cmdargc++] = pstrdup(s.data);
	resetStringInfo(&s);

	/* connection string */
	appendStringInfo(&s, "--dbname=%s", dsn);
	cmdargv[cmdargc++] = pstrdup(s.data);
	resetStringInfo(&s);
	free(dsn);

	cmdargv[cmdargc++] = NULL;

	if (exec_cmd(pg_dump, cmdargv) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not execute pg_dump (\"%s\"): %m",
						pg_dump)));
}

static void
restore_structure(PGLogicalSubscription *sub, const char *srcfile,
				  const char *section)
{
	char	   *dsn;
	char	   *err_msg;
	char		pg_restore[MAXPGPATH];
	char	   *cmdargv[20];
	int			cmdargc = 0;
	StringInfoData	s;

	dsn = pgl_get_connstr((char *) sub->target_if->dsn, NULL,
						  "-cpglogical.subscription_schema_restore=true",
						  &err_msg);
	if (dsn == NULL)
		elog(ERROR, "invalid connection string \"%s\": %s",
			 sub->target_if->dsn, err_msg);

	get_pg_executable(PGRESTORE_BINARY, pg_restore);

	cmdargv[cmdargc++] = pg_restore;

	/* section */
	if (section)
	{
		initStringInfo(&s);
		appendStringInfo(&s, "--section=%s", section);
		cmdargv[cmdargc++] = pstrdup(s.data);
		resetStringInfo(&s);
	}

	/* stop execution on any error */
	cmdargv[cmdargc++] = "--exit-on-error";

	/* apply everything in single tx */
	cmdargv[cmdargc++] = "-1";

	/* connection string */
	initStringInfo(&s);
	appendStringInfo(&s, "--dbname=%s", dsn);
	cmdargv[cmdargc++] = pstrdup(s.data);
	free(dsn);

	/* source file */
	cmdargv[cmdargc++] = pstrdup(srcfile);

	cmdargv[cmdargc++] = NULL;

	if (exec_cmd(pg_restore, cmdargv) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not execute pg_restore (\"%s\"): %m",
						pg_restore)));
}

/*
 * Create slot and get the exported snapshot.
 *
 * This will try to recreate slot if already exists and not active.
 *
 * The reported LSN is the confirmed flush LSN at the point the slot reached
 * consistency and exported its snapshot.
 */
static char *
ensure_replication_slot_snapshot(PGconn *sql_conn, PGconn *repl_conn,
								 char *slot_name, bool use_failover_slot,
								 XLogRecPtr *lsn)
{
	PGresult	   *res;
	StringInfoData	query;
	char		   *snapshot;

retry:
	initStringInfo(&query);

	appendStringInfo(&query, "CREATE_REPLICATION_SLOT \"%s\" LOGICAL %s%s",
					 slot_name, "pglogical_output",
					 use_failover_slot ? " FAILOVER" : "");


	res = PQexec(repl_conn, query.data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);

		/*
		 * If our slot already exist but is not used, it's leftover from
		 * previous unsucessful attempt to synchronize table, try dropping
		 * it and recreating.
		 */
		if (sqlstate &&
			strcmp(sqlstate, "42710" /*ERRCODE_DUPLICATE_OBJECT*/) == 0 &&
			!pglogical_remote_slot_active(sql_conn, slot_name))
		{
			pfree(query.data);
			PQclear(res);

			pglogical_drop_remote_slot(sql_conn, slot_name);

			goto retry;
		}

		elog(ERROR, "could not create replication slot on provider: %s\n",
			 PQresultErrorMessage(res));
	}

	*lsn = DatumGetLSN(DirectFunctionCall1Coll(pg_lsn_in, InvalidOid,
					  CStringGetDatum(PQgetvalue(res, 0, 1))));
	snapshot = pstrdup(PQgetvalue(res, 0, 2));

	PQclear(res);

	return snapshot;
}

/*
 * Get or create replication origin for a given slot.
 */
static RepOriginId
ensure_replication_origin(char *slot_name)
{
	RepOriginId origin = replorigin_by_name(slot_name, true);

	if (origin == InvalidRepOriginId)
		origin = replorigin_create(slot_name);

	return origin;
}


/*
 * Transaction management for COPY.
 */
static void
start_copy_origin_tx(PGconn *conn, const char *snapshot)
{
	PGresult	   *res;
	char		   *s;
	const char	   *setup_query =
		"BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;\n"
		"SET DATESTYLE = ISO;\n"
		"SET INTERVALSTYLE = POSTGRES;\n"
		"SET extra_float_digits TO 3;\n"
		"SET statement_timeout = 0;\n"
		"SET lock_timeout = 0;\n";
	StringInfoData	query;

	initStringInfo(&query);
	appendStringInfoString(&query, setup_query);

	if (snapshot)
	{
		s = PQescapeLiteral(conn, snapshot, strlen(snapshot));
		appendStringInfo(&query, "SET TRANSACTION SNAPSHOT %s;\n", s);
	}

	res = PQexec(conn, query.data);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		elog(ERROR, "BEGIN on origin node failed: %s",
				PQresultErrorMessage(res));
	PQclear(res);
}

static void
start_copy_target_tx(PGconn *conn, const char *origin_name)
{
	PGresult	   *res;
	char		   *s;
	const char	   *setup_query =
		"BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;\n"
		"SET session_replication_role = 'replica';\n"
		"SET DATESTYLE = ISO;\n"
		"SET INTERVALSTYLE = POSTGRES;\n"
		"SET extra_float_digits TO 3;\n"
		"SET statement_timeout = 0;\n"
		"SET lock_timeout = 0;\n";
	StringInfoData	query;

	initStringInfo(&query);

	/*
	 * Set correct origin if target db supports it.
	 * We must do this before starting the transaction otherwise the status
	 * code bellow would get much more complicated.
	 */
	if (PQserverVersion(conn) >= 90500)
	{
		s = PQescapeLiteral(conn, origin_name, strlen(origin_name));
		appendStringInfo(&query,
						 "SELECT pg_catalog.pg_replication_origin_session_setup(%s);\n",
						 s);
		PQfreemem(s);
	}

	appendStringInfoString(&query, setup_query);

	res = PQexec(conn, query.data);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		elog(ERROR, "BEGIN on target node failed: %s",
				PQresultErrorMessage(res));
	PQclear(res);
}

static void
finish_copy_origin_tx(PGconn *conn)
{
	PGresult   *res;

	/* Close the  transaction and connection on origin node. */
	res = PQexec(conn, "ROLLBACK");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		elog(WARNING, "ROLLBACK on origin node failed: %s",
				PQresultErrorMessage(res));
	PQclear(res);
	PQfinish(conn);
}

static void
finish_copy_target_tx(PGconn *conn)
{
	PGresult   *res;

	/* Close the transaction and connection on target node. */
	res = PQexec(conn, "COMMIT");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		elog(ERROR, "COMMIT on target node failed: %s",
				PQresultErrorMessage(res));
	PQclear(res);

	/*
	 * Resetting the origin explicitly before the backend exits will help
	 * prevent races with other accesses to the same replication origin.
	 */
	if (PQserverVersion(conn) >= 90500)
	{
		res = PQexec(conn, "SELECT pg_catalog.pg_replication_origin_session_reset();\n");
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			elog(WARNING, "Resetting session origin on target node failed: %s",
					PQresultErrorMessage(res));
		PQclear(res);
	}

	PQfinish(conn);
}


static int
physatt_in_attmap(PGLogicalRelation *rel, int attid)
{
	AttrNumber	i;

	for (i = 0; i < rel->natts; i++)
		if (rel->attmap[i] == attid)
			return i;

	return -1;
}

/*
 * Create list of columns for COPY based on logical relation mapping.
 */
static List *
make_copy_attnamelist(PGLogicalRelation *rel)
{
	List	   *attnamelist = NIL;
	TupleDesc	desc = RelationGetDescr(rel->rel);
	int			attnum;

	for (attnum = 0; attnum < desc->natts; attnum++)
	{
		int		remoteattnum = physatt_in_attmap(rel, attnum);

		/* Skip dropped attributes. */
		if (TupleDescAttr(desc,attnum)->attisdropped)
			continue;

		if (remoteattnum < 0)
			continue;

		attnamelist = lappend(attnamelist,
							  makeString(rel->attnames[remoteattnum]));
	}

	return attnamelist;
}

/*
 * COPY single table over wire.
 */
static void
copy_table_data(PGconn *origin_conn, PGconn *target_conn,
				PGLogicalRemoteRel *remoterel, List *replication_sets)
{
	PGLogicalRelation *rel;
	PGresult   *res;
	int			bytes;
	char	   *copybuf;
	List	   *attnamelist;
	ListCell   *lc;
	bool		first;
	StringInfoData	query;
	StringInfoData	attlist;
	MemoryContext	curctx = CurrentMemoryContext,
					oldctx;

	/* Build the relation map. */
	StartTransactionCommand();
	oldctx = MemoryContextSwitchTo(curctx);
	pglogical_relation_cache_updater(remoterel);
	rel = pglogical_relation_open(remoterel->relid, AccessShareLock);
	attnamelist = make_copy_attnamelist(rel);

	initStringInfo(&attlist);
	first = true;
	foreach (lc, attnamelist)
	{
		char *attname = strVal(lfirst(lc));
		if (first)
			first = false;
		else
			appendStringInfoString(&attlist, ",");
		appendStringInfoString(&attlist,
							   PQescapeIdentifier(origin_conn, attname,
												  strlen(attname)));
	}
	MemoryContextSwitchTo(oldctx);
	pglogical_relation_close(rel, AccessShareLock);
	CommitTransactionCommand();

	/* Build COPY TO query. */
	initStringInfo(&query);
	appendStringInfoString(&query, "COPY ");

	/*
	 * If the table is row-filtered we need to run query over the table
	 * to execute the filter.
	 */
	if (remoterel->hasRowFilter)
	{
		StringInfoData	relname;
		StringInfoData	repsetarr;
		ListCell   *lc;

		initStringInfo(&relname);
		appendStringInfo(&relname, "%s.%s",
						 PQescapeIdentifier(origin_conn, remoterel->nspname,
											strlen(remoterel->nspname)),
						 PQescapeIdentifier(origin_conn, remoterel->relname,
											strlen(remoterel->relname)));

		initStringInfo(&repsetarr);
		first = true;
		foreach (lc, replication_sets)
		{
			char	   *repset_name = lfirst(lc);

			if (first)
				first = false;
			else
				appendStringInfoChar(&repsetarr, ',');

			appendStringInfo(&repsetarr, "%s",
							 PQescapeLiteral(origin_conn, repset_name,
											 strlen(repset_name)));
		}

		appendStringInfo(&query,
						 "(SELECT %s FROM pglogical.table_data_filtered(NULL::%s, %s::regclass, ARRAY[%s])) ",
						 list_length(attnamelist) ? attlist.data : "*",
						 relname.data,
						 PQescapeLiteral(origin_conn, relname.data, relname.len),
						 repsetarr.data);
	}
	else
	{
		/* Otherwise just copy the table. */
		appendStringInfo(&query, "%s.%s ",
						 PQescapeIdentifier(origin_conn, remoterel->nspname,
											strlen(remoterel->nspname)),
						 PQescapeIdentifier(origin_conn, remoterel->relname,
											strlen(remoterel->relname)));

		if (list_length(attnamelist))
			appendStringInfo(&query, "(%s) ", attlist.data);
	}
	appendStringInfoString(&query, "TO stdout");


	/* Execute COPY TO. */
	res = PQexec(origin_conn, query.data);
	if (PQresultStatus(res) != PGRES_COPY_OUT)
	{
		ereport(ERROR,
				(errmsg("table copy failed"),
				 errdetail("Query '%s': %s", query.data,
					 PQerrorMessage(origin_conn))));
	}

	/* Build COPY FROM query. */
	resetStringInfo(&query);
	appendStringInfo(&query, "COPY %s.%s ",
					 PQescapeIdentifier(origin_conn, remoterel->nspname,
										strlen(remoterel->nspname)),
					 PQescapeIdentifier(origin_conn, remoterel->relname,
										strlen(remoterel->relname)));
	if (list_length(attnamelist))
		appendStringInfo(&query, "(%s) ", attlist.data);
	appendStringInfoString(&query, "FROM stdin");

	/* Execute COPY FROM. */
	res = PQexec(target_conn, query.data);
	if (PQresultStatus(res) != PGRES_COPY_IN)
	{
		ereport(ERROR,
				(errmsg("table copy failed"),
				 errdetail("Query '%s': %s", query.data,
					 PQerrorMessage(origin_conn))));
	}

	while ((bytes = PQgetCopyData(origin_conn, &copybuf, false)) > 0)
	{
		if (PQputCopyData(target_conn, copybuf, bytes) != 1)
		{
			ereport(ERROR,
					(errmsg("writing to target table failed"),
					 errdetail("destination connection reported: %s",
						 PQerrorMessage(target_conn))));
		}
		PQfreemem(copybuf);

		CHECK_FOR_INTERRUPTS();
	}

	if (bytes != -1)
	{
		ereport(ERROR,
				(errmsg("reading from origin table failed"),
				 errdetail("source connection returned %d: %s",
					bytes, PQerrorMessage(origin_conn))));
	}

	/* Send local finish */
	if (PQputCopyEnd(target_conn, NULL) != 1)
	{
		ereport(ERROR,
				(errmsg("sending copy-completion to destination connection failed"),
				 errdetail("destination connection reported: %s",
					 PQerrorMessage(target_conn))));
	}

	PQclear(res);

	elog(INFO, "finished synchronization of data for table %s.%s",
		 remoterel->nspname, remoterel->relname);
}

/*
 * Copy data from origin node to target node.
 *
 * Creates new connection to origin and target.
 */
static void
copy_tables_data(char *sub_name, const char *origin_dsn,
				 const char *target_dsn, const char *origin_snapshot,
				 List *tables, List *replication_sets,
				 const char *origin_name)
{
	PGconn	   *origin_conn;
	PGconn	   *target_conn;
	ListCell   *lc;

	/* Connect to origin node. */
	origin_conn = pglogical_connect(origin_dsn, sub_name, "copy");
	start_copy_origin_tx(origin_conn, origin_snapshot);

	/* Connect to target node. */
	target_conn = pglogical_connect(target_dsn, sub_name, "copy");
	start_copy_target_tx(target_conn, origin_name);

	/* Copy every table. */
	foreach (lc, tables)
	{
		RangeVar	*rv = lfirst(lc);
		PGLogicalRemoteRel	*remoterel;

		remoterel = pg_logical_get_remote_repset_table(origin_conn, rv,
													   replication_sets);

		copy_table_data(origin_conn, target_conn, remoterel, replication_sets);

		CHECK_FOR_INTERRUPTS();
	}

	/* Finish the transactions and disconnect. */
	finish_copy_origin_tx(origin_conn);
	finish_copy_target_tx(target_conn);
}

/*
 * Copy data from origin node to target node.
 *
 * Creates new connection to origin and target.
 *
 * This is basically same as the copy_tables_data, but it can't be easily
 * merged to single function because we need to get list of tables here after
 * the transaction is bound to a snapshot.
 */
static List *
copy_replication_sets_data(char *sub_name, const char *origin_dsn,
						   const char *target_dsn,
						   const char *origin_snapshot,
						   List *replication_sets, const char *origin_name)
{
	PGconn	   *origin_conn;
	PGconn	   *target_conn;
	List	   *tables;
	ListCell   *lc;

	/* Connect to origin node. */
	origin_conn = pglogical_connect(origin_dsn, sub_name, "copy");
	start_copy_origin_tx(origin_conn, origin_snapshot);

	/* Get tables to copy from origin node. */
	tables = pg_logical_get_remote_repset_tables(origin_conn,
												 replication_sets);

	/* Connect to target node. */
	target_conn = pglogical_connect(target_dsn, sub_name, "copy");
	start_copy_target_tx(target_conn, origin_name);

	/* Copy every table. */
	foreach (lc, tables)
	{
		PGLogicalRemoteRel	*remoterel = lfirst(lc);

		copy_table_data(origin_conn, target_conn, remoterel, replication_sets);

		CHECK_FOR_INTERRUPTS();
	}

	/* Finish the transactions and disconnect. */
	finish_copy_origin_tx(origin_conn);
	finish_copy_target_tx(target_conn);

	return tables;
}

static void
pglogical_sync_worker_cleanup(PGLogicalSubscription *sub)
{
	PGconn			   *origin_conn;

	/* Drop the slot on the remote side. */
	origin_conn = pglogical_connect(sub->origin_if->dsn, sub->name,
									"cleanup");
	/* Wait for slot to be free. */
	while (!got_SIGTERM)
	{
		int	rc;

		if (!pglogical_remote_slot_active(origin_conn, sub->slot_name))
			break;

		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   1000L);

        ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}

	pglogical_drop_remote_slot(origin_conn, sub->slot_name);
	PQfinish(origin_conn);

	/* Drop the origin tracking locally. */
	if (replorigin_session_origin != InvalidRepOriginId)
	{
		replorigin_session_reset();
#if PG_VERSION_NUM >= 140000
		replorigin_drop_by_name(sub->slot_name, true, true);
#else
		replorigin_drop(replorigin_session_origin, true);
#endif
		replorigin_session_origin = InvalidRepOriginId;
	}
}

static void
pglogical_sync_worker_cleanup_error_cb(int code, Datum arg)
{
	PGLogicalSubscription  *sub = (PGLogicalSubscription *) DatumGetPointer(arg);
	pglogical_sync_worker_cleanup(sub);
}

static void
pglogical_sync_tmpfile_cleanup_cb(int code, Datum arg)
{
	const char *tmpfile = DatumGetCString(arg);

	if (unlink(tmpfile) != 0 && errno != ENOENT)
		elog(WARNING, "Failed to clean up pglogical temporary dump file \"%s\" on exit/error: %m",
			 tmpfile);
}

void
pglogical_sync_subscription(PGLogicalSubscription *sub)
{
	PGLogicalSyncStatus *sync;
	XLogRecPtr		lsn;
	char			status;
	MemoryContext	myctx,
					oldctx;

	/* We need our own context for keeping things between transactions. */
	myctx = AllocSetContextCreate(CurrentMemoryContext,
								   "pglogical_sync_subscription cxt",
								   ALLOCSET_DEFAULT_SIZES);

	StartTransactionCommand();
	oldctx = MemoryContextSwitchTo(myctx);
	sync = get_subscription_sync_status(sub->id, false);
	MemoryContextSwitchTo(oldctx);
	CommitTransactionCommand();

	status = sync->status;

	switch (status)
	{
		/* Already synced, nothing to do except cleanup. */
		case SYNC_STATUS_READY:
			MemoryContextDelete(myctx);
			return;
		/* We can recover from crashes during these. */
		case SYNC_STATUS_INIT:
		case SYNC_STATUS_CATCHUP:
			break;
		default:
			elog(ERROR,
				 "subscriber %s initialization failed during nonrecoverable step (%c), please try the setup again",
				 sub->name, status);
			break;
	}

	if (status == SYNC_STATUS_INIT)
	{
		PGconn	   *origin_conn;
		PGconn	   *origin_conn_repl;
		RepOriginId	originid;
		char	   *snapshot;
		bool		use_failover_slot;

		elog(INFO, "initializing subscriber %s", sub->name);

		origin_conn = pglogical_connect(sub->origin_if->dsn,
										sub->name, "snap");

		/* 2QPG9.6 and 2QPG11 support failover slots */
		use_failover_slot =
			pglogical_remote_function_exists(origin_conn, "pg_catalog",
											 "pg_create_logical_replication_slot",
											 -1,
											 "failover");
		origin_conn_repl = pglogical_connect_replica(sub->origin_if->dsn,
													 sub->name, "snap");

		snapshot = ensure_replication_slot_snapshot(origin_conn,
													origin_conn_repl,
													sub->slot_name,
													use_failover_slot, &lsn);

		PQfinish(origin_conn);

		PG_ENSURE_ERROR_CLEANUP(pglogical_sync_worker_cleanup_error_cb,
								PointerGetDatum(sub));
		{
			char	tmpfile[MAXPGPATH];

			snprintf(tmpfile, MAXPGPATH, "%s/pglogical-%d.dump",
					 pglogical_temp_directory, MyProcPid);
			canonicalize_path(tmpfile);

			PG_ENSURE_ERROR_CLEANUP(pglogical_sync_tmpfile_cleanup_cb,
									CStringGetDatum(tmpfile));
			{
#if PG_VERSION_NUM >= 90500
				Relation replorigin_rel;
#endif

				StartTransactionCommand();

				originid = ensure_replication_origin(sub->slot_name);
				elog(DEBUG3, "advancing origin with oid %u for forwarded row to %X/%X during subscription sync",
					originid,
					(uint32)(XactLastCommitEnd>>32), (uint32)XactLastCommitEnd);
#if PG_VERSION_NUM >= 90500
				replorigin_rel = table_open(ReplicationOriginRelationId, RowExclusiveLock);
#endif
				replorigin_advance(originid, lsn, XactLastCommitEnd, true,
								   true);
#if PG_VERSION_NUM >= 90500
				table_close(replorigin_rel, RowExclusiveLock);
#endif

				CommitTransactionCommand();

				if (SyncKindStructure(sync->kind))
				{
					elog(INFO, "synchronizing structure");

					status = SYNC_STATUS_STRUCTURE;
					StartTransactionCommand();
					set_subscription_sync_status(sub->id, status);
					CommitTransactionCommand();

					/* Dump structure to temp storage. */
					dump_structure(sub, tmpfile, snapshot);

					/* Restore base pre-data structure (types, tables, etc). */
					restore_structure(sub, tmpfile, "pre-data");
				}

				/* Copy data. */
				if (SyncKindData(sync->kind))
				{
					List	   *tables;
					ListCell   *lc;

					elog(INFO, "synchronizing data");

					status = SYNC_STATUS_DATA;
					StartTransactionCommand();
					set_subscription_sync_status(sub->id, status);
					CommitTransactionCommand();

					tables = copy_replication_sets_data(sub->name,
														sub->origin_if->dsn,
														sub->target_if->dsn,
														snapshot,
														sub->replication_sets,
														sub->slot_name);

					/* Store info about all the synchronized tables. */
					StartTransactionCommand();
					foreach (lc, tables)
					{
						PGLogicalRemoteRel	   *remoterel = lfirst(lc);
						PGLogicalSyncStatus	   *oldsync;

						oldsync = get_table_sync_status(sub->id,
														remoterel->nspname,
														remoterel->relname, true);
						if (oldsync)
						{
							set_table_sync_status(sub->id, remoterel->nspname,
												  remoterel->relname,
												  SYNC_STATUS_READY,
												  lsn);
						}
						else
						{
							PGLogicalSyncStatus	   newsync;

							newsync.kind = SYNC_KIND_FULL;
							newsync.subid = sub->id;
							namestrcpy(&newsync.nspname, remoterel->nspname);
							namestrcpy(&newsync.relname, remoterel->relname);
							newsync.status = SYNC_STATUS_READY;
							newsync.statuslsn = lsn;
							create_local_sync_status(&newsync);
						}
					}
					CommitTransactionCommand();
				}

				/* Restore post-data structure (indexes, constraints, etc). */
				if (SyncKindStructure(sync->kind))
				{
					elog(INFO, "synchronizing constraints");

					status = SYNC_STATUS_CONSTRAINTS;
					StartTransactionCommand();
					set_subscription_sync_status(sub->id, status);
					CommitTransactionCommand();

					restore_structure(sub, tmpfile, "post-data");
				}
			}
			PG_END_ENSURE_ERROR_CLEANUP(pglogical_sync_tmpfile_cleanup_cb,
										CStringGetDatum(tmpfile));
			pglogical_sync_tmpfile_cleanup_cb(0,
											  CStringGetDatum(tmpfile));
		}
		PG_END_ENSURE_ERROR_CLEANUP(pglogical_sync_worker_cleanup_error_cb,
									PointerGetDatum(sub));

		PQfinish(origin_conn_repl);

		status = SYNC_STATUS_CATCHUP;
		StartTransactionCommand();
		set_subscription_sync_status(sub->id, status);
		CommitTransactionCommand();
	}

	if (status == SYNC_STATUS_CATCHUP)
	{
		/* Nothing to do here yet. */
		status = SYNC_STATUS_READY;
		StartTransactionCommand();
		set_subscription_sync_status(sub->id, status);
		CommitTransactionCommand();

		elog(INFO, "finished synchronization of subscriber %s, ready to enter normal replication", sub->name);
	}

	MemoryContextDelete(myctx);
}

char
pglogical_sync_table(PGLogicalSubscription *sub, RangeVar *table,
					 XLogRecPtr *status_lsn)
{
	PGconn	   *origin_conn_repl, *origin_conn;
	RepOriginId	originid;
	char	   *snapshot;
	PGLogicalSyncStatus	   *sync;

	StartTransactionCommand();

	/* Sanity check. */
	sync = get_subscription_sync_status(sub->id, false);
	if (sync->status != SYNC_STATUS_READY)
	{
		elog(ERROR,
			 "subscriber %s is not ready, cannot synchronzie individual tables", sub->name);
	}

	/* Check current state of the table. */
	sync = get_table_sync_status(sub->id, table->schemaname, table->relname, false);
	*status_lsn = sync->statuslsn;

	/* Already synchronized, nothing to do here. */
	if (sync->status == SYNC_STATUS_READY ||
		sync->status == SYNC_STATUS_SYNCDONE)
		return sync->status;

	/* If previous sync attempt failed, we need to start from beginning. */
	if (sync->status != SYNC_STATUS_INIT)
		set_table_sync_status(sub->id, table->schemaname, table->relname,
							  SYNC_STATUS_INIT, InvalidXLogRecPtr);

	CommitTransactionCommand();

	origin_conn_repl = pglogical_connect_replica(sub->origin_if->dsn,
												 sub->name, "copy");

	origin_conn = pglogical_connect(sub->origin_if->dsn, sub->name, "copy_slot");
	snapshot = ensure_replication_slot_snapshot(origin_conn, origin_conn_repl,
												sub->slot_name, false,
												status_lsn);
	PQfinish(origin_conn);

	/* Make sure we cleanup the slot if something goes wrong. */
	PG_ENSURE_ERROR_CLEANUP(pglogical_sync_worker_cleanup_error_cb,
							PointerGetDatum(sub));
	{
#if PG_VERSION_NUM >= 90500
		Relation replorigin_rel;
#endif

		StartTransactionCommand();
		originid = ensure_replication_origin(sub->slot_name);
		elog(DEBUG2, "advancing origin %s (oid %u) for forwarded row to %X/%X after sync error",
			MySubscription->slot_name, originid,
			(uint32)(XactLastCommitEnd>>32), (uint32)XactLastCommitEnd);

#if PG_VERSION_NUM >= 90500
		replorigin_rel = table_open(ReplicationOriginRelationId, RowExclusiveLock);
#endif
		replorigin_advance(originid, *status_lsn, XactLastCommitEnd, true,
						   true);
#if PG_VERSION_NUM >= 90500
		table_close(replorigin_rel, RowExclusiveLock);
#endif

		set_table_sync_status(sub->id, table->schemaname, table->relname,
							  SYNC_STATUS_DATA, *status_lsn);
		CommitTransactionCommand();

		/* Copy data. */
		copy_tables_data(sub->name, sub->origin_if->dsn,sub->target_if->dsn,
						 snapshot, list_make1(table), sub->replication_sets,
						 sub->slot_name);
	}
	PG_END_ENSURE_ERROR_CLEANUP(pglogical_sync_worker_cleanup_error_cb,
								PointerGetDatum(sub));

	PQfinish(origin_conn_repl);

	return SYNC_STATUS_SYNCWAIT;
}

void
pglogical_sync_worker_finish(void)
{
	PGLogicalWorker	   *apply;

	/*
	 * Commit any outstanding transaction. This is the usual case, unless
	 * there was nothing to do for the table.
	 */
	if (IsTransactionState())
	{
		CommitTransactionCommand();
		pgstat_report_stat(false);
	}

	/* And flush all writes. */
	XLogFlush(GetXLogWriteRecPtr());

	StartTransactionCommand();
	pglogical_sync_worker_cleanup(MySubscription);
	CommitTransactionCommand();

	/*
	 * In case there is apply process running, it might be waiting
	 * for the table status change so tell it to check.
	 */
	LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);
	apply = pglogical_apply_find(MyPGLogicalWorker->dboid,
								 MyApplyWorker->subid);
	if (pglogical_worker_running(apply))
		SetLatch(&apply->proc->procLatch);
	LWLockRelease(PGLogicalCtx->lock);

	elog(LOG, "finished sync of table %s.%s for subscriber %s",
		 NameStr(MySyncWorker->nspname), NameStr(MySyncWorker->relname),
		 MySubscription->name);
}

void
pglogical_sync_main(Datum main_arg)
{
	int				slot = DatumGetInt32(main_arg);
	PGconn		   *streamConn;
	RepOriginId		originid;
	XLogRecPtr		lsn;
	XLogRecPtr		status_lsn;
	StringInfoData	slot_name;
	RangeVar	   *copytable = NULL;
	MemoryContext	saved_ctx;
	char		   *tablename;
	char			status;

	/* Setup shmem. */
	pglogical_worker_attach(slot, PGLOGICAL_WORKER_SYNC);
	MySyncWorker = &MyPGLogicalWorker->worker.sync;
	MyApplyWorker = &MySyncWorker->apply;

	/* Establish signal handlers. */
	pqsignal(SIGTERM, handle_sigterm);

	/* Attach to dsm segment. */
	Assert(CurrentResourceOwner == NULL);
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "pglogical sync");

	/* Setup synchronous commit according to the user's wishes */
	SetConfigOption("synchronous_commit",
					pglogical_synchronous_commit ? "local" : "off",
					PGC_BACKEND, PGC_S_OVERRIDE);	/* other context? */

	/* Run as replica session replication role. */
	SetConfigOption("session_replication_role", "replica",
					PGC_SUSET, PGC_S_OVERRIDE);	/* other context? */

	/*
	 * Disable function body checks during replay. That's necessary because a)
	 * the creator of the function might have had it disabled b) the function
	 * might be search_path dependant and we don't fix the contents of
	 * functions.
	 */
	SetConfigOption("check_function_bodies", "off",
					PGC_INTERNAL, PGC_S_OVERRIDE);

	StartTransactionCommand();
	saved_ctx = MemoryContextSwitchTo(TopMemoryContext);
	MySubscription = get_subscription(MySyncWorker->apply.subid);
	MemoryContextSwitchTo(saved_ctx);
	CommitTransactionCommand();

	copytable = makeRangeVar(NameStr(MySyncWorker->nspname),
							 NameStr(MySyncWorker->relname), -1);

	tablename = quote_qualified_identifier(copytable->schemaname,
										   copytable->relname);

	initStringInfo(&slot_name);
	appendStringInfo(&slot_name, "%s_%08x", MySubscription->slot_name,
					 DatumGetUInt32(hash_any((unsigned char *) tablename,
											 strlen(tablename))));
	MySubscription->slot_name = slot_name.data;

	elog(LOG, "starting sync of table %s.%s for subscriber %s",
		 copytable->schemaname, copytable->relname, MySubscription->name);
	elog(DEBUG1, "connecting to provider %s, dsn %s",
		 MySubscription->origin_if->name, MySubscription->origin_if->dsn);

	/* Do the initial sync first. */
	status = pglogical_sync_table(MySubscription, copytable, &status_lsn);
	if (status == SYNC_STATUS_SYNCDONE || status == SYNC_STATUS_READY)
	{
		pglogical_sync_worker_finish();
		proc_exit(0);
	}

	/* Wait for ack from the main apply thread. */
	StartTransactionCommand();
	set_table_sync_status(MySubscription->id, copytable->schemaname,
						  copytable->relname, SYNC_STATUS_SYNCWAIT,
						  status_lsn);
	CommitTransactionCommand();

	wait_for_sync_status_change(MySubscription->id, copytable->schemaname,
								copytable->relname, SYNC_STATUS_CATCHUP,
								&lsn);
	Assert(lsn == status_lsn);

	/* Setup the origin and get the starting position for the replication. */
	StartTransactionCommand();
	originid = replorigin_by_name(MySubscription->slot_name, false);
	elog(DEBUG2, "setting origin %s (oid %u) for subscription sync",
		MySubscription->slot_name, originid);
	replorigin_session_setup(originid);
	replorigin_session_origin = originid;
	Assert(status_lsn == replorigin_session_get_progress(false));

	/*
	 * In case there is nothing to catchup, finish immediately.
	 * Note pglogical_sync_worker_finish() will commit.
	 */
	if (status_lsn >= MyApplyWorker->replay_stop_lsn)
	{
		/* Mark local table as done. */
		set_table_sync_status(MyApplyWorker->subid,
							  NameStr(MyPGLogicalWorker->worker.sync.nspname),
							  NameStr(MyPGLogicalWorker->worker.sync.relname),
							  SYNC_STATUS_SYNCDONE, status_lsn);
		pglogical_sync_worker_finish();
		proc_exit(0);
	}

	CommitTransactionCommand();

	/* Start the replication. */
	streamConn = pglogical_connect_replica(MySubscription->origin_if->dsn,
										   MySubscription->name, "catchup");

	/*
	 * IDENTIFY_SYSTEM sets up some internal state on walsender so call it even
	 * if we don't (yet) want to use any of the results.
     */
	pglogical_identify_system(streamConn, NULL, NULL, NULL, NULL);

	pglogical_start_replication(streamConn, MySubscription->slot_name,
								status_lsn, "all", NULL, tablename,
								MySubscription->force_text_transfer);

	/* Leave it to standard apply code to do the replication. */
	apply_work(streamConn);

	PQfinish(streamConn);

	/*
	 * We should only get here if we received sigTERM, which in case of
	 * sync worker is not expected.
	 */
	proc_exit(1);
}


/* Catalog access */

/* Create subscription sync status record in catalog. */
void
create_local_sync_status(PGLogicalSyncStatus *sync)
{
	RangeVar   *rv;
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_local_sync_state];
	bool		nulls[Natts_local_sync_state];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_LOCAL_SYNC_STATUS, -1);
	rel = table_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* Form a tuple. */
	memset(nulls, false, sizeof(nulls));

	values[Anum_sync_kind - 1] = CharGetDatum(sync->kind);
	values[Anum_sync_subid - 1] = ObjectIdGetDatum(sync->subid);

	if (sync->nspname.data[0])
		values[Anum_sync_nspname - 1] = NameGetDatum(&sync->nspname);
	else
		nulls[Anum_sync_nspname - 1] = true;

	if (sync->relname.data[0])
		values[Anum_sync_relname - 1] = NameGetDatum(&sync->relname);
	else
		nulls[Anum_sync_relname - 1] = true;

	values[Anum_sync_status - 1] = CharGetDatum(sync->status);
	values[Anum_sync_statuslsn - 1] = LSNGetDatum(sync->statuslsn);

	tup = heap_form_tuple(tupDesc, values, nulls);

	/* Insert the tuple to the catalog. */
	CatalogTupleInsert(rel, tup);

	/* Cleanup. */
	heap_freetuple(tup);
	table_close(rel, RowExclusiveLock);
}

/* Remove subscription sync status record from catalog. */
void
drop_subscription_sync_status(Oid subid)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_LOCAL_SYNC_STATUS, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_sync_subid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(subid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);

	/* Remove the tuples. */
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
		simple_heap_delete(rel, &tuple->t_self);

	/* Cleanup. */
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);
}

static PGLogicalSyncStatus *
syncstatus_fromtuple(HeapTuple tuple, TupleDesc desc)
{
	PGLogicalSyncStatus	   *sync;
	Datum					d;
	bool					isnull;

	sync = (PGLogicalSyncStatus *) palloc0(sizeof(PGLogicalSyncStatus));

	d = fastgetattr(tuple, Anum_sync_kind, desc, &isnull);
	Assert(!isnull);
	sync->kind = DatumGetChar(d);

	d = fastgetattr(tuple, Anum_sync_subid, desc, &isnull);
	Assert(!isnull);
	sync->subid = DatumGetObjectId(d);

	d = fastgetattr(tuple, Anum_sync_nspname, desc, &isnull);
	if (!isnull)
		namestrcpy(&sync->nspname, NameStr(*DatumGetName(d)));

	d = fastgetattr(tuple, Anum_sync_relname, desc, &isnull);
	if (!isnull)
		namestrcpy(&sync->relname, NameStr(*DatumGetName(d)));

	d = fastgetattr(tuple, Anum_sync_status, desc, &isnull);
	Assert(!isnull);
	sync->status = DatumGetChar(d);

	d = fastgetattr(tuple, Anum_sync_statuslsn, desc, &isnull);
	Assert(!isnull);
	sync->statuslsn = DatumGetLSN(d);

	return sync;
}

/* Get the sync status for a subscription. */
PGLogicalSyncStatus *
get_subscription_sync_status(Oid subid, bool missing_ok)
{
	PGLogicalSyncStatus	   *sync;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];
	TupleDesc		tupDesc;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_LOCAL_SYNC_STATUS, -1);
	rel = table_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	ScanKeyInit(&key[0],
				Anum_sync_subid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(subid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		if (pgl_heap_attisnull(tuple, Anum_sync_nspname, NULL) &&
			pgl_heap_attisnull(tuple, Anum_sync_relname, NULL))
			break;
	}

	if (!HeapTupleIsValid(tuple))
	{
		if (missing_ok)
		{
			systable_endscan(scan);
			table_close(rel, RowExclusiveLock);
			return NULL;
		}

		elog(ERROR, "subscription %u status not found", subid);
	}

	sync = syncstatus_fromtuple(tuple, tupDesc);

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	return sync;
}

/* Set the sync status for a subscription. */
void
set_subscription_sync_status(Oid subid, char status)
{
	RangeVar	   *rv;
	Relation		rel;
	TupleDesc		tupDesc;
	SysScanDesc		scan;
	HeapTuple		oldtup,
					newtup;
	ScanKeyData		key[1];
	Datum			values[Natts_local_sync_state];
	bool			nulls[Natts_local_sync_state];
	bool			replaces[Natts_local_sync_state];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_LOCAL_SYNC_STATUS, -1);
	rel = table_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	ScanKeyInit(&key[0],
				Anum_sync_subid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(subid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	while (HeapTupleIsValid(oldtup = systable_getnext(scan)))
	{
		if (pgl_heap_attisnull(oldtup, Anum_sync_nspname, NULL) &&
			pgl_heap_attisnull(oldtup, Anum_sync_relname, NULL))
			break;
	}

	if (!HeapTupleIsValid(oldtup))
		elog(ERROR, "subscription %u status not found", subid);

	memset(nulls, false, sizeof(nulls));
	memset(replaces, false, sizeof(replaces));

	values[Anum_sync_status - 1] = CharGetDatum(status);
	replaces[Anum_sync_status - 1] = true;
	values[Anum_sync_statuslsn - 1] = LSNGetDatum(InvalidXLogRecPtr);
	replaces[Anum_sync_statuslsn - 1] = true;

	newtup = heap_modify_tuple(oldtup, tupDesc, values, nulls, replaces);

	/* Update the tuple in catalog. */
	CatalogTupleUpdate(rel, &oldtup->t_self, newtup);

	/* Cleanup. */
	heap_freetuple(newtup);
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);
}

/* Remove table sync status record from catalog. */
void
drop_table_sync_status(const char *nspname, const char *relname)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[2];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_LOCAL_SYNC_STATUS, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_sync_nspname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(nspname));
	ScanKeyInit(&key[1],
				Anum_sync_relname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(relname));

	scan = systable_beginscan(rel, 0, true, NULL, 2, key);

	/* Remove the tuples. */
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
		simple_heap_delete(rel, &tuple->t_self);

	/* Cleanup. */
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);
}

/* Remove table sync status record from catalog. */
void
drop_table_sync_status_for_sub(Oid subid, const char *nspname,
							   const char *relname)
{
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[3];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_LOCAL_SYNC_STATUS, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_sync_subid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(subid));
	ScanKeyInit(&key[1],
				Anum_sync_nspname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(nspname));
	ScanKeyInit(&key[2],
				Anum_sync_relname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(relname));

	scan = systable_beginscan(rel, 0, true, NULL, 3, key);

	/* Remove the tuples. */
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
		simple_heap_delete(rel, &tuple->t_self);

	/* Cleanup. */
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

}

/* Get the sync status for a table. */
PGLogicalSyncStatus *
get_table_sync_status(Oid subid, const char *nspname, const char *relname,
					  bool missing_ok)
{
	PGLogicalSyncStatus	   *sync;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[3];
	TupleDesc		tupDesc;
	Oid				idxoid = InvalidOid;
	List		   *indexes;
	ListCell	   *l;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_LOCAL_SYNC_STATUS, -1);
	rel = table_openrv(rv, RowExclusiveLock);

	/* Find an index we can use to scan this catalog. */
	indexes = RelationGetIndexList(rel);
	foreach (l, indexes)
	{
		Relation	idx = index_open(lfirst_oid(l), AccessShareLock);

		if (idx->rd_index->indkey.values[0] == Anum_sync_subid &&
			idx->rd_index->indkey.values[1] == Anum_sync_nspname &&
			idx->rd_index->indkey.values[2] == Anum_sync_relname)
		{
			idxoid = lfirst_oid(l);
			index_close(idx, AccessShareLock);
			break;
		}
		index_close(idx, AccessShareLock);
	}
	if (!OidIsValid(idxoid))
		elog(ERROR, "could not find index on local_sync_status");
	list_free(indexes);

	tupDesc = RelationGetDescr(rel);

	ScanKeyInit(&key[0],
				Anum_sync_subid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(subid));
	ScanKeyInit(&key[1],
				Anum_sync_nspname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(nspname));
	ScanKeyInit(&key[2],
				Anum_sync_relname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(relname));

	scan = systable_beginscan(rel, idxoid, true, NULL, 3, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
	{
		if (missing_ok)
		{
			systable_endscan(scan);
			table_close(rel, RowExclusiveLock);
			return NULL;
		}

		elog(ERROR, "subscription %u table %s.%s status not found", subid,
			 nspname, relname);
	}

	sync = syncstatus_fromtuple(tuple, tupDesc);

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	return sync;
}

/* Get the sync status for a table. */
List *
get_unsynced_tables(Oid subid)
{
	PGLogicalSyncStatus	   *sync;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];
	List		   *res = NIL;
	TupleDesc		tupDesc;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_LOCAL_SYNC_STATUS, -1);
	rel = table_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	ScanKeyInit(&key[0],
				Anum_sync_subid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(subid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		if (pgl_heap_attisnull(tuple, Anum_sync_nspname, NULL) &&
			pgl_heap_attisnull(tuple, Anum_sync_relname, NULL))
			continue;

		sync = syncstatus_fromtuple(tuple, tupDesc);
		if (sync->status != SYNC_STATUS_READY)
			res = lappend(res, sync);
	}

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	return res;
}

/* Get the sync status for all tables known to subscription. */
List *
get_subscription_tables(Oid subid)
{
	PGLogicalSyncStatus	   *sync;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];
	List		   *res = NIL;
	TupleDesc		tupDesc;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_LOCAL_SYNC_STATUS, -1);
	rel = table_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	ScanKeyInit(&key[0],
				Anum_sync_subid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(subid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		if (pgl_heap_attisnull(tuple, Anum_sync_nspname, NULL) &&
			pgl_heap_attisnull(tuple, Anum_sync_relname, NULL))
			continue;

		sync = syncstatus_fromtuple(tuple, tupDesc);
		res = lappend(res, sync);
	}

	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);

	return res;
}

/* Set the sync status for a table. */
void
set_table_sync_status(Oid subid, const char *nspname, const char *relname,
					  char status, XLogRecPtr statuslsn)
{
	RangeVar	   *rv;
	Relation		rel;
	TupleDesc	tupDesc;
	SysScanDesc		scan;
	HeapTuple		oldtup,
					newtup;
	ScanKeyData		key[3];
	Datum			values[Natts_local_sync_state];
	bool			nulls[Natts_local_sync_state];
	bool			replaces[Natts_local_sync_state];

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_LOCAL_SYNC_STATUS, -1);
	rel = table_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	ScanKeyInit(&key[0],
				Anum_sync_subid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(subid));
	ScanKeyInit(&key[1],
				Anum_sync_nspname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(nspname));
	ScanKeyInit(&key[2],
				Anum_sync_relname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(relname));

	scan = systable_beginscan(rel, 0, true, NULL, 3, key);
	oldtup = systable_getnext(scan);

	if (!HeapTupleIsValid(oldtup))
		elog(ERROR, "subscription %u table %s.%s status not found", subid,
			 nspname, relname);

	memset(nulls, false, sizeof(nulls));
	memset(replaces, false, sizeof(replaces));

	values[Anum_sync_status - 1] = CharGetDatum(status);
	replaces[Anum_sync_status - 1] = true;
	values[Anum_sync_statuslsn - 1] = LSNGetDatum(statuslsn);
	replaces[Anum_sync_statuslsn - 1] = true;

	newtup = heap_modify_tuple(oldtup, tupDesc, values, nulls, replaces);

	/* Update the tuple in catalog. */
	CatalogTupleUpdate(rel, &oldtup->t_self, newtup);

	/* Cleanup. */
	heap_freetuple(newtup);
	systable_endscan(scan);
	table_close(rel, RowExclusiveLock);
}

/*
 * Wait until the table sync status has changed desired one.
 *
 * We also exit if the worker is no longer recognized as sync worker as
 * that means something bad happened to it.
 *
 * Care with allocations is required here since it typically runs
 * in TopMemoryContext.
 */
bool
wait_for_sync_status_change(Oid subid, const char *nspname, const char *relname,
							char desired_state, XLogRecPtr *lsn)
{
	int rc;
	MemoryContext old_ctx = CurrentMemoryContext;
	bool ret = false;

	*lsn = InvalidXLogRecPtr;

	Assert(!IsTransactionState());

	while (!got_SIGTERM)
	{
		PGLogicalWorker		   *worker;
		PGLogicalSyncStatus	   *sync;

		StartTransactionCommand();
		sync = get_table_sync_status(subid, nspname, relname, true);
		if (!sync)
		{
			CommitTransactionCommand();
			break;
		}
		if (sync->status == desired_state)
		{
			*lsn = sync->statuslsn;
			CommitTransactionCommand();
			ret = true;
			break;
		}
		CommitTransactionCommand();
		(void) MemoryContextSwitchTo(old_ctx);

		/* Check if the worker is still alive - no point waiting if it died. */
		LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);
		worker = pglogical_sync_find(MyDatabaseId, subid, nspname, relname);
		LWLockRelease(PGLogicalCtx->lock);
		if (!worker)
			break;

		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   60000L);

        ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}

	(void) MemoryContextSwitchTo(old_ctx);
	return ret;
}

/*
 * Truncates table if it exists.
 */
void
truncate_table(char *nspname, char *relname)
{
	RangeVar	   *rv;
	Oid				relid;
	TruncateStmt   *truncate;
	StringInfoData	sql;

	rv = makeRangeVar(nspname, relname, -1);

	relid = RangeVarGetRelid(rv, AccessExclusiveLock, true);
	if (relid == InvalidOid)
		return;

	initStringInfo(&sql);
	appendStringInfo(&sql, "TRUNCATE TABLE %s",
			quote_qualified_identifier(rv->schemaname, rv->relname));

	/* Truncate the table. */
	truncate = makeNode(TruncateStmt);
	truncate->relations = list_make1(rv);
	truncate->restart_seqs = false;
	truncate->behavior = DROP_RESTRICT;

	/*
	 * We use standard_ProcessUtility to process the truncate statement. This
	 * allows us to let Postgres-XL do the correct things in order to truncate
	 * the table from the remote nodes.
	 *
	 * Except the query string, most other parameters are made-up. This is OK
	 * for TruncateStmt, but if we ever decide to use standard_ProcessUtility
	 * for other utility statements, then we must take a careful relook.
	 */
#ifdef PGXC
	standard_ProcessUtility((Node *)truncate,
			sql.data, PROCESS_UTILITY_TOPLEVEL, NULL, NULL,
			false,
			NULL
			);
#else
	ExecuteTruncate(truncate);
#endif
	/* release memory allocated to create SQL statement */
	pfree(sql.data);

	CommandCounterIncrement();
}


/*
 * exec_cmd support for win32
 */
#ifdef WIN32
/*
 * Return formatted message from GetLastError() in a palloc'd string in the
 * current memory context, or a copy of a constant generic error string if
 * there's no recorded error state.
 */
static char *
PglGetLastWin32Error(void)
{
	LPVOID lpMsgBuf;
	DWORD dw = GetLastError();
	char * pgstr = NULL;

	if (dw != ERROR_SUCCESS)
	{
		FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
				NULL, dw, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPTSTR) &lpMsgBuf, 0, NULL);
		pgstr = pstrdup((LPTSTR) lpMsgBuf);
		LocalFree(lpMsgBuf);
	} else {
		pgstr = pstrdup("Unknown error or no recent error");
	}

	return pgstr;
}


/*
 * See https://docs.microsoft.com/en-us/archive/blogs/twistylittlepassagesallalike/everyone-quotes-command-line-arguments-the-wrong-way
 * for the utterly putrid way Windows handles command line arguments, and the insane lack of any inverse
 * form of the CommandLineToArgvW function in the win32 API.
 */
static void
QuoteWindowsArgvElement(StringInfo cmdline, const char *arg, bool force)
{
	if (!force && *arg != '\0'
			&& strchr(arg, ' ') == NULL
			&& strchr(arg, '\t') == NULL
			&& strchr(arg, '\n') == NULL
			&& strchr(arg, '\v') == NULL
			&& strchr(arg, '"') == NULL)
    {
		appendStringInfoString(cmdline, arg);
    }
    else {
		const char *it;

		/* Begin quoted argument */
		appendStringInfoChar(cmdline, '"');

		/*
		 * In terms of the algorithm described in CommandLineToArgvW's
		 * documentation we are now "in quotes".
		 */

        for (it = arg; *it != '\0'; it++)
		{
            unsigned int NumberBackslashes = 0;

			/*
			 * Accumulate runs of backslashes. They may or may not have special
			 * meaning depending on what follows them.
			 */
            while (*it != '\0' && *it == '\\')
			{
                ++it;
                ++NumberBackslashes;
            }

            if (*it == '\0')
			{
				/*
				 * Handle command line arguments ending with or consisting only
				 * of backslashes.  Particularly important for Windows, given
				 * its backslash paths.
				 *
				 * We want NumberBackSlashes * 2 backslashes here to prevent the
				 * final backslash from escaping the quote we'll append at the
				 * end of the argument.
				 */
				for (; NumberBackslashes > 0; NumberBackslashes--)
					appendStringInfoString(cmdline, "\\\\");
				break;
            }
            else if (*it == '"') {
				/*
				 * Escape all accumulated backslashes, then append escaped
				 * quotation mark.
				 *
				 * We want NumberBackSlashes * 2 + 1 backslashes to prevent
				 * the backslashes from escaping the backslash we have to append
				 * to escape the quote char that's part of the argument itself.
				 */
				for (; NumberBackslashes > 0; NumberBackslashes--)
					appendStringInfoString(cmdline, "\\\\");
                appendStringInfoString(cmdline, "\\\"");
			}
            else {
				/*
				 * A series of backslashes followed by something other than a
				 * double quote is not special to the CommandLineToArgvW parser
				 * in MSVCRT and must be appended literally.
				 */
				for (; NumberBackslashes > 0; NumberBackslashes--)
					appendStringInfoChar(cmdline, '\\');
				/* Finally any normal char */
                appendStringInfoChar(cmdline, *it);
            }
        }

		/* End quoted argument */
		appendStringInfoChar(cmdline, '"');

		/*
		 * In terms of the algorithm described in CommandLineToArgvW's
		 * documentation we are now "not in quotes".
		 */
    }
}

/*
 * Turn an execv-style argument vector into something that Win32's
 * CommandLineToArgvW will parse back into the original argument
 * vector.
 *
 * You'd think this would be part of the win32 API. But no...
 *
 * (This should arguably be part of libpq_fe.c, but I didn't want to expand our
 * abuse of PqExpBuffer.)
 */
static void
QuoteWindowsArgv(StringInfo cmdline, const char * argv[])
{
	/* argv0 is required */
	Assert(*argv != NULL && **argv != '\0');
	QuoteWindowsArgvElement(cmdline, *argv, false);
	++argv;

	for (; *argv != NULL; ++argv)
	{
		appendStringInfoChar(cmdline, ' ');
		QuoteWindowsArgvElement(cmdline, *argv, false);
	}
}

/*
 * Run a process on Windows and wait for it to exit, then return its exit code.
 * Preserve argument quoting. See exec_cmd() for the function contract details.
 * This is only split out to keep all the win32 horror separate for reability.
 *
 * Don't be tempted to use Win32's _spawnv. It is not like execv. It does *not*
 * preserve the individual arguments in the vector, it concatenates them
 * without any escaping or quoting. Thus any arguments with spaces, double
 * quotes, etc will be mangled by the child process's MSVC runtime when it
 * tries to turn the argument string back into an argument vector for the main
 * function by calling CommandLineToArgv() from the C library entrypoint.
 * _spawnv is also limited to 1024 characters not the 32767 characters permited
 * by the underlying Win32 APIs, and that could matter for pg_dump.
 *
 * This provides something more like we'e expect from execv and waitpid()
 * including a waitpid()-style return code with the exit code in the high
 * 8 bits of a 16 bit value. Use WEXITSTATUS() for the exit status. The
 * special value -1 is returned for a failure to launch the process,
 * wait for it, or get its exit code.
 */
static int
exec_cmd_win32(const char *cmd, char *cmdargv[])
{
	BOOL					ret;
	int						exitcode = -1;
	PROCESS_INFORMATION 	pi;

	elog(DEBUG1, "trying to launch \"%s\"", cmd);

	/* Launch the process */
	{
		STARTUPINFO 			si;
		StringInfoData 			cmdline;
		char 				   *cmd_tmp;

		/* Deal with insane windows command line quoting */
		initStringInfo(&cmdline);
		QuoteWindowsArgv(&cmdline, cmdargv);

		/* CreateProcess may scribble on the cmd string */
		cmd_tmp = pstrdup(cmd);

		/*
		 * STARTUPINFO contains various extra options for the process that are
		 * not passed as CreateProcess flags, and is required.
		 */
		ZeroMemory( &si, sizeof(si) );
		si.cb = sizeof(si);

		/*
		 * PROCESS_INFORMATION accepts the returned process handle.
		 */
		ZeroMemory( &pi, sizeof(pi) );
		ret = CreateProcess(cmd_tmp, cmdline.data,
				NULL /* default process attributes */,
				NULL /* default thread attributes */,
				TRUE /* handles (fds) are inherited, to match execv */,
				CREATE_NO_WINDOW    /* process creation flags */,
				NULL /* inherit environment variables */,
				NULL /* inherit working directory */,
				&si,
				&pi);

		pfree(cmd_tmp);
		pfree(cmdline.data);
	}

	if (!ret)
	{
		char *winerr = PglGetLastWin32Error();
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("failed to launch \"%s\": %s",
					 	cmd, winerr)));
		pfree(winerr);
	}
	else
	{
		/*
		 * Process created. It can still fail due to DLL linkage errors,
		 * startup problems etc, but the handle exists.
		 *
		 * Wait for it to exit, while responding to interrupts. Ideally we
		 * should be able to use WaitEventSetWait here since Windows sees a
		 * process handle much like a socket, but the Pg API for it won't
		 * let us, so we have to DIY.
		 */

		elog(DEBUG1, "process launched, waiting");

		do {
			ret = WaitForSingleObject( pi.hProcess, 500 /* timeout in ms */ );

			/*
			 * Note that if we elog(ERROR) or elog(FATAL) as a result of a
			 * signal here we won't kill the child proc.
			 */
			CHECK_FOR_INTERRUPTS();

			if (ret == WAIT_TIMEOUT)
				continue;

			if (ret != WAIT_OBJECT_0)
			{
				char *winerr = PglGetLastWin32Error();
				ereport(DEBUG1,
						(errcode_for_file_access(),
						 errmsg("unexpected WaitForSingleObject() return code %d while waiting for child process \"%s\": %s",
							 ret, cmd, winerr)));
				pfree(winerr);
				/* Try to get the exit code anyway */
			}

			if (!GetExitCodeProcess( pi.hProcess, &exitcode))
			{
				char *winerr = PglGetLastWin32Error();
				ereport(DEBUG1,
						(errcode_for_file_access(),
						 errmsg("failed to get exit code from process \"%s\": %s",
								cmd, winerr)));
				pfree(winerr);
				/* Give up on learning about the process's outcome */
				exitcode = -1;
				break;
			}
			else
			{
				/* Woken up for a reason other than child process termination */
				if (exitcode == STILL_ACTIVE)
					continue;

				/*
				 * Process must've exited, so code is a value from ExitProcess,
				 * TerminateProcess, main or WinMain.
				 */
				ereport(DEBUG1,
						(errmsg("process \"%s\" exited with code %d",
								cmd, exitcode)));

				/*
				 * Adapt exit code to WEXITSTATUS form to behave like waitpid().
				 *
				 * The lower 8 bits are the terminating signal, with 0 for no
				 * signal.
				 */
				exitcode = exitcode << 8;

				break;
			}
		} while (true);

		CloseHandle( pi.hProcess );
		CloseHandle( pi.hThread );
	}

	elog(DEBUG1, "exec_cmd_win32 for \"%s\" exiting with %d", cmd, exitcode);
	return exitcode;
}
#endif
