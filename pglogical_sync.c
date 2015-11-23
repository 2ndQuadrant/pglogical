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

#include "libpq-fe.h"

#include "miscadmin.h"

#include "access/xact.h"

#include "commands/dbcommands.h"

#include "lib/stringinfo.h"

#include "utils/memutils.h"

#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"

#include "replication/origin.h"

#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/proc.h"

#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/resowner.h"

#include "pglogical_repset.h"
#include "pglogical_sync.h"
#include "pglogical_worker.h"
#include "pglogical.h"

void pglogical_sync_main(Datum main_arg);

static PGLogicalSyncWorker	   *MySyncWorker = NULL;


static void
dump_structure(PGLogicalSubscriber *sub, const char *snapshot)
{
	char		pg_dump[MAXPGPATH];
	uint32		version;
	int			res;
	StringInfoData	command;

	if (find_other_exec_version(my_exec_path, "pg_dump", &version, pg_dump))
		elog(ERROR, "pglogical subscriber init failed to find pg_dump relative to binary %s",
			 my_exec_path);

	if (version / 100 != PG_VERSION_NUM / 100)
		elog(ERROR, "pglogical subscriber init found pg_dump with wrong major version %d.%d, expected %d.%d",
			 version / 100 / 100, version / 100 % 100,
			 PG_VERSION_NUM / 100 / 100, PG_VERSION_NUM / 100 % 100);

	initStringInfo(&command);
	appendStringInfo(&command, "%s --snapshot=\"%s\" -s -N %s -F c -f \"%s\" \"%s\"",
					 pg_dump, snapshot, EXTENSION_NAME, "/tmp/pglogical.dump",
					 sub->provider_dsn);

	res = system(command.data);
	if (res != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not execute command \"%s\"",
						command.data)));
}

/* TODO: switch to SPI? */
static void
restore_structure(PGLogicalSubscriber *sub, const char *section)
{
	char		pg_restore[MAXPGPATH];
	uint32		version;
	int			res;
	StringInfoData	command;

	if (find_other_exec_version(my_exec_path, "pg_restore", &version, pg_restore))
		elog(ERROR, "pglogical subscriber init failed to find pg_restore relative to binary %s",
			 my_exec_path);

	if (version / 100 != PG_VERSION_NUM / 100)
		elog(ERROR, "pglogical subscriber init found pg_restore with wrong major version %d.%d, expected %d.%d",
			 version / 100 / 100, version / 100 % 100,
			 PG_VERSION_NUM / 100 / 100, PG_VERSION_NUM / 100 % 100);

	initStringInfo(&command);
	appendStringInfo(&command,
					 "%s --section=\"%s\" --exit-on-error -1 -d \"%s\" \"%s\"",
					 pg_restore, section, sub->local_dsn,
					 "/tmp/pglogical.dump");

	res = system(command.data);
	if (res != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not execute command \"%s\"",
						command.data)));
}


/*
 * Ensure slot exists.
 */
static char *
ensure_replication_slot_snapshot(PGconn *origin_conn, Name slot_name,
								 XLogRecPtr *lsn)
{
	PGresult	   *res;
	StringInfoData	query;
	char		   *snapshot;
	MemoryContext	saved_ctx;

	initStringInfo(&query);

	appendStringInfo(&query, "CREATE_REPLICATION_SLOT \"%s\" LOGICAL %s",
					 NameStr(*slot_name), "pglogical_output");

	res = PQexec(origin_conn, query.data);

	/* TODO: check and handle already existing slot. */
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		elog(FATAL, "could not send replication command \"%s\": status %s: %s\n",
			 query.data,
			 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
	}

	saved_ctx = MemoryContextSwitchTo(TopMemoryContext);
	*lsn = DatumGetLSN(DirectFunctionCall1Coll(pg_lsn_in, InvalidOid,
					  CStringGetDatum(PQgetvalue(res, 0, 1))));
	snapshot = pstrdup(PQgetvalue(res, 0, 2));
	MemoryContextSwitchTo(saved_ctx);

	PQclear(res);

	return snapshot;
}

/*
 * Get or create replication origin for a given slot.
 */
static RepOriginId
ensure_replication_origin(Name slot_name)
{
	RepOriginId origin = replorigin_by_name(NameStr(*slot_name), true);

	if (origin == InvalidRepOriginId)
		origin = replorigin_create(NameStr(*slot_name));

	return origin;
}


/*
 * Transaction management for COPY.
 */
static void
start_copy_origin_tx(PGconn *conn, const char *snapshot)
{
	PGresult	   *res;
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
		appendStringInfo(&query, "SET TRANSACTION SNAPSHOT '%s';\n", snapshot);

	res = PQexec(conn, query.data);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		elog(ERROR, "BEGIN on origin node failed: %s",
				PQresultErrorMessage(res));
	PQclear(res);
}

static void
start_copy_target_tx(PGconn *conn)
{
	PGresult	   *res;
	const char	   *setup_query =
		"BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;\n"
		"SET DATESTYLE = ISO;\n"
		"SET INTERVALSTYLE = POSTGRES;\n"
		"SET extra_float_digits TO 3;\n"
		"SET statement_timeout = 0;\n"
		"SET lock_timeout = 0;\n";

	res = PQexec(conn, setup_query);
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
	PQfinish(conn);
}


/*
 * COPY single table over wire.
 */
static void
copy_table_data(PGconn *origin_conn, PGconn *target_conn,
				const char *schemaname, const char *relname)
{
	PGresult   *res;
	int			bytes;
	char	   *copybuf;
	StringInfoData	query;

	/* Build COPY TO query. */
	initStringInfo(&query);
	appendStringInfo(&query, "COPY %s.%s TO stdout",
					 PQescapeIdentifier(origin_conn, schemaname,
										strlen(schemaname)),
					 PQescapeIdentifier(origin_conn, relname,
										strlen(relname)));

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
	appendStringInfo(&query, "COPY %s.%s FROM stdin",
					 PQescapeIdentifier(origin_conn, schemaname,
										strlen(schemaname)),
					 PQescapeIdentifier(origin_conn, relname,
										strlen(relname)));

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
}

/*
 * Fetch list of tables that are grouped in specified replication sets.
 */
static List *
get_copy_tables(PGconn *origin_conn, List *replication_sets)
{
	PGresult   *res;
	int			i;
	List	   *tables = NIL;
	ListCell   *lc;
	StringInfoData	query;
	StringInfoData	repsetarr;

	initStringInfo(&repsetarr);
	appendStringInfo(&repsetarr, "{");
	foreach (lc, replication_sets)
	{
		PGLogicalRepSet *rs = lfirst(lc);

		appendStringInfo(&repsetarr, "%s", rs->name);
	}
	appendStringInfo(&repsetarr, "}");

	/* Build COPY TO query. */
	initStringInfo(&query);
	appendStringInfo(&query, "SELECT nspname, relname FROM %s.tables WHERE set_name = ANY(%s)",
					 EXTENSION_NAME,
					 PQescapeLiteral(origin_conn, repsetarr.data,
									 repsetarr.len));

	res = PQexec(origin_conn, query.data);
	/* TODO: better error message */
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		elog(ERROR, "could not get table list");

	for (i = 0; i < PQntuples(res); i++)
	{
		RangeVar *rv;

		rv = makeRangeVar(pstrdup(PQgetvalue(res, i, 0)),
						  pstrdup(PQgetvalue(res, i, 1)), -1);

		tables = lappend(tables, rv);
	}

	PQclear(res);

	return tables;
}

/*
 * Copy data from origin node to target node.
 *
 * Creates new connection to origin and target.
 */
static void
copy_tables_data(const char *origin_dsn, const char *target_dsn,
				 const char *origin_snapshot, List *tables)
{
	PGconn	   *origin_conn;
	PGconn	   *target_conn;
	ListCell   *lc;

	/* Connect to origin node. */
	origin_conn = pglogical_connect(origin_dsn, EXTENSION_NAME "_copy");
	start_copy_origin_tx(origin_conn, origin_snapshot);

	/* Connect to target node. */
	target_conn = pglogical_connect(target_dsn, EXTENSION_NAME "_copy");
	start_copy_target_tx(target_conn);

	/* Copy every table. */
	foreach (lc, tables)
	{
		RangeVar	*rv = lfirst(lc);

		copy_table_data(origin_conn, target_conn,
						rv->schemaname, rv->relname);

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
static void
copy_replication_sets_data(const char *origin_dsn, const char *target_dsn,
						   const char *origin_snapshot, List *replication_sets)
{
	PGconn	   *origin_conn;
	PGconn	   *target_conn;
	List	   *tables;
	ListCell   *lc;

	/* Connect to origin node. */
	origin_conn = pglogical_connect(origin_dsn, EXTENSION_NAME "_copy");
	start_copy_origin_tx(origin_conn, origin_snapshot);

	/* Get tables to copy from origin node. */
	tables = get_copy_tables(origin_conn, replication_sets);

	/* Connect to target node. */
	target_conn = pglogical_connect(target_dsn, EXTENSION_NAME "_copy");
	start_copy_target_tx(target_conn);

	/* Copy every table. */
	foreach (lc, tables)
	{
		RangeVar	*rv = lfirst(lc);

		copy_table_data(origin_conn, target_conn,
						rv->schemaname, rv->relname);

		CHECK_FOR_INTERRUPTS();
	}

	/* Finish the transactions and disconnect. */
	finish_copy_origin_tx(origin_conn);
	finish_copy_target_tx(target_conn);
}

void
pglogical_copy_database(PGLogicalSubscriber *sub)
{
	XLogRecPtr	lsn;
	char		status;

	status = sub->status;

	switch (status)
	{
		/* We can recover from crashes during these. */
		case SUBSCRIBER_STATUS_INIT:
		case SUBSCRIBER_STATUS_SYNC_SCHEMA:
		case SUBSCRIBER_STATUS_CATCHUP:
			break;
		default:
			elog(ERROR,
				 "subscriber %s initialization failed during nonrecoverable step (%c), please try the setup again",
				 sub->name, status);
			break;
	}

	if (status == SUBSCRIBER_STATUS_INIT)
	{
		PGconn	   *origin_conn_repl;
		RepOriginId	originid;
		char	   *snapshot;
		NameData	slot_name;

		elog(INFO, "initializing subscriber");

		StartTransactionCommand();

		gen_slot_name(&slot_name, get_database_name(MyDatabaseId),
					  sub->provider_name, sub->name, NULL);

		origin_conn_repl = pglogical_connect_replica(sub->provider_dsn,
													 EXTENSION_NAME "_snapshot");

		snapshot = ensure_replication_slot_snapshot(origin_conn_repl, &slot_name,
													&lsn);
		originid = ensure_replication_origin(&slot_name);
		replorigin_advance(originid, lsn, XactLastCommitEnd, true, true);

		CommitTransactionCommand();

		status = SUBSCRIBER_STATUS_SYNC_SCHEMA;
		set_subscriber_status(sub->id, status);

		elog(INFO, "synchronizing schemas");

		/* Dump structure to temp storage. */
		dump_structure(sub, snapshot);

		/* Restore base pre-data structure (types, tables, etc). */
		restore_structure(sub, "pre-data");

		/* Copy data. */
		copy_replication_sets_data(sub->provider_dsn, sub->local_dsn,
								   snapshot, sub->replication_sets);

		/* Restore post-data structure (indexes, constraints, etc). */
		restore_structure(sub, "post-data");

		PQfinish(origin_conn_repl);

		status = SUBSCRIBER_STATUS_CATCHUP;
		set_subscriber_status(sub->id, status);
	}

	if (status == SUBSCRIBER_STATUS_CATCHUP)
	{
		/* Nothing to do here yet. */
		status = SUBSCRIBER_STATUS_READY;
		set_subscriber_status(sub->id, status);

		elog(INFO, "finished init_replica, ready to enter normal replication");
	}
}


void
pglogical_copy_table(PGLogicalSubscriber *sub, RangeVar *table)
{
	XLogRecPtr	lsn;
	PGconn	   *origin_conn_repl;
	RepOriginId	originid;
	char	   *snapshot;
	NameData	slot_name;

	if (sub->status != SUBSCRIBER_STATUS_READY)
	{
		elog(ERROR,
			 "subscriber %s is not ready, cannot copy tables", sub->name);
	}

	StartTransactionCommand();

	gen_slot_name(&slot_name, get_database_name(MyDatabaseId),
				  sub->provider_name, sub->name, table->relname);

	origin_conn_repl = pglogical_connect_replica(sub->provider_dsn,
												 EXTENSION_NAME "_copy");

	snapshot = ensure_replication_slot_snapshot(origin_conn_repl, &slot_name,
												&lsn);
	originid = ensure_replication_origin(&slot_name);
	replorigin_advance(originid, lsn, XactLastCommitEnd, true, true);

	CommitTransactionCommand();

	/* Copy data. */
	copy_tables_data(sub->provider_dsn, sub->local_dsn, snapshot,
					 list_make1(table));

	PQfinish(origin_conn_repl);
}


void
pglogical_sync_main(Datum main_arg)
{
	int				slot = DatumGetInt32(main_arg);
	PGconn		   *streamConn;
	RepOriginId		originid;
	XLogRecPtr		origin_startpos;
	NameData		slot_name;
	PGLogicalSubscriber	   *sub;
	RangeVar	   *copytable = NULL;
	MemoryContext	saved_ctx;
	char		   *tablename;

	/* Setup shmem. */
	pglogical_worker_attach(slot);
	MySyncWorker = &MyPGLogicalWorker->worker.sync;
	MyApplyWorker = &MySyncWorker->apply;

	/* Establish signal handlers. */
	pqsignal(SIGTERM, handle_sigterm);
	BackgroundWorkerUnblockSignals();

	/* Should never happen. */
	if (MySyncWorker->status == TABLE_SYNC_STATUS_NONE)
		elog(ERROR, "sync worker started without table");

	/* Attach to dsm segment. */
	Assert(CurrentResourceOwner == NULL);
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "pglogical sync");

	/* Connect to our database. */
	BackgroundWorkerInitializeConnectionByOid(MyPGLogicalWorker->dboid, InvalidOid);

	StartTransactionCommand();
	saved_ctx = MemoryContextSwitchTo(TopMemoryContext);
	sub = get_subscriber(MySyncWorker->apply.subscriberid);
	MemoryContextSwitchTo(saved_ctx);
	CommitTransactionCommand();

	copytable = makeRangeVar(NameStr(MySyncWorker->nspname),
							 NameStr(MySyncWorker->relname), -1);

	elog(LOG, "starting sync of table %s.%s for subscriber %s",
		 copytable->schemaname, copytable->relname, sub->name);
	elog(DEBUG1, "conneting to provider %s, dsn %s",
		 sub->provider_name, sub->provider_dsn);

	/* COPY data if requested. */
	if (MySyncWorker->status == TABLE_SYNC_STATUS_INIT ||
		MySyncWorker->status == TABLE_SYNC_STATUS_DATA)
	{
		MySyncWorker->status = TABLE_SYNC_STATUS_DATA;
		pglogical_copy_table(sub, copytable);
	}

	StartTransactionCommand();

	/* Setup the origin and get the starting position for the replication. */
	gen_slot_name(&slot_name, get_database_name(MyDatabaseId),
				  sub->provider_name, sub->name,
				  NameStr(MySyncWorker->relname));
	originid = replorigin_by_name(NameStr(slot_name), false);
	replorigin_session_setup(originid);
	replorigin_session_origin = originid;
	origin_startpos = replorigin_session_get_progress(false);

	CommitTransactionCommand();

	MySyncWorker->apply.replay_stop_lsn = origin_startpos;
	MySyncWorker->status = TABLE_SYNC_STATUS_SYNCWAIT;
	wait_for_sync_status_change(MyPGLogicalWorker, TABLE_SYNC_STATUS_CATCHUP);

	/* Start the replication. */
	streamConn = pglogical_connect_replica(sub->provider_dsn, sub->name);

	tablename = quote_qualified_identifier(copytable->schemaname,
										   copytable->relname);

	pglogical_start_replication(streamConn, NameStr(slot_name),
								origin_startpos, "all", NULL, tablename);

	pfree(tablename);

	apply_work(streamConn);

	/*
	 * never exit gracefully (as that'd unregister the worker) unless
	 * explicitly asked to do so.
	 */
	proc_exit(1);
}
