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

#include "access/genam.h"
#include "access/heapam.h"
#include "access/skey.h"
#include "access/stratnum.h"
#include "access/xact.h"

#include "catalog/indexing.h"

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
#include "utils/fmgroids.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/resowner.h"

#include "pglogical_repset.h"
#include "pglogical_rpc.h"
#include "pglogical_sync.h"
#include "pglogical_worker.h"
#include "pglogical.h"

#define CATALOG_LOCAL_SYNC_STATUS	"local_sync_status"

#define Natts_local_sync_state	5
#define Anum_sync_kind			1
#define Anum_sync_subid			2
#define Anum_sync_nspname		3
#define Anum_sync_relname		4
#define Anum_sync_status		5


void pglogical_sync_main(Datum main_arg);

static PGLogicalSyncWorker	   *MySyncWorker = NULL;


static void
dump_structure(PGLogicalSubscription *sub, const char *snapshot)
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
					 sub->origin_if->dsn);

	res = system(command.data);
	if (res != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not execute command \"%s\"",
						command.data)));
}

/* TODO: switch to SPI? */
static void
restore_structure(PGLogicalSubscription *sub, const char *section)
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
					 pg_restore, section, sub->target_if->dsn,
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
				const char *nspname, const char *relname)
{
	PGresult   *res;
	int			bytes;
	char	   *copybuf;
	StringInfoData	query;

	/* Build COPY TO query. */
	initStringInfo(&query);
	appendStringInfo(&query, "COPY %s.%s TO stdout",
					 PQescapeIdentifier(origin_conn, nspname,
										strlen(nspname)),
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
					 PQescapeIdentifier(origin_conn, nspname,
										strlen(nspname)),
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
								   ALLOCSET_DEFAULT_MINSIZE,
								   ALLOCSET_DEFAULT_INITSIZE,
								   ALLOCSET_DEFAULT_MAXSIZE);

	StartTransactionCommand();
	oldctx = MemoryContextSwitchTo(myctx);
	sync = get_subscription_sync_status(sub->id);
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
		PGconn	   *origin_conn_repl;
		RepOriginId	originid;
		char	   *snapshot;
		NameData	slot_name;

		elog(INFO, "initializing subscriber %s", sub->name);

		StartTransactionCommand();

		gen_slot_name(&slot_name, get_database_name(MyDatabaseId),
					  sub->origin->name, sub->name, NULL);

		origin_conn_repl = pglogical_connect_replica(sub->origin_if->dsn,
													 EXTENSION_NAME "_snapshot");

		snapshot = ensure_replication_slot_snapshot(origin_conn_repl, &slot_name,
													&lsn);
		originid = ensure_replication_origin(&slot_name);
		replorigin_advance(originid, lsn, XactLastCommitEnd, true, true);

		CommitTransactionCommand();

		if (SyncKindStructure(sync->kind))
		{
			elog(INFO, "synchronizing structure");

			status = SYNC_STATUS_STRUCTURE;
			StartTransactionCommand();
			set_subscription_sync_status(sub->id, status);
			CommitTransactionCommand();

			/* Dump structure to temp storage. */
			dump_structure(sub, snapshot);

			/* Restore base pre-data structure (types, tables, etc). */
			restore_structure(sub, "pre-data");
		}

		/* Copy data. */
		if (SyncKindData(sync->kind))
		{
			elog(INFO, "synchronizing data");

			status = SYNC_STATUS_DATA;
			StartTransactionCommand();
			set_subscription_sync_status(sub->id, status);
			CommitTransactionCommand();

			copy_replication_sets_data(sub->origin_if->dsn,
									   sub->target_if->dsn, snapshot,
									   sub->replication_sets);
		}

		/* Restore post-data structure (indexes, constraints, etc). */
		if (SyncKindStructure(sync->kind))
		{
			elog(INFO, "synchronizing constraints");

			status = SYNC_STATUS_CONSTAINTS;
			StartTransactionCommand();
			set_subscription_sync_status(sub->id, status);
			CommitTransactionCommand();

			restore_structure(sub, "post-data");
		}

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

		elog(INFO, "finished synchronization of subsriber %s, ready to enter normal replication", sub->name);
	}

	MemoryContextDelete(myctx);
}


void
pglogical_sync_table(PGLogicalSubscription *sub, RangeVar *table)
{
	XLogRecPtr	lsn;
	PGconn	   *origin_conn_repl;
	RepOriginId	originid;
	char	   *snapshot;
	NameData	slot_name;
	PGLogicalSyncStatus	   *sync;

	StartTransactionCommand();

	/* Sanity check. */
	sync = get_subscription_sync_status(sub->id);
	if (sync->status != SYNC_STATUS_READY)
	{
		elog(ERROR,
			 "subscriber %s is not ready, cannot synchronzie individual tables", sub->name);
	}

	/* Check current state of the table. */
	sync = get_table_sync_status(sub->id, table->schemaname, table->relname);

	/* Already synchronized, nothing to do here. */
	if (sync->status == SYNC_STATUS_READY)
		proc_exit(0);

	/* If previous sync attempt failed, we need to start from beginning. */
	if (sync->status != SYNC_STATUS_INIT)
		set_table_sync_status(sub->id, table->schemaname, table->relname, SYNC_STATUS_INIT);

	gen_slot_name(&slot_name, get_database_name(MyDatabaseId),
				  sub->origin->name, sub->name, table->relname);

	CommitTransactionCommand();

	origin_conn_repl = pglogical_connect_replica(sub->origin_if->dsn,
												 EXTENSION_NAME "_copy");

	snapshot = ensure_replication_slot_snapshot(origin_conn_repl, &slot_name,
												&lsn);

	StartTransactionCommand();
	originid = ensure_replication_origin(&slot_name);
	replorigin_advance(originid, lsn, XactLastCommitEnd, true, true);

	set_table_sync_status(sub->id, table->schemaname, table->relname, SYNC_STATUS_DATA);
	CommitTransactionCommand();

	/* Copy data. */
	copy_tables_data(sub->origin_if->dsn,sub->target_if->dsn, snapshot,
					 list_make1(table));

	PQfinish(origin_conn_repl);
}

void
pglogical_sync_worker_finish(PGconn *applyconn)
{
	PGLogicalWorker	   *apply;
	NameData			slot_name;
	PGconn			   *origin_conn;
	PGLogicalSubscription  *sub;

	StartTransactionCommand();
	sub = get_subscription(MyApplyWorker->subid);
	gen_slot_name(&slot_name, get_database_name(MyDatabaseId),
				  sub->origin->name, sub->name,
				  NameStr(MyPGLogicalWorker->worker.sync.relname));

	/* Disconnect from the slot so we can drop it. */
	if (applyconn)
		PQfinish(applyconn);

	/* Mark local table as ready. */
	set_table_sync_status(MyApplyWorker->subid,
						  NameStr(MyPGLogicalWorker->worker.sync.nspname),
						  NameStr(MyPGLogicalWorker->worker.sync.relname),
						  SYNC_STATUS_READY);

	/* Drop the slot on the remote side. */
	origin_conn = pglogical_connect(sub->origin_if->dsn, sub->name);
	pglogical_drop_remote_slot(origin_conn, NameStr(slot_name));
	PQfinish(origin_conn);

	/* Drop the origin tracking locally. */
	replorigin_session_reset();
	replorigin_drop(replorigin_session_origin);
	replorigin_session_origin = InvalidRepOriginId;
	CommitTransactionCommand();

	/*
	 * In case there is apply process running, it might be waiting
	 * for the table status change so tell it to check.
	 */
	LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);
	apply = pglogical_apply_find(MyPGLogicalWorker->dboid,
								 MyApplyWorker->subid);
	if (apply)
		SetLatch(&apply->proc->procLatch);
	LWLockRelease(PGLogicalCtx->lock);
}

void
pglogical_sync_main(Datum main_arg)
{
	int				slot = DatumGetInt32(main_arg);
	PGconn		   *streamConn;
	RepOriginId		originid;
	XLogRecPtr		origin_startpos;
	NameData		slot_name;
	PGLogicalSubscription	   *sub;
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

	/* Attach to dsm segment. */
	Assert(CurrentResourceOwner == NULL);
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "pglogical sync");

	/* Connect to our database. */
	BackgroundWorkerInitializeConnectionByOid(MyPGLogicalWorker->dboid, InvalidOid);

	StartTransactionCommand();
	saved_ctx = MemoryContextSwitchTo(TopMemoryContext);
	sub = get_subscription(MySyncWorker->apply.subid);
	MemoryContextSwitchTo(saved_ctx);
	CommitTransactionCommand();

	copytable = makeRangeVar(NameStr(MySyncWorker->nspname),
							 NameStr(MySyncWorker->relname), -1);

	elog(LOG, "starting sync of table %s.%s for subscriber %s",
		 copytable->schemaname, copytable->relname, sub->name);
	elog(DEBUG1, "conneting to provider %s, dsn %s",
		 sub->origin_if->name, sub->origin_if->dsn);

	/* Do the initial sync first. */
	pglogical_sync_table(sub, copytable);

	/* Wait for ack from the main apply thread. */
	StartTransactionCommand();
	set_table_sync_status(sub->id, copytable->schemaname, copytable->relname,
						  SYNC_STATUS_SYNCWAIT);
	CommitTransactionCommand();

	wait_for_sync_status_change(sub->id, copytable->schemaname,
								copytable->relname, SYNC_STATUS_CATCHUP);

	/* Setup the origin and get the starting position for the replication. */
	StartTransactionCommand();
	gen_slot_name(&slot_name, get_database_name(MyDatabaseId),
				  sub->origin->name, sub->name,
				  NameStr(MySyncWorker->relname));
	originid = replorigin_by_name(NameStr(slot_name), false);
	replorigin_session_setup(originid);
	replorigin_session_origin = originid;
	origin_startpos = replorigin_session_get_progress(false);
	CommitTransactionCommand();

	/* In case there is nothing to catchup, finish immediately. */
	if (origin_startpos >= MyApplyWorker->replay_stop_lsn)
	{
		pglogical_sync_worker_finish(NULL);
		proc_exit(0);
	}

	/* Start the replication. */
	streamConn = pglogical_connect_replica(sub->origin_if->dsn, sub->name);

	tablename = quote_qualified_identifier(copytable->schemaname,
										   copytable->relname);

	pglogical_start_replication(streamConn, NameStr(slot_name),
								origin_startpos, "all", NULL, tablename);

	pfree(tablename);

	/* Leave it to standard apply code to do the replication. */
	apply_work(streamConn);

	/*
	 * never exit gracefully (as that'd unregister the worker) unless
	 * explicitly asked to do so.
	 */
	proc_exit(1);
}


/* Catalog access */

/* Create subscription sync status record in catalog. */
void
create_subscription_sync_status(PGLogicalSyncStatus *sync)
{
	RangeVar   *rv;
	Relation	rel;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Datum		values[Natts_local_sync_state];
	bool		nulls[Natts_local_sync_state];
	NameData	nspname;
	NameData	relname;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_LOCAL_SYNC_STATUS, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* Form a tuple. */
	memset(nulls, false, sizeof(nulls));

	values[Anum_sync_kind - 1] = CharGetDatum(sync->kind);
	values[Anum_sync_subid - 1] = ObjectIdGetDatum(sync->subid);
	if (sync->nspname)
	{
		namestrcpy(&nspname, sync->nspname);
		values[Anum_sync_nspname - 1] = NameGetDatum(&nspname);
	}
	else
		nulls[Anum_sync_nspname - 1] = true;
	if (sync->relname)
	{
		namestrcpy(&relname, sync->relname);
		values[Anum_sync_relname - 1] = NameGetDatum(&relname);
	}
	else
		nulls[Anum_sync_relname - 1] = true;
	values[Anum_sync_status - 1] = CharGetDatum(sync->status);

	tup = heap_form_tuple(tupDesc, values, nulls);

	/* Insert the tuple to the catalog. */
	simple_heap_insert(rel, tup);

	/* Update the indexes. */
	CatalogUpdateIndexes(rel, tup);

	/* Cleanup. */
	heap_freetuple(tup);
	heap_close(rel, RowExclusiveLock);
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
	rel = heap_openrv(rv, RowExclusiveLock);

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
	heap_close(rel, RowExclusiveLock);

}

static PGLogicalSyncStatus *
syncstatus_fromtuple(HeapTuple tuple, TupleDesc desc)
{
	PGLogicalSyncStatus	   *sync;
	Datum					d;
	bool					isnull;

	sync = (PGLogicalSyncStatus *) palloc(sizeof(PGLogicalSyncStatus));

	d = fastgetattr(tuple, Anum_sync_kind, desc, &isnull);
	Assert(!isnull);
	sync->kind = DatumGetChar(d);

	d = fastgetattr(tuple, Anum_sync_subid, desc, &isnull);
	Assert(!isnull);
	sync->subid = DatumGetObjectId(d);

	d = fastgetattr(tuple, Anum_sync_nspname, desc, &isnull);
	if (isnull)
		sync->nspname = NULL;
	else
		sync->nspname = pstrdup(NameStr(*DatumGetName(d)));

	d = fastgetattr(tuple, Anum_sync_relname, desc, &isnull);
	if (isnull)
		sync->relname = NULL;
	else
		sync->relname = pstrdup(NameStr(*DatumGetName(d)));

	d = fastgetattr(tuple, Anum_sync_status, desc, &isnull);
	Assert(!isnull);
	sync->status = DatumGetChar(d);

	sync->nspname = NULL;
	sync->relname = NULL;

	return sync;
}

/* Get the sync status for a subscription. */
PGLogicalSyncStatus *
get_subscription_sync_status(Oid subid)
{
	PGLogicalSyncStatus	   *sync;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[1];
	TupleDesc		tupDesc;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_LOCAL_SYNC_STATUS, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	ScanKeyInit(&key[0],
				Anum_sync_subid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(subid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		if (heap_attisnull(tuple, Anum_sync_nspname) &&
			heap_attisnull(tuple, Anum_sync_relname))
			break;
	}

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "subscription %u status not found", subid);

	sync = syncstatus_fromtuple(tuple, tupDesc);

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

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
	rel = heap_openrv(rv, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	ScanKeyInit(&key[0],
				Anum_sync_subid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(subid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);
	while (HeapTupleIsValid(oldtup = systable_getnext(scan)))
	{
		if (heap_attisnull(oldtup, Anum_sync_nspname) &&
			heap_attisnull(oldtup, Anum_sync_relname))
			break;
	}

	if (!HeapTupleIsValid(oldtup))
		elog(ERROR, "subscription %u status not found", subid);

	memset(nulls, false, sizeof(nulls));
	memset(replaces, false, sizeof(replaces));

	values[Anum_sync_status - 1] = CharGetDatum(status);
	replaces[Anum_sync_status - 1] = true;

	newtup = heap_modify_tuple(oldtup, tupDesc, values, nulls, replaces);

	/* Update the tuple in catalog. */
	simple_heap_update(rel, &oldtup->t_self, newtup);

	/* Update the indexes. */
	CatalogUpdateIndexes(rel, newtup);

	/* Cleanup. */
	heap_freetuple(newtup);
	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);
}

/* Get the sync status for a table. */
PGLogicalSyncStatus *
get_table_sync_status(Oid subid, const char *nspname, const char *relname)
{
	PGLogicalSyncStatus	   *sync;
	RangeVar	   *rv;
	Relation		rel;
	SysScanDesc		scan;
	HeapTuple		tuple;
	ScanKeyData		key[3];
	TupleDesc		tupDesc;

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_LOCAL_SYNC_STATUS, -1);
	rel = heap_openrv(rv, RowExclusiveLock);
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
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "subscription %u table %s.%s status not found", subid,
			 nspname, relname);

	sync = syncstatus_fromtuple(tuple, tupDesc);

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

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

	rv = makeRangeVar(EXTENSION_NAME, CATALOG_LOCAL_SYNC_STATUS, -1);
	rel = heap_openrv(rv, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_sync_subid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(subid));

	scan = systable_beginscan(rel, 0, true, NULL, 1, key);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		if (heap_attisnull(tuple, Anum_sync_nspname) &&
			heap_attisnull(tuple, Anum_sync_relname))
			continue;

		sync = (PGLogicalSyncStatus *) GETSTRUCT(tuple);
		if (sync->status != SYNC_STATUS_READY)
			res = lappend(res, makeRangeVar(sync->nspname, sync->relname, -1));
	}

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	return res;
}

/* Set the sync status for a table. */
void
set_table_sync_status(Oid subid, const char *nspname, const char *relname,
					  char status)
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
	rel = heap_openrv(rv, RowExclusiveLock);
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

	newtup = heap_modify_tuple(oldtup, tupDesc, values, nulls, replaces);

	/* Update the tuple in catalog. */
	simple_heap_update(rel, &oldtup->t_self, newtup);

	/* Update the indexes. */
	CatalogUpdateIndexes(rel, newtup);

	/* Cleanup. */
	heap_freetuple(newtup);
	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);
}

/*
 * Wait until the table sync status has changed desired one.
 *
 * We also exit if the worker is no longer recognized as sync worker as
 * that means something bad happened to it.
 */
bool
wait_for_sync_status_change(Oid subid, char *nspname, char *relname,
							char desired_state)
{
	int rc;

	while (!got_SIGTERM)
	{
		PGLogicalWorker		   *worker;
		PGLogicalSyncStatus	   *sync;

		StartTransactionCommand();
		sync = get_table_sync_status(subid, nspname, relname);
		if (sync->status == desired_state)
		{
			CommitTransactionCommand();
			return true;
		}
		CommitTransactionCommand();

		/* Check if the worker is still alive - no point waiting if it died. */
		LWLockAcquire(PGLogicalCtx->lock, LW_EXCLUSIVE);
		worker = pglogical_sync_find(MyDatabaseId, subid, nspname, relname);
		LWLockRelease(PGLogicalCtx->lock);
		if (!worker)
			return false;

		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   60000L);

        ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}

	return false; /* Silence compiler. */
}
