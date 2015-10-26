/*-------------------------------------------------------------------------
 *
 * pglogical_init_replica.c
 *		Initial node sync
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *                pglogical_init_replica.c
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

#include "utils/pg_lsn.h"

#include "pglogical_node.h"
#include "pglogical_repset.h"
#include "pglogical_rpc.h"
#include "pglogical_init_replica.h"
#include "pglogical.h"

/*
 * Find another program in our binary's directory,
 * and return its version.
 */
static int
find_other_exec_version(const char *argv0, const char *target,
						uint32 *version, char *retpath)
{
	char		cmd[MAXPGPATH];
	char		cmd_output[1024];
	FILE       *output;
	int			pre_dot,
				post_dot;

	if (find_my_exec(argv0, retpath) < 0)
		return -1;

	/* Trim off program name and keep just directory */
	*last_dir_separator(retpath) = '\0';
	canonicalize_path(retpath);

	/* Now append the other program's name */
	snprintf(retpath + strlen(retpath), MAXPGPATH - strlen(retpath),
			 "/%s%s", target, EXE);

	snprintf(cmd, sizeof(cmd), "\"%s\" -V", retpath);

	if ((output = popen(cmd, "r")) == NULL)
		return -1;

	if (fgets(cmd_output, sizeof(cmd_output), output) == NULL)
	{
		pclose(output);
		return -1;
	}
	pclose(output);

	if (sscanf(cmd_output, "%*s %*s %d.%d", &pre_dot, &post_dot) != 2)
		return -2;

	*version = (pre_dot * 100 + post_dot) * 100;

	return 0;
}

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
	appendStringInfo(&command, "%s --snapshot=\"%s\" -N %s -F c -f \"%s\" \"%s\"",
					 pg_dump, snapshot, EXTENSION_NAME, "/tmp/pglogical.dump",
					 sub->provider_dsn);

	res = system(command.data);
	if (res != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not execute command \"%s\": %m",
						command.data)));
}

/* TODO: switch to SPI */
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
				 errmsg("could not execute command \"%s\": %m",
						command.data)));
}

/*
 * Make standard postgres connection, ERROR on failure.
 */
static PGconn *
pg_connect(const char *connstring, const char *connname)
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
static PGconn *
pg_connect_replica(const char *connstring, const char *connname)
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


static void
start_copy_origin_tx(PGconn *conn, const char *snapshot)
{
	PGresult	   *res;
	const char	   *setup_query_template =
		"BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;\n"
		"SET TRANSACTION SNAPSHOT '%s';\n"
		"SET DATESTYLE = ISO;\n"
		"SET INTERVALSTYLE = POSTGRES;\n"
		"SET extra_float_digits TO 3;\n"
		"SET statement_timeout = 0;\n"
		"SET lock_timeout = 0;\n";
	StringInfoData	query;

	initStringInfo(&query);
	appendStringInfo(&query, setup_query_template, snapshot);

	res = PQexec(conn, query.data);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		elog(ERROR, "BEGIN on origin node failed: %s",
				PQresultErrorMessage(res));
	PQclear(res);
}

static void
start_copy_target_tx(PGconn *conn, const char *snapshot)
{
	PGresult	   *res;
	const char	   *setup_query_template =
		"BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;\n"
		"SET DATESTYLE = ISO;\n"
		"SET INTERVALSTYLE = POSTGRES;\n"
		"SET extra_float_digits TO 3;\n"
		"SET statement_timeout = 0;\n"
		"SET lock_timeout = 0;\n";
	StringInfoData	query;

	initStringInfo(&query);
	appendStringInfo(&query, setup_query_template, snapshot);

	res = PQexec(conn, query.data);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		elog(ERROR, "BEGIN on target node failed: %s",
				PQresultErrorMessage(res));
	PQclear(res);
}

/*
 * COPY table.
 *
 * TODO: move to separate worker and use COPY API instead of libpq connection.
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
 * For now restores complete structure, but data is copied only for
 * replicated tables.
 */
static void
copy_node_data(PGLogicalSubscriber *sub, const char *snapshot)
{
	PGconn	   *origin_conn;
	PGconn	   *target_conn;
	List	   *tables;
	ListCell   *lc;
	PGresult   *res;

	/* Connect to origin node. */
	origin_conn = pg_connect(sub->provider_dsn, EXTENSION_NAME "_init");
	start_copy_origin_tx(origin_conn, snapshot);

	/* Get tables to copy from origin node. */
	tables = get_copy_tables(origin_conn, sub->replication_sets);

	/* Connect to target node. */
	/* TODO: make work */
	target_conn = pg_connect(sub->local_dsn, EXTENSION_NAME "_init");
	start_copy_target_tx(target_conn, snapshot);

	/* Copy every table. */
	foreach (lc, tables)
	{
		RangeVar	*rv = lfirst(lc);

		copy_table_data(origin_conn, target_conn,
						rv->schemaname, rv->relname);

		CHECK_FOR_INTERRUPTS();
	}

	/* Close the  transaction and connection on origin node. */
	res = PQexec(origin_conn, "ROLLBACK");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		elog(WARNING, "ROLLBACK on origin node failed: %s",
				PQresultErrorMessage(res));
	PQclear(res);
	PQfinish(origin_conn);

	/* Close the transaction and connection on target node. */
	res = PQexec(target_conn, "COMMIT");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		elog(ERROR, "COMMIT on target node failed: %s",
				PQresultErrorMessage(res));
	PQclear(res);
	PQfinish(target_conn);
}

/*
 * Ensure slot exitst.
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

void
pglogical_init_replica(PGLogicalSubscriber *sub)
{
	XLogRecPtr	lsn;
	char		status;

	status = sub->status;

	switch (status)
	{
		/* We can recover from crashes during these. */
		case SUBSCRIBER_STATUS_INIT:
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
					  sub->provider_name, sub->name);

		origin_conn_repl = pg_connect_replica(sub->provider_dsn,
											  EXTENSION_NAME "_snapshot");

		snapshot = ensure_replication_slot_snapshot(origin_conn_repl, &slot_name,
													&lsn);
		originid = ensure_replication_origin(&slot_name);
		replorigin_advance(originid, lsn, XactLastCommitEnd, true, true);

		CommitTransactionCommand();

		set_subscriber_status(sub->id, SUBSCRIBER_STATUS_SYNC_SCHEMA);
		status = SUBSCRIBER_STATUS_SYNC_SCHEMA;

		elog(INFO, "synchronizing schemas");

		/* Dump structure to temp storage. */
		dump_structure(sub, snapshot);

		/* Restore base pre-data structure (types, tables, etc). */
		restore_structure(sub, "pre-data");

		/* Copy data. */
		copy_node_data(sub, snapshot);

		/* Restore post-data structure (indexes, constraints, etc). */
		restore_structure(sub, "post-data");

		set_subscriber_status(sub->id, SUBSCRIBER_STATUS_CATCHUP);
		status = SUBSCRIBER_STATUS_CATCHUP;
	}

	if (status == SUBSCRIBER_STATUS_CATCHUP)
	{
		/* Nothing to do here yet. */
		status = SUBSCRIBER_STATUS_READY;
		set_subscriber_status(sub->id, status);

		elog(INFO, "finished init_replica, ready to enter normal replication");
	}
}
