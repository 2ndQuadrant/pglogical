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
#include "pglogical_sync.h"
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
				 errmsg("could not execute command \"%s\"",
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
				 errmsg("could not execute command \"%s\"",
						command.data)));
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
					  sub->provider_name, sub->name);

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
