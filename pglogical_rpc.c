/*-------------------------------------------------------------------------
 *
 * pglogical_rpc.c
 *				Remote calls
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *				pglogical_rpc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "lib/stringinfo.h"

#include "nodes/makefuncs.h"

#include "catalog/pg_type.h"

#include "pglogical_repset.h"
#include "pglogical_rpc.h"
#include "pglogical.h"

#define atooid(x)  ((Oid) strtoul((x), NULL, 10))

/*
 * Fetch list of tables that are grouped in specified replication sets.
 */
List *
pg_logical_get_remote_repset_tables(PGconn *conn, List *replication_sets)
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
		char	   *repset_name = lfirst(lc);
		appendStringInfo(&repsetarr, "%s", repset_name);
	}
	appendStringInfo(&repsetarr, "}");

	/* Build COPY TO query. */
	initStringInfo(&query);
	appendStringInfo(&query, "SELECT nspname, relname FROM %s.tables WHERE set_name = ANY(%s)",
					 EXTENSION_NAME,
					 PQescapeLiteral(conn, repsetarr.data,
									 repsetarr.len));

	res = PQexec(conn, query.data);
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
 * Drops replication slot on remote node that has been used by the local node.
 */
void
pglogical_drop_remote_slot(PGconn *conn, const char *slot_name)
{
	PGresult	   *res;
	const char	   *values[1];
	Oid				types[1] = { TEXTOID };

	values[0] = slot_name;

	/* Check if the slot exists */
	res = PQexecParams(conn,
					   "SELECT plugin "
					   "FROM pg_catalog.pg_replication_slots "
					   "WHERE slot_name = $1",
					   1, types, values, NULL, NULL, 0);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		ereport(ERROR,
				(errmsg("getting remote slot info failed"),
				 errdetail("SELECT FROM pg_catalog.pg_replication_slots failed with: %s",
						   PQerrorMessage(conn))));
	}

	/* Slot not found return false */
	if (PQntuples(res) == 0)
	{
		PQclear(res);
		return;
	}

	/* Slot found, validate that it's BDR slot */
	if (PQgetisnull(res, 0, 0))
		elog(ERROR, "Unexpectedly null field %s", PQfname(res, 0));

	if (strcmp("pglogical_output", PQgetvalue(res, 0, 0)) != 0)
		ereport(ERROR,
				(errmsg("slot %s is not pglogical_outputR slot", slot_name)));

	PQclear(res);

	res = PQexecParams(conn, "SELECT pg_drop_replication_slot($1)",
					   1, types, values, NULL, NULL, 0);

	/* And finally, drop the slot. */
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		ereport(ERROR,
				(errmsg("remote slot drop failed"),
				 errdetail("SELECT pg_drop_replication_slot() failed with: %s",
						   PQerrorMessage(conn))));
	}

	PQclear(res);
}

void
pglogical_remote_node_info(PGconn *conn, Oid *nodeid, char **node_name, char **sysid, char **dbname, char **replication_sets)
{
	PGresult	   *res;

	res = PQexec(conn, "SELECT node_id, node_name, sysid, dbname, replication_sets FROM pglogical.pglogical_node_info()");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		elog(ERROR, "could fetch remote node info: %s\n", PQerrorMessage(conn));

	/* No nodes found? */
	if (PQntuples(res) == 0)
		elog(ERROR, "the remote database is not configured as a pglogical node.\n");

	if (PQntuples(res) > 1)
		elog(ERROR, "the remote database has multiple nodes configured. That is not supported with current version of pglogical.\n");

	*nodeid = atooid(PQgetvalue(res, 0, 0));
	*node_name = pstrdup(PQgetvalue(res, 0, 1));
	if (sysid)
		*sysid = pstrdup(PQgetvalue(res, 0, 2));
	if (dbname)
		*dbname = pstrdup(PQgetvalue(res, 0, 3));
	if (replication_sets)
		*replication_sets = pstrdup(PQgetvalue(res, 0, 4));

	PQclear(res);
}
