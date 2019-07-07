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

#include "storage/lock.h"

#include "utils/rel.h"

#include "pglogical_relcache.h"
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
	bool		first = true;
	StringInfoData	query;
	StringInfoData	repsetarr;

	initStringInfo(&repsetarr);
	foreach (lc, replication_sets)
	{
		char	   *repset_name = lfirst(lc);

		if (first)
			first = false;
		else
			appendStringInfoChar(&repsetarr, ',');

		appendStringInfo(&repsetarr, "%s",
						 PQescapeLiteral(conn, repset_name, strlen(repset_name)));
	}

	initStringInfo(&query);
	if (pglogical_remote_function_exists(conn, "pglogical", "show_repset_table_info", 2, NULL))
	{
		/* PGLogical 2.0+ */
		appendStringInfo(&query,
						 "SELECT i.relid, i.nspname, i.relname, i.att_list,"
						 "       i.has_row_filter"
						 "  FROM (SELECT DISTINCT relid FROM pglogical.tables WHERE set_name = ANY(ARRAY[%s])) t,"
						 "       LATERAL pglogical.show_repset_table_info(t.relid, ARRAY[%s]) i",
						 repsetarr.data, repsetarr.data);
	}
	else
	{
		/* PGLogical 1.x */
		appendStringInfo(&query,
						 "SELECT r.oid AS relid, t.nspname, t.relname, ARRAY(SELECT attname FROM pg_attribute WHERE attrelid = r.oid AND NOT attisdropped AND attnum > 0) AS att_list,"
						 "       false AS has_row_filter"
						 "  FROM pglogical.tables t, pg_catalog.pg_class r, pg_catalog.pg_namespace n"
						 " WHERE t.set_name = ANY(ARRAY[%s]) AND r.relname = t.relname AND n.oid = r.relnamespace AND n.nspname = t.nspname",
						 repsetarr.data);
	}

	res = PQexec(conn, query.data);
	/* TODO: better error message? */
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		elog(ERROR, "could not get table list: %s", PQresultErrorMessage(res));

	for (i = 0; i < PQntuples(res); i++)
	{
		PGLogicalRemoteRel *remoterel = palloc0(sizeof(PGLogicalRemoteRel));

		remoterel->relid = atooid(PQgetvalue(res, i, 0));
		remoterel->nspname = pstrdup(PQgetvalue(res, i, 1));
		remoterel->relname = pstrdup(PQgetvalue(res, i, 2));
		if (!parsePGArray(PQgetvalue(res, i, 3), &remoterel->attnames,
						  &remoterel->natts))
			elog(ERROR, "could not parse column list for table");
		remoterel->hasRowFilter = (strcmp(PQgetvalue(res, i, 4), "t") == 0);

		tables = lappend(tables, remoterel);
	}

	PQclear(res);

	return tables;
}

/*
 * Like above but for one table.
 */
PGLogicalRemoteRel *
pg_logical_get_remote_repset_table(PGconn *conn, RangeVar *rv,
								   List *replication_sets)
{
	PGLogicalRemoteRel *remoterel = palloc0(sizeof(PGLogicalRemoteRel));
	PGresult   *res;
	ListCell   *lc;
	bool		first = true;
	StringInfoData	query;
	StringInfoData	repsetarr;
	StringInfoData	relname;

	initStringInfo(&relname);
	appendStringInfo(&relname, "%s.%s",
					 PQescapeIdentifier(conn, rv->schemaname, strlen(rv->schemaname)),
					 PQescapeIdentifier(conn, rv->relname, strlen(rv->relname)));

	initStringInfo(&repsetarr);
	foreach (lc, replication_sets)
	{
		char	   *repset_name = lfirst(lc);

		if (first)
			first = false;
		else
			appendStringInfoChar(&repsetarr, ',');

		appendStringInfo(&repsetarr, "%s",
						 PQescapeLiteral(conn, repset_name, strlen(repset_name)));
	}

	initStringInfo(&query);
	if (pglogical_remote_function_exists(conn, "pglogical", "show_repset_table_info", 2, NULL))
	{
		/* PGLogical 2.0+ */
		appendStringInfo(&query,
						 "SELECT i.relid, i.nspname, i.relname, i.att_list,"
						 "       i.has_row_filter"
						 "  FROM pglogical.show_repset_table_info(%s::regclass, ARRAY[%s]) i",
						 PQescapeLiteral(conn, relname.data, relname.len),
						 repsetarr.data);
	}
	else
	{
		/* PGLogical 1.x */
		appendStringInfo(&query,
						 "SELECT r.oid AS relid, t.nspname, t.relname, ARRAY(SELECT attname FROM pg_attribute WHERE attrelid = r.oid AND NOT attisdropped AND attnum > 0) AS att_list,"
						 "       false AS has_row_filter"
						 "  FROM pglogical.tables t, pg_catalog.pg_class r, pg_catalog.pg_namespace n"
						 " WHERE r.oid = %s::regclass AND t.set_name = ANY(ARRAY[%s]) AND r.relname = t.relname AND n.oid = r.relnamespace AND n.nspname = t.nspname",
						 PQescapeLiteral(conn, relname.data, relname.len),
						 repsetarr.data);
	}

	res = PQexec(conn, query.data);
	/* TODO: better error message? */
	if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) != 1)
		elog(ERROR, "could not get table list: %s", PQresultErrorMessage(res));

	remoterel->relid = atooid(PQgetvalue(res, 0, 0));
	remoterel->nspname = pstrdup(PQgetvalue(res, 0, 1));
	remoterel->relname = pstrdup(PQgetvalue(res, 0, 2));
	if (!parsePGArray(PQgetvalue(res, 0, 3), &remoterel->attnames,
					  &remoterel->natts))
		elog(ERROR, "could not parse column list for table");
	remoterel->hasRowFilter = (strcmp(PQgetvalue(res, 0, 4), "t") == 0);


	PQclear(res);

	return remoterel;
}


/*
 * Is the remote slot active?.
 */
bool
pglogical_remote_slot_active(PGconn *conn, const char *slot_name)
{
	PGresult	   *res;
	const char	   *values[1];
	Oid				types[1] = { TEXTOID };
	bool			ret;

	values[0] = slot_name;

	res = PQexecParams(conn,
					   "SELECT plugin, active "
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
		return false;
	}

	/* Slot found, validate that it's pglogical slot */
	if (PQgetisnull(res, 0, 0))
		elog(ERROR, "Unexpectedly null field %s", PQfname(res, 0));

	if (strcmp("pglogical_output", PQgetvalue(res, 0, 0)) != 0 &&
		strcmp("pglogical", PQgetvalue(res, 0, 0)) != 0)
		ereport(ERROR,
				(errmsg("slot %s is not pglogical slot", slot_name)));

	ret = (strcmp(PQgetvalue(res, 0, 1), "t") == 0);

	PQclear(res);

	return ret;
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

	if (strcmp("pglogical_output", PQgetvalue(res, 0, 0)) != 0 &&
		strcmp("pglogical", PQgetvalue(res, 0, 0)) != 0)
		ereport(ERROR,
				(errmsg("slot %s is not pglogical slot", slot_name)));

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
		elog(ERROR, "could not fetch remote node info: %s\n", PQerrorMessage(conn));

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

bool
pglogical_remote_function_exists(PGconn *conn, const char *nspname,
								 const char *proname, int nargs, char *argname)
{
	PGresult	   *res;
	const char	   *values[2];
	Oid				types[2] = { TEXTOID, TEXTOID };
	bool			ret;
	StringInfoData	query;

	values[0] = proname;
	values[1] = nspname;

	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT oid "
					 "  FROM pg_catalog.pg_proc "
					 " WHERE proname = $1 "
					 "   AND pronamespace = "
					 "       (SELECT oid "
					 "          FROM pg_catalog.pg_namespace "
					 "         WHERE nspname = $2)");

	if (nargs >= 0)
		appendStringInfo(&query,
						 "   AND pronargs = '%d'", nargs);
	if (argname != NULL)
		appendStringInfo(&query,
						 "   AND %s = ANY (proargnames)",
						 PQescapeLiteral(conn, argname, strlen(argname)));

	res = PQexecParams(conn, query.data, 2, types, values, NULL, NULL, 0);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		elog(ERROR, "could not fetch remote function info: %s\n",
			PQerrorMessage(conn));

	ret = PQntuples(res) > 0;

	PQclear(res);

	return ret;
}
