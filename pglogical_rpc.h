/*-------------------------------------------------------------------------
 *
 * pglogical_rpc.h
 *				Remote calls
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *				pglogical_rpc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGLOGICAL_RPC_H
#define PGLOGICAL_RPC_H

#include "libpq-fe.h"

extern List *pg_logical_get_remote_repset_tables(PGconn *conn,
									List *replication_sets);
extern PGLogicalRemoteRel *pg_logical_get_remote_repset_table(PGconn *conn,
								  RangeVar *rv, List *replication_sets);

extern bool pglogical_remote_slot_active(PGconn *conn, const char *slot_name);
extern void pglogical_drop_remote_slot(PGconn *conn, const char *slot_name);
extern void pglogical_remote_node_info(PGconn *conn, Oid *nodeid,
						   char **node_name, char **sysid, char **dbname,
						   char **replication_sets);
extern bool pglogical_remote_function_exists(PGconn *conn, const char *nspname,
								 const char *proname, int nargs, char *argname);

#endif /* PGLOGICAL_RPC_H */
