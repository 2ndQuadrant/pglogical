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

extern void pglogical_drop_remote_slot(PGconn *conn, const char *slot_name);
extern void pglogical_remote_node_info(PGconn *conn, Oid *nodeid, char **node_name, char **sysid, char **dbname, char **replication_sets);

#endif /* PGLOGICAL_RPC_H */
