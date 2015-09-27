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

extern void set_remote_node_status(PGconn *conn, const char *node_name,
								   char node_status);

#endif /* PGLOGICAL_RPC_H */



