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

extern void pglogical_drop_remote_slot(PGconn *conn, const char *slot_name);

#endif /* PGLOGICAL_RPC_H */
