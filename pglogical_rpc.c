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

#include "libpq-fe.h"

void
set_remote_node_status(PGconn *conn, const char *node_name, char node_status)
{
}
