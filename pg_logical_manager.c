/*-------------------------------------------------------------------------
 *
 * pg_logical_manager.c
 * 		pg_logical worker for managing apply workers in a database
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pg_logical_manager.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq-fe.h"

#include "miscadmin.h"

#include "pg_logical_proto.h"
#include "pg_logical_relcache.h"
#include "pg_logical_node.h"

#include "commands/extension.h"

#include "postmaster/bgworker.h"

#include "storage/ipc.h"

#define EXTENSION_NAME "pg_logical"


void
pg_logical_manager_main(Datum main_arg)
{
	Oid		dbid = DatumGetObjectId(main_arg);
	Oid		extoid;
	PGLogicalNode *node;

	BackgroundWorkerInitializeConnectionByOid(dbid, InvalidOid);

	/* If the extension is not installed in this DB, exit. */
	extoid = get_extension_oid(EXTENSION_NAME, true);
	if (!OidIsValid(extoid))
		proc_exit(0);

	/* Fetch local node info. */
	node = get_local_node();

	/* No local node. */
	if (!node)
		proc_exit(0);
}
