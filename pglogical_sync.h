/*-------------------------------------------------------------------------
 *
 * pglogical_sync.h
 *		table synchronization functions
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *              pglogical_sync.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGLOGICAL_SYNC_H
#define PGLOGICAL_SYNC_H

#include "pglogical_node.h"

extern void copy_tables_data(const char *origin_dsn,
							 const char *target_dsn,
							 const char *origin_snapshot,
							 List *tables);

extern void copy_replication_sets_data(const char *origin_dsn,
									   const char *target_dsn,
									   const char *origin_snapshot,
									   List *replication_sets);
#endif /* PGLOGICAL_SYNC_H */
