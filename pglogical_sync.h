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

#include "nodes/primnodes.h"
#include "pglogical_node.h"

#define TABLE_SYNC_STATUS_NONE		'\0'	/* No table sync. */
#define TABLE_SYNC_STATUS_INIT		'i'		/* Ask for table sync. */
#define TABLE_SYNC_STATUS_DATA		'd'     /* Table sync is copying data. */
#define TABLE_SYNC_STATUS_SYNCWAIT	'w'		/* Table sync is waiting to get OK from main thread. */
#define TABLE_SYNC_STATUS_CATCHUP	'c'		/* Table sync is catching up to a given lsn. */
#define TABLE_SYNC_STATUS_READY		'r'		/* Table sync is done. */

extern void pglogical_copy_database(PGLogicalSubscriber *sub);
extern void pglogical_copy_table(PGLogicalSubscriber *sub, RangeVar *table);

#endif /* PGLOGICAL_SYNC_H */
