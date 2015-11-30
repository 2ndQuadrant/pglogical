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

#include "libpq-fe.h"

#include "nodes/primnodes.h"
#include "pglogical_node.h"

typedef struct PGLogicalSyncStatus
{
	char	kind;
	Oid		subid;
	char   *nspname;
	char   *relname;
	char	status;
} PGLogicalSyncStatus;

#define SYNC_KIND_INIT		'i'
#define SYNC_KIND_FULL		'f'
#define SYNC_KIND_STRUCTURE	's'
#define SYNC_KIND_DATA		'd'

#define SyncKindData(kind) \
	(kind == SYNC_KIND_FULL || kind == SYNC_KIND_DATA)

#define SyncKindStructure(kind) \
	(kind == SYNC_KIND_FULL || kind == SYNC_KIND_STRUCTURE)

#define SYNC_STATUS_NONE		'\0'	/* No sync. */
#define SYNC_STATUS_INIT		'i'		/* Ask for sync. */
#define SYNC_STATUS_STRUCTURE	's'     /* Sync structure */
#define SYNC_STATUS_DATA		'd'		/* Data sync. */
#define SYNC_STATUS_CONSTAINTS	'c'		/* Constraint sync (post-data structure). */
#define SYNC_STATUS_SYNCWAIT	'w'		/* Table sync is waiting to get OK from main thread. */
#define SYNC_STATUS_CATCHUP		'u'		/* Catching up. */
#define SYNC_STATUS_READY		'r'		/* Done. */

extern void pglogical_sync_worker_finish(PGconn *applyconn);

extern void pglogical_sync_subscription(PGLogicalSubscription *sub);
extern void pglogical_sync_table(PGLogicalSubscription *sub, RangeVar *table);

extern void create_local_sync_status(PGLogicalSyncStatus *sync);
extern void drop_subscription_sync_status(Oid subid);

extern PGLogicalSyncStatus *get_subscription_sync_status(Oid subid);
extern void set_subscription_sync_status(Oid subid, char status);

extern void drop_table_sync_status(const char *nspname, const char *relname);
extern PGLogicalSyncStatus *get_table_sync_status(Oid subid,
												  const char *schemaname,
												  const char *relname,
												  bool missing_ok);
extern void set_table_sync_status(Oid subid, const char *schemaname,
								  const char *relname, char status);
extern List *get_unsynced_tables(Oid subid);

extern bool wait_for_sync_status_change(Oid subid, char *nspname,
										char *relname, char desired_state);

#endif /* PGLOGICAL_SYNC_H */

