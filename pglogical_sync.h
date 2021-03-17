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
	char		kind;
	Oid			subid;
	NameData	nspname;
	NameData	relname;
	char		status;
	XLogRecPtr	statuslsn;		/* remote lsn of the state change used for
								 * synchronization coordination */
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
#define SYNC_STATUS_CONSTRAINTS	'c'		/* Constraint sync (post-data structure). */
#define SYNC_STATUS_SYNCWAIT	'w'		/* Table sync is waiting to get OK from main thread. */
#define SYNC_STATUS_CATCHUP		'u'		/* Catching up. */
#define SYNC_STATUS_SYNCDONE	'y'		/* Synchronization finished (at lsn). */
#define SYNC_STATUS_READY		'r'		/* Done. */

extern void pglogical_sync_worker_finish(void);

extern void pglogical_sync_subscription(PGLogicalSubscription *sub);
extern char pglogical_sync_table(PGLogicalSubscription *sub, RangeVar *table, XLogRecPtr *status_lsn);

extern void create_local_sync_status(PGLogicalSyncStatus *sync);
extern void drop_subscription_sync_status(Oid subid);

extern PGLogicalSyncStatus *get_subscription_sync_status(Oid subid,
														 bool missing_ok);
extern void set_subscription_sync_status(Oid subid, char status);

extern void drop_table_sync_status(const char *nspname, const char *relname);
extern void drop_table_sync_status_for_sub(Oid subid, const char *nspname,
							   const char *relname);

extern PGLogicalSyncStatus *get_table_sync_status(Oid subid,
												  const char *schemaname,
												  const char *relname,
												  bool missing_ok);
extern void set_table_sync_status(Oid subid, const char *schemaname,
								  const char *relname, char status,
								  XLogRecPtr status_lsn);
extern List *get_unsynced_tables(Oid subid);

/* For interface compat with pgl3 */
inline static void free_sync_status(PGLogicalSyncStatus *sync)
{
	pfree(sync);
}

extern bool wait_for_sync_status_change(Oid subid, const char *nspname,
										const char *relname, char desired_state,
										XLogRecPtr *status_lsn);

extern void truncate_table(char *nspname, char *relname);
extern List *get_subscription_tables(Oid subid);

#ifdef WIN32
extern void QuoteWindowsArgv(StringInfo cmdline, const char * argv[]);
#endif

#endif /* PGLOGICAL_SYNC_H */

