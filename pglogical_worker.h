/*-------------------------------------------------------------------------
 *
 * pglogical_worker.h
 *              pglogical worker helper functions
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *              pglogical_worker.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGLOGICAL_WORKER_H
#define PGLOGICAL_WORKER_H

#include "pglogical.h"

typedef enum {
	PGLOGICAL_WORKER_NONE,		/* Unused slot. */
	PGLOGICAL_WORKER_MANAGER,	/* Manager. */
	PGLOGICAL_WORKER_APPLY,		/* Apply. */
	PGLOGICAL_WORKER_SYNC		/* Special type of Apply that synchronizes
								 * one table. */
} PGLogicalWorkerType;

typedef struct PGLogicalApplyWorker
{
	Oid			subid;				/* Subscription id for apply worker. */
	bool		sync_pending;		/* Is there new synchronization info pending?. */
	XLogRecPtr	replay_stop_lsn;	/* Replay should stop here if defined. */
} PGLogicalApplyWorker;

typedef struct PGLogicalSyncWorker
{
	PGLogicalApplyWorker	apply; /* Apply worker info, must be first. */
	NameData	nspname;	/* Name of the schema of table to copy if any. */
	NameData	relname;	/* Name of the table to copy if any. */
} PGLogicalSyncWorker;

typedef struct PGLogicalWorker {
	PGLogicalWorkerType	worker_type;

	/* Pointer to proc array. NULL if not running. */
	PGPROC *proc;

	/* Database id to connect to. */
	Oid		dboid;

	/* Connection id, for apply worker. */
	union
	{
		PGLogicalApplyWorker apply;
		PGLogicalSyncWorker sync;
	} worker;

} PGLogicalWorker;

typedef struct PGLogicalContext {
	/* Write lock. */
	LWLock	   *lock;

	/* Supervisor process. */
	PGPROC	   *supervisor;

	/* Background workers. */
	int			total_workers;
	PGLogicalWorker  workers[FLEXIBLE_ARRAY_MEMBER];
} PGLogicalContext;

extern PGLogicalContext		   *PGLogicalCtx;
extern PGLogicalWorker		   *MyPGLogicalWorker;
extern PGLogicalApplyWorker	   *MyApplyWorker;
extern PGLogicalSubscription   *MySubscription;

extern volatile sig_atomic_t got_SIGTERM;

extern void handle_sigterm(SIGNAL_ARGS);

extern void pglogical_connections_changed(void);

extern void pglogical_worker_shmem_init(void);

extern int pglogical_worker_register(PGLogicalWorker *worker);
extern void pglogical_worker_attach(int slot);
extern void pglogical_worker_detach(bool signal_supervisor);

extern PGLogicalWorker *pglogical_manager_find(Oid dboid);
extern PGLogicalWorker *pglogical_apply_find(Oid dboid, Oid subscriberid);
extern List *pglogical_apply_find_all(Oid dboid);

extern PGLogicalWorker *pglogical_sync_find(Oid dboid, Oid subid,
											char *nspname, char *relname);
extern List *pglogical_sync_find_all(Oid dboid, Oid subscriberid);

extern PGLogicalWorker *pglogical_get_worker(int slot);
extern bool pglogical_worker_running(PGLogicalWorker *w);

#endif /* PGLOGICAL_WORKER_H */
