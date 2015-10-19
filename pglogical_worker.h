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

typedef enum {
	PGLOGICAL_WORKER_NONE,		/* Unused slot. */
	PGLOGICAL_WORKER_MANAGER,	/* Manager. */
	PGLOGICAL_WORKER_APPLY		/* Apply. */
} PGLogicalWorkerType;

typedef struct PGLogicalWorker {
	PGLogicalWorkerType	worker_type;

	/* Pointer to proc array. NULL if not running. */
	PGPROC *proc;

	/* Database id to connect to. */
	Oid		dboid;

	/* Connection id, for apply worker. */
	union
	{
		struct
		{
			int     connid;	/* Connection id for apply worker. */
		} apply;
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

extern PGLogicalContext	   *PGLogicalCtx;
extern PGLogicalWorker	   *MyPGLogicalWorker;

extern volatile sig_atomic_t got_SIGTERM;

extern void handle_sigterm(SIGNAL_ARGS);

extern void pglogical_connections_changed(void);

extern void pglogical_worker_shmem_init(void);

extern int pglogical_worker_register(PGLogicalWorker *worker);
extern void pglogical_worker_attach(int slot);
extern void pglogical_worker_detach(bool signal_supervisor);

extern PGLogicalWorker *pglogical_manager_find(Oid dboid);
extern PGLogicalWorker *pglogical_apply_find(Oid dboid, int connid);

#endif /* PGLOGICAL_WORKER_H */
