/*-------------------------------------------------------------------------
 *
 * pglogical.h
 *              pglogical replication plugin
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *              pglogical.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGLOGICAL_H
#define PGLOGICAL_H

#include "storage/s_lock.h"
#include "postmaster/bgworker.h"

#include "pglogical_node.h"

#define EXTENSION_NAME "pglogical"

#define PGLOGICAL_MASTER_TOC_MAGIC	123
#define PGLOGICAL_MASTER_TOC_STATE	1
#define PGLOGICAL_MASTER_TOC_APPLY	2

#define REPLICATION_ORIGIN_ALL "all"

typedef struct PGLogicalApplyWorker
{
	Oid		dboid;
	int		connid;
	BackgroundWorkerHandle *bgwhandle;
} PGLogicalApplyWorker;

extern PGLogicalApplyWorker *MyApplyWorker;

typedef struct PGLogicalDBState
{
	/* Oid of the database this flock works on. */
	Oid		dboid;

	/* Mutex protecting the writes */
	slock_t	mutex;

	/* Number of apply processes needed. */
	int		apply_total;

	/* Number of apply processes attached. */
	int		apply_attached;
} PGLogicalDBState;

extern volatile sig_atomic_t got_SIGTERM;

extern void gen_slot_name(Name slot_name, char *dbname,
						  PGLogicalNode *origin_node,
						  PGLogicalNode *target_node);
extern Oid pglogical_generate_id(void);
extern List *textarray_to_list(ArrayType *textarray);

extern void pglogical_connections_changed(void);

extern void pglogical_manager_attach(Oid dboid);
extern void pglogical_manager_detach(bool signal_supervisor);

extern void handle_sigterm(SIGNAL_ARGS);

void pglogical_execute_sql_command(char *cmdstr, char *role, bool isTopLevel);

#endif /* PGLOGICAL_H */
