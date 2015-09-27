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

#define EXTENSION_NAME "pglogical"

#define PGLOGICAL_MASTER_TOC_MAGIC	123
#define PGLOGICAL_MASTER_TOC_STATE	1
#define PGLOGICAL_MASTER_TOC_APPLY	2

typedef struct PGLogicalApplyWorker
{
	int		connid;
} PGLogicalApplyWorker;

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


extern void gen_slot_name(Name slot_name, char *dbname,
						  PGLogicalNode *origin_node,
						  PGLogicalNode *target_node);

extern void pg_logical_manager_main(Datum main_arg);
extern void pg_logical_apply_main(Datum main_arg);
extern void handle_sigterm(SIGNAL_ARGS);

#endif /* PGLOGICAL_H */

