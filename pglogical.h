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

#endif /* PGLOGICAL_H */

