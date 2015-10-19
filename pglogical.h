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
#include "utils/array.h"

#include "pglogical_node.h"

#define EXTENSION_NAME "pglogical"

#define PGLOGICAL_MASTER_TOC_MAGIC	123
#define PGLOGICAL_MASTER_TOC_STATE	1
#define PGLOGICAL_MASTER_TOC_APPLY	2

#define REPLICATION_ORIGIN_ALL "all"

extern void gen_slot_name(Name slot_name, char *dbname,
						  PGLogicalNode *origin_node,
						  PGLogicalNode *target_node);
extern Oid pglogical_generate_id(void);
extern List *textarray_to_list(ArrayType *textarray);

void pglogical_execute_sql_command(char *cmdstr, char *role, bool isTopLevel);

void pglogical_commandfilter_init(void);

#endif /* PGLOGICAL_H */
