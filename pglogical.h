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

#include "libpq-fe.h"

#include "pglogical_node.h"

#define EXTENSION_NAME "pglogical"

#define PGLOGICAL_MASTER_TOC_MAGIC	123
#define PGLOGICAL_MASTER_TOC_STATE	1
#define PGLOGICAL_MASTER_TOC_APPLY	2

#define REPLICATION_ORIGIN_ALL "all"

extern void gen_slot_name(Name slot_name, char *dbname,
						  const char *provider_name,
						  const char *subscriber_name,
						  const char *suffix);
extern Oid pglogical_generate_id(void);
extern List *textarray_to_list(ArrayType *textarray);

extern void pglogical_execute_sql_command(char *cmdstr, char *role,
										  bool isTopLevel);

extern PGconn *pglogical_connect(const char *connstring, const char *connname);
extern PGconn *pglogical_connect_replica(const char *connstring,
										 const char *connname);
extern void pglogical_start_replication(PGconn *streamConn,
										const char *slot_name,
										XLogRecPtr start_pos,
										const char *forward_origins,
										const char *replication_sets,
										const char *replicate_table);

extern void apply_work(PGconn *streamConn);

#endif /* PGLOGICAL_H */
