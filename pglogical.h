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
#include "access/xlogdefs.h"
#include "executor/executor.h"

#include "libpq-fe.h"

#include "pglogical_fe.h"
#include "pglogical_node.h"

#include "pglogical_compat.h"

#define PGLOGICAL_VERSION "2.0.1"
#define PGLOGICAL_VERSION_NUM 20001

#define PGLOGICAL_MIN_PROTO_VERSION_NUM 1
#define PGLOGICAL_MAX_PROTO_VERSION_NUM 1

#define EXTENSION_NAME "pglogical"

#define REPLICATION_ORIGIN_ALL "all"

extern bool pglogical_synchronous_commit;
extern char *pglogical_temp_directory;
extern bool pglogical_use_spi;
extern bool pglogical_batch_inserts;
extern char *pglogical_extra_connection_options;

extern char *shorten_hash(const char *str, int maxlen);

extern List *textarray_to_list(ArrayType *textarray);
extern bool parsePGArray(const char *atext, char ***itemarray, int *nitems);

extern Oid get_pglogical_table_oid(const char *table);

extern void pglogical_execute_sql_command(char *cmdstr, char *role,
										  bool isTopLevel);

extern PGconn *pglogical_connect(const char *connstring, const char *connname,
								 const char *suffix);
extern PGconn *pglogical_connect_replica(const char *connstring,
										 const char *connname,
										 const char *suffix);
extern void pglogical_identify_system(PGconn *streamConn, uint64* sysid,
									  TimeLineID *timeline, XLogRecPtr *xlogpos,
									  Name *dbname);
extern void pglogical_start_replication(PGconn *streamConn,
										const char *slot_name,
										XLogRecPtr start_pos,
										const char *forward_origins,
										const char *replication_sets,
										const char *replicate_only_table);

extern void pglogical_manage_extension(void);

extern void apply_work(PGconn *streamConn);

extern bool synchronize_sequences(void);
extern void synchronize_sequence(Oid seqoid);
extern void pglogical_create_sequence_state_record(Oid seqoid);
extern void pglogical_drop_sequence_state_record(Oid seqoid);
extern int64 sequence_get_last_value(Oid seqoid);

#endif /* PGLOGICAL_H */
