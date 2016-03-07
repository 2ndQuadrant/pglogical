/*-------------------------------------------------------------------------
 *
 * pglogical_proto_internal.h
 *		pglogical protocol
 *
 * Copyright (c) 2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_proto_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGLOGICAL_PROTO_INTERNAL_H
#define PGLOGICAL_PROTO_INTERNAL_H

#include "lib/stringinfo.h"
#include "replication/reorderbuffer.h"
#include "utils/relcache.h"

#include "pglogical_proto.h"
#include "pglogical_relmetacache.h"


typedef void (*pglogical_write_rel_fn) (StringInfo out, PGLogicalOutputData * data,
						   Relation rel, PGLRelMetaCacheEntry * cache_entry);

typedef void (*pglogical_write_begin_fn) (StringInfo out, PGLogicalOutputData * data,
													  ReorderBufferTXN *txn);
typedef void (*pglogical_write_commit_fn) (StringInfo out, PGLogicalOutputData * data,
							   ReorderBufferTXN *txn, XLogRecPtr commit_lsn);

typedef void (*pglogical_write_origin_fn) (StringInfo out, const char *origin,
													   XLogRecPtr origin_lsn);

typedef void (*pglogical_write_insert_fn) (StringInfo out, PGLogicalOutputData * data,
										   Relation rel, HeapTuple newtuple);
typedef void (*pglogical_write_update_fn) (StringInfo out, PGLogicalOutputData * data,
											Relation rel, HeapTuple oldtuple,
													   HeapTuple newtuple);
typedef void (*pglogical_write_delete_fn) (StringInfo out, PGLogicalOutputData * data,
										   Relation rel, HeapTuple oldtuple);

typedef void (*write_startup_message_fn) (StringInfo out, List *msg);

/* typedef appears in pglogical_proto.h */
struct PGLogicalProtoAPI
{
	pglogical_write_rel_fn write_rel;
	pglogical_write_begin_fn write_begin;
	pglogical_write_commit_fn write_commit;
	pglogical_write_origin_fn write_origin;
	pglogical_write_insert_fn write_insert;
	pglogical_write_update_fn write_update;
	pglogical_write_delete_fn write_delete;
	write_startup_message_fn write_startup_message;
};


#endif   /* PGLOGICAL_PROTO_INTERNAL_H */
