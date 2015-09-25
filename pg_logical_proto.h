/*-------------------------------------------------------------------------
 *
 * pg_logical_proto.c
 *		pg_logical protocol
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pg_logical_proto.c
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_LOGICAL_PROTO_H
#define PG_LOGICAL_PROTO_H


void pg_logical_write_rel(StringInfo out, Relation rel);

void pg_logical_write_begin(StringInfo out, ReorderBufferTXN *txn);
void pg_logical_write_commit(StringInfo out, ReorderBufferTXN *txn,
							 XLogRecPtr commit_lsn);

void pg_logical_write_origin(StringInfo out, const char *origin,
							 XLogRecPtr origin_lsn);

void pg_logical_write_insert(StringInfo out, PGLogicalOutputData *data,
							 Relation rel, HeapTuple newtuple);
void pg_logical_write_update(StringInfo out, PGLogicalOutputData *data,
							 Relation rel, HeapTuple oldtuple,
							 HeapTuple newtuple);
void pg_logical_write_delete(StringInfo out, PGLogicalOutputData *data,
							 Relation rel, HeapTuple oldtuple);

void write_startup_message(StringInfo out, const char *msg, int len);

#endif /* PG_LOGICAL_PROTO_H */
