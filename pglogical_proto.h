/*-------------------------------------------------------------------------
 *
 * pglogical_proto.c
 *		pglogical protocol
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_proto.c
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_LOGICAL_PROTO_H
#define PG_LOGICAL_PROTO_H


void pglogical_write_rel(StringInfo out, Relation rel);

void pglogical_write_begin(StringInfo out, ReorderBufferTXN *txn);
void pglogical_write_commit(StringInfo out, ReorderBufferTXN *txn,
							 XLogRecPtr commit_lsn);

void pglogical_write_origin(StringInfo out, const char *origin,
							 XLogRecPtr origin_lsn);

void pglogical_write_insert(StringInfo out, PGLogicalOutputData *data,
							 Relation rel, HeapTuple newtuple);
void pglogical_write_update(StringInfo out, PGLogicalOutputData *data,
							 Relation rel, HeapTuple oldtuple,
							 HeapTuple newtuple);
void pglogical_write_delete(StringInfo out, PGLogicalOutputData *data,
							 Relation rel, HeapTuple oldtuple);

void write_startup_message(StringInfo out, const char *msg, int len);

#endif /* PG_LOGICAL_PROTO_H */
