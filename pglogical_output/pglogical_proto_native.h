/*-------------------------------------------------------------------------
 *
 * pglogical_proto_native.h
 *		pglogical protocol, native implementation
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_proto_native.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_LOGICAL_PROTO_NATIVE_H
#define PG_LOGICAL_PROTO_NATIVE_H

#include "pglogical_output_plugin.h"

#include "pglogical_output_proto.h"

extern void pglogical_write_rel(StringInfo out, PGLogicalOutputData *data,
		Relation rel, Bitmapset *att_list);
extern void pglogical_write_begin(StringInfo out, PGLogicalOutputData *data,
		ReorderBufferTXN *txn);
extern void pglogical_write_commit(StringInfo out, PGLogicalOutputData *data,
		ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
extern void pglogical_write_origin(StringInfo out, const char *origin,
		XLogRecPtr origin_lsn);
extern void pglogical_write_insert(StringInfo out, PGLogicalOutputData *data,
		Relation rel, HeapTuple newtuple, Bitmapset *att_list);
extern void pglogical_write_update(StringInfo out, PGLogicalOutputData *data,
		Relation rel, HeapTuple oldtuple, HeapTuple newtuple,
		Bitmapset *att_list);
extern void pglogical_write_delete(StringInfo out, PGLogicalOutputData *data,
		Relation rel, HeapTuple oldtuple, Bitmapset *att_list);
extern void write_startup_message(StringInfo out, List *msg);

#endif /* PG_LOGICAL_PROTO_NATIVE_H */
