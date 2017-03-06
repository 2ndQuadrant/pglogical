/*-------------------------------------------------------------------------
 *
 * pglogical_proto_json.h
 *		pglogical protocol, json implementation
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_proto_json.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_LOGICAL_PROTO_JSON_H
#define PG_LOGICAL_PROTO_JSON_H

#include "pglogical_output_plugin.h"

#include "lib/stringinfo.h"
#include "nodes/pg_list.h"

#include "pglogical_output_proto.h"

extern void pglogical_json_write_begin(StringInfo out, PGLogicalOutputData *data,
								 ReorderBufferTXN *txn);
extern void pglogical_json_write_commit(StringInfo out, PGLogicalOutputData *data,
								 ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
extern void pglogical_json_write_insert(StringInfo out, PGLogicalOutputData *data,
								 Relation rel, HeapTuple newtuple,
								 Bitmapset *att_list);
extern void pglogical_json_write_update(StringInfo out, PGLogicalOutputData *data,
								 Relation rel, HeapTuple oldtuple,
								 HeapTuple newtuple, Bitmapset *att_list);
extern void pglogical_json_write_delete(StringInfo out, PGLogicalOutputData *data,
								 Relation rel, HeapTuple oldtuple,
								 Bitmapset *att_list);
extern void json_write_startup_message(StringInfo out, List *msg);

#endif /* PG_LOGICAL_PROTO_JSON_H */
