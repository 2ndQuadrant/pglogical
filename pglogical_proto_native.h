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

#include "pglogical_output.h"

#include "pglogical_proto_internal.h"
#include "pglogical_relmetacache.h"

/*
 * For similar reasons to the startup params
 * (PGLOGICAL_STARTUP_PARAM_FORMAT_FLAT) the startup reply message format is
 * versioned separately to the rest of the protocol. The client has to be able
 * to read it to find out what protocol version was selected by the upstream
 * when using the native protocol.
 */
#define PGLOGICAL_STARTUP_MSG_FORMAT_FLAT 1

extern void pglogical_write_rel(StringInfo out, PGLogicalOutputData *data,
		Relation rel, PGLRelMetaCacheEntry *cache_entry);
extern void pglogical_write_begin(StringInfo out, PGLogicalOutputData *data,
		ReorderBufferTXN *txn);
extern void pglogical_write_commit(StringInfo out, PGLogicalOutputData *data,
		ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
extern void pglogical_write_origin(StringInfo out, const char *origin,
		XLogRecPtr origin_lsn);
extern void pglogical_write_insert(StringInfo out, PGLogicalOutputData *data,
		Relation rel, HeapTuple newtuple);
extern void pglogical_write_update(StringInfo out, PGLogicalOutputData *data,
		Relation rel, HeapTuple oldtuple, HeapTuple newtuple);
extern void pglogical_write_delete(StringInfo out, PGLogicalOutputData *data,
		Relation rel, HeapTuple oldtuple);
extern void write_startup_message(StringInfo out, List *msg);

#endif /* PG_LOGICAL_PROTO_NATIVE_H */
