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

#include "lib/stringinfo.h"

#include "utils/timestamp.h"

#include "pglogical_output_plugin.h"
#include "pglogical_output_proto.h"
#include "pglogical_relcache.h"

typedef struct PGLogicalTupleData
{
	Datum	values[MaxTupleAttributeNumber];
	bool	nulls[MaxTupleAttributeNumber];
	bool	changed[MaxTupleAttributeNumber];
} PGLogicalTupleData;

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

extern void pglogical_read_begin(StringInfo in, XLogRecPtr *remote_lsn,
					  TimestampTz *committime, TransactionId *remote_xid);
extern void pglogical_read_commit(StringInfo in, XLogRecPtr *commit_lsn,
					   XLogRecPtr *end_lsn, TimestampTz *committime);
extern char *pglogical_read_origin(StringInfo in, XLogRecPtr *origin_lsn);
extern uint32 pglogical_read_rel(StringInfo in);
extern PGLogicalRelation *pglogical_read_insert(StringInfo in, LOCKMODE lockmode,
					   PGLogicalTupleData *newtup);
extern PGLogicalRelation *pglogical_read_update(StringInfo in, LOCKMODE lockmode, bool *hasoldtup,
					   PGLogicalTupleData *oldtup, PGLogicalTupleData *newtup);
extern PGLogicalRelation *pglogical_read_delete(StringInfo in, LOCKMODE lockmode,
												 PGLogicalTupleData *oldtup);
#endif /* PG_LOGICAL_PROTO_NATIVE_H */
