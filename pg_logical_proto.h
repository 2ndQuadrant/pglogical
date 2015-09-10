/*-------------------------------------------------------------------------
 *
 * pg_logical_proto.h
 *		pg_logical protocol
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pg_logical_proto.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_LOGICAL_PROTO_H
#define PG_LOGICAL_PROTO_H

#include "utils/timestamp.h"

#include "pg_logical_relcache.h"

typedef struct PGLogicalTupleData
{
	Datum	values[MaxTupleAttributeNumber];
	bool	nulls[MaxTupleAttributeNumber];
	bool	changed[MaxTupleAttributeNumber];
} PGLogicalTupleData;

extern void pg_logical_read_begin(StringInfo in, XLogRecPtr *remote_lsn,
					  TimestampTz *committime, TransactionId *remote_xid);
extern void pg_logical_read_commit(StringInfo in, XLogRecPtr *commit_lsn,
					   XLogRecPtr *end_lsn, TimestampTz *committime);
extern char *pg_logical_read_origin(StringInfo in, XLogRecPtr *origin_lsn);

extern uint32 pg_logical_read_rel(StringInfo in);

extern PGLogicalRelation *pg_logical_read_insert(StringInfo in, LOCKMODE lockmode,
					   PGLogicalTupleData *newtup);
extern PGLogicalRelation *pg_logical_read_update(StringInfo in, LOCKMODE lockmode, bool *hasoldtup,
					   PGLogicalTupleData *oldtup, PGLogicalTupleData *newtup);
extern PGLogicalRelation *pg_logical_read_delete(StringInfo in, LOCKMODE lockmode,
												 PGLogicalTupleData *oldtup);

#endif /* PG_LOGICAL_PROTO_H */
