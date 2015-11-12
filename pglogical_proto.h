/*-------------------------------------------------------------------------
 *
 * pglogical_proto.h
 *		pglogical protocol
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_proto.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_LOGICAL_PROTO_H
#define PG_LOGICAL_PROTO_H

typedef void (*pglogical_write_rel_fn)(StringInfo out, Relation rel);

typedef void (*pglogical_write_begin_fn)(StringInfo out, PGLogicalOutputData *data,
							 ReorderBufferTXN *txn);
typedef void (*pglogical_write_commit_fn)(StringInfo out, PGLogicalOutputData *data,
							 ReorderBufferTXN *txn, XLogRecPtr commit_lsn);

typedef void (*pglogical_write_origin_fn)(StringInfo out, const char *origin,
							 XLogRecPtr origin_lsn);

typedef void (*pglogical_write_insert_fn)(StringInfo out, PGLogicalOutputData *data,
							 Relation rel, HeapTuple newtuple);
typedef void (*pglogical_write_update_fn)(StringInfo out, PGLogicalOutputData *data,
							 Relation rel, HeapTuple oldtuple,
							 HeapTuple newtuple);
typedef void (*pglogical_write_delete_fn)(StringInfo out, PGLogicalOutputData *data,
							 Relation rel, HeapTuple oldtuple);

typedef void (*write_startup_message_fn)(StringInfo out, List *msg);

typedef struct PGLogicalProtoAPI
{
	pglogical_write_rel_fn		write_rel;
	pglogical_write_begin_fn	write_begin;
	pglogical_write_commit_fn	write_commit;
	pglogical_write_origin_fn	write_origin;
	pglogical_write_insert_fn	write_insert;
	pglogical_write_update_fn	write_update;
	pglogical_write_delete_fn	write_delete;
	write_startup_message_fn	write_startup_message;
} PGLogicalProtoAPI;


typedef enum PGLogicalProtoType
{
	PGLogicalProtoNative,
	PGLogicalProtoJson
} PGLogicalProtoType;

extern PGLogicalProtoAPI *pglogical_init_api(PGLogicalProtoType typ);

#endif /* PG_LOGICAL_PROTO_H */
