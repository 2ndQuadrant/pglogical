/*-------------------------------------------------------------------------
 *
 * pglogical_output_proto.h
 *		pglogical protocol
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_output_proto.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_LOGICAL_OUTPUT_PROTO_H
#define PG_LOGICAL_OUTPUT_PROTO_H

#include "lib/stringinfo.h"
#include "replication/reorderbuffer.h"
#include "utils/relcache.h"

#include "pglogical_output_plugin.h"

/*
 * Protocol capabilities
 *
 * PGLOGICAL_PROTO_VERSION_NUM is our native protocol and the greatest version
 * we can support. PGLOGICAL_PROTO_MIN_VERSION_NUM is the oldest version we
 * have backwards compatibility for. We negotiate protocol versions during the
 * startup handshake. See the protocol documentation for details.
 */
#define PGLOGICAL_PROTO_VERSION_NUM 1
#define PGLOGICAL_PROTO_MIN_VERSION_NUM 1

/*
 * The startup parameter format is versioned separately to the rest of the wire
 * protocol because we negotiate the wire protocol version using the startup
 * parameters sent to us. It hopefully won't ever need to change, but this
 * field is present in case we do need to change it, e.g. to a structured json
 * object. We can look at the startup params version to see whether we can
 * understand the startup params sent by the client and to fall back to
 * reading an older format if needed.
 */
#define PGLOGICAL_STARTUP_PARAM_FORMAT_FLAT 1

/*
 * For similar reasons to the startup params
 * (PGLOGICAL_STARTUP_PARAM_FORMAT_FLAT) the startup reply message format is
 * versioned separately to the rest of the protocol. The client has to be able
 * to read it to find out what protocol version was selected by the upstream
 * when using the native protocol.
 */
#define PGLOGICAL_STARTUP_MSG_FORMAT_FLAT 1

typedef enum PGLogicalProtoType
{
	PGLogicalProtoNative,
	PGLogicalProtoJson
} PGLogicalProtoType;

typedef void (*pglogical_write_rel_fn) (StringInfo out, PGLogicalOutputData * data,
						   Relation rel, Bitmapset *att_list);

typedef void (*pglogical_write_begin_fn) (StringInfo out, PGLogicalOutputData * data,
													  ReorderBufferTXN *txn);
typedef void (*pglogical_write_commit_fn) (StringInfo out, PGLogicalOutputData * data,
							   ReorderBufferTXN *txn, XLogRecPtr commit_lsn);

typedef void (*pglogical_write_origin_fn) (StringInfo out, const char *origin,
													   XLogRecPtr origin_lsn);

typedef void (*pglogical_write_insert_fn) (StringInfo out, PGLogicalOutputData * data,
										   Relation rel, HeapTuple newtuple,
										   Bitmapset *att_list);
typedef void (*pglogical_write_update_fn) (StringInfo out, PGLogicalOutputData * data,
											Relation rel, HeapTuple oldtuple,
											HeapTuple newtuple,
											Bitmapset *att_list);
typedef void (*pglogical_write_delete_fn) (StringInfo out, PGLogicalOutputData * data,
										   Relation rel, HeapTuple oldtuple,
										   Bitmapset *att_list);

typedef void (*write_startup_message_fn) (StringInfo out, List *msg);

typedef struct PGLogicalProtoAPI
{
	pglogical_write_rel_fn write_rel;
	pglogical_write_begin_fn write_begin;
	pglogical_write_commit_fn write_commit;
	pglogical_write_origin_fn write_origin;
	pglogical_write_insert_fn write_insert;
	pglogical_write_update_fn write_update;
	pglogical_write_delete_fn write_delete;
	write_startup_message_fn write_startup_message;
} PGLogicalProtoAPI;

extern PGLogicalProtoAPI *pglogical_init_api(PGLogicalProtoType typ);

#endif /* PG_LOGICAL_OUTPUT_PROTO_H */
