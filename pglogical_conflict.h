/*-------------------------------------------------------------------------
 *
 * pglogical_conflict.h
 *		pglogical conflict detection and resolution
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pglogical_conflict.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGLOGICAL_CONGLICT_H
#define PGLOGICAL_CONGLICT_H

#include "nodes/execnodes.h"

#include "replication/origin.h"

#include "utils/guc.h"

#include "pglogical_proto_native.h"

typedef enum PGLogicalConflictResolution
{
	PGLogicalResolution_ApplyRemote,
	PGLogicalResolution_KeepLocal,
	PGLogicalResolution_Skip
} PGLogicalConflictResolution;

typedef enum
{
	PGLOGICAL_RESOLVE_ERROR,
	PGLOGICAL_RESOLVE_APPLY_REMOTE,
	PGLOGICAL_RESOLVE_KEEP_LOCAL,
	PGLOGICAL_RESOLVE_LAST_UPDATE_WINS,
	PGLOGICAL_RESOLVE_FIRST_UPDATE_WINS
} PGLogicalResolveOption;

extern int pglogical_conflict_resolver;
extern int pglogical_conflict_log_level;

typedef enum PGLogicalConflictType
{
	CONFLICT_INSERT_INSERT,
	CONFLICT_UPDATE_UPDATE,
	CONFLICT_UPDATE_DELETE,
	CONFLICT_DELETE_DELETE
} PGLogicalConflictType;

extern bool pglogical_tuple_find_replidx(ResultRelInfo *relinfo,
										 PGLogicalTupleData *tuple,
										 TupleTableSlot *oldslot,
										 Oid *idxrelid);

extern Oid pglogical_tuple_find_conflict(ResultRelInfo *relinfo,
										 PGLogicalTupleData *tuple,
										 TupleTableSlot *oldslot);

extern bool get_tuple_origin(HeapTuple local_tuple, TransactionId *xmin,
							 RepOriginId *local_origin, TimestampTz *local_ts);

extern bool try_resolve_conflict(Relation rel, HeapTuple localtuple,
								 HeapTuple remotetuple, HeapTuple *resulttuple,
								 PGLogicalConflictResolution *resolution);


extern void pglogical_report_conflict(PGLogicalConflictType conflict_type,
						  PGLogicalRelation *rel,
						  HeapTuple localtuple,
						  PGLogicalTupleData *oldkey,
						  HeapTuple remotetuple,
						  HeapTuple applytuple,
						  PGLogicalConflictResolution resolution,
						  TransactionId local_tuple_xid,
						  bool found_local_origin,
						  RepOriginId local_tuple_origin,
						  TimestampTz local_tuple_timestamp,
						  Oid conflict_idx_id,
						  bool has_before_triggers);

extern bool pglogical_conflict_resolver_check_hook(int *newval, void **extra,
									   GucSource source);

#endif /* PGLOGICAL_CONGLICT_H */
