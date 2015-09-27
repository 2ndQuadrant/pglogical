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

#include "utils/guc.h"

#include "pglogical_proto.h"

typedef enum PGLogicalConflictResolution
{
	PGLogicalResolution_ApplyRemote,
	PGLogicalResolution_KeepLocal,
	PGLogicalResolution_Skip,
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

typedef enum PGLogicalConflictType
{
	CONFLICT_INSERT_INSERT,
	CONFLICT_UPDATE_UPDATE,
	CONFLICT_UPDATE_DELETE,
	CONFLICT_DELETE_DELETE
} PGLogicalConflictType;

extern bool pglogical_tuple_find_replidx(EState *estate,
										 PGLogicalTupleData *tuple,
										 TupleTableSlot *oldslot);

extern Oid pglogical_tuple_find_conflict(EState *estate,
										 PGLogicalTupleData *tuple,
										 TupleTableSlot *oldslot);

extern bool try_resolve_conflict(Relation rel, HeapTuple localtuple,
								 HeapTuple remotetuple, HeapTuple *resulttuple,
								 PGLogicalConflictResolution *resolution);


extern void pglogical_report_conflict(PGLogicalConflictType conflict_type, Relation rel,
						  HeapTuple localtuple, HeapTuple remotetuple,
						  HeapTuple applytuple,
						  PGLogicalConflictResolution resolution);

extern bool pglogical_conflict_resolver_check_hook(int *newval, void **extra,
									   GucSource source);

#endif /* PGLOGICAL_CONGLICT_H */
