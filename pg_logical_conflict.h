/*-------------------------------------------------------------------------
 *
 * pg_logical_conflict.h
 *		pg_logical conflict detection and resolution
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pg_logical_conflict.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_LOGICAL_CONGLICT_H
#define PG_LOGICAL_CONGLICT_H

typedef enum PGLogicalConflictResolution
{
	LogicalCR_KeepRemote,
	LogicalCR_KeepLocal
} PGLogicalConflictResolution;


extern bool pg_logical_tuple_find(Relation rel, Relation idxrel,
								  PGLogicalTupleData *tuple,
								  TupleTableSlot *oldslot);

extern Oid pg_logical_tuple_conflict(EState *estate, PGLogicalTupleData *tuple,
									 bool insert, TupleTableSlot *oldslot);

extern bool try_resolve_conflict(Relation rel, HeapTuple localtuple,
								 HeapTuple remotetuple, bool insert,
								 HeapTuple *resulttuple,
								 PGLogicalConflictResolution *resolution);

#endif /* PG_LOGICAL_CONGLICT_H */
