/*-------------------------------------------------------------------------
 *
 * pglogical_apply_heap.h
 * 		pglogical apply functions using heap api
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pglogical_apply_heap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGLOGICAL_APPLY_HEAP_H
#define PGLOGICAL_APPLY_HEAP_H

#include "pglogical_relcache.h"
#include "pglogical_proto_native.h"

extern void pglogical_apply_heap_begin(void);
extern void pglogical_apply_heap_commit(void);

extern void pglogical_apply_heap_insert(PGLogicalRelation *rel,
										PGLogicalTupleData *newtup);
extern void pglogical_apply_heap_update(PGLogicalRelation *rel,
										PGLogicalTupleData *oldtup,
										PGLogicalTupleData *newtup);
extern void pglogical_apply_heap_delete(PGLogicalRelation *rel,
										PGLogicalTupleData *oldtup);

bool pglogical_apply_heap_can_mi(PGLogicalRelation *rel);
void pglogical_apply_heap_mi_add_tuple(PGLogicalRelation *rel,
									   PGLogicalTupleData *tup);
void pglogical_apply_heap_mi_finish(PGLogicalRelation *rel);

#endif /* PGLOGICAL_APPLY_HEAP_H */
