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
#include "pglogical_proto.h"

extern void pglogical_apply_heap_begin(void);
extern void pglogical_apply_heap_commit(void);

extern void pglogical_apply_heap_insert(PGLogicalRelation *rel,
										PGLogicalTupleData *newtup);
extern void pglogical_apply_heap_update(PGLogicalRelation *rel,
										PGLogicalTupleData *oldtup,
										PGLogicalTupleData *newtup);
extern void pglogical_apply_heap_delete(PGLogicalRelation *rel,
										PGLogicalTupleData *oldtup);

extern void pglogical_apply_heap_sql(char *cmdstr, char *role,
									 bool isTopLevel);

#endif /* PGLOGICAL_APPLY_HEAP_H */
