/*-------------------------------------------------------------------------
 *
 * pglogical_apply.h
 * 		pglogical apply functions
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pglogical_apply.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGLOGICAL_APPLY_H
#define PGLOGICAL_APPLY_H

#include "pglogical_relcache.h"
#include "pglogical_proto_native.h"

typedef void (*pglogical_apply_begin_fn) (void);
typedef void (*pglogical_apply_commit_fn) (void);

typedef void (*pglogical_apply_insert_fn) (PGLogicalRelation *rel,
									   PGLogicalTupleData *newtup);
typedef void (*pglogical_apply_update_fn) (PGLogicalRelation *rel,
									   PGLogicalTupleData *oldtup,
									   PGLogicalTupleData *newtup);
typedef void (*pglogical_apply_delete_fn) (PGLogicalRelation *rel,
									   PGLogicalTupleData *oldtup);

typedef bool (*pglogical_apply_can_mi_fn) (PGLogicalRelation *rel);
typedef void (*pglogical_apply_mi_add_tuple_fn) (PGLogicalRelation *rel,
												 PGLogicalTupleData *tup);
typedef void (*pglogical_apply_mi_finish_fn) (PGLogicalRelation *rel);

#endif /* PGLOGICAL_APPLY_H */
