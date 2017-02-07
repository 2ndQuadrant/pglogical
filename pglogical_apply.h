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
#include "pglogical_proto.h"

typedef void (*pglogical_apply_begin_fn) (void);
typedef void (*pglogical_apply_commit_fn) (void);

typedef void (*pglogical_apply_insert_fn) (PGLogicalRelation *rel,
									   PGLogicalTupleData *newtup);
typedef void (*pglogical_apply_update_fn) (PGLogicalRelation *rel,
									   PGLogicalTupleData *oldtup,
									   PGLogicalTupleData *newtup);
typedef void (*pglogical_apply_delete_fn) (PGLogicalRelation *rel,
									   PGLogicalTupleData *oldtup);

typedef struct PGLogicalApplyFunctions
{
	pglogical_apply_begin_fn	on_begin;
	pglogical_apply_commit_fn	on_commit;
	pglogical_apply_insert_fn	do_insert;
	pglogical_apply_update_fn	do_update;
	pglogical_apply_delete_fn	do_delete;
} PGLogicalApplyFunctions;

#endif /* PGLOGICAL_APPLY_H */
