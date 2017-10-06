/*-------------------------------------------------------------------------
 *
 * pglogical_executor.h
 *              pglogical replication plugin
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *              pglogical_executor.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGLOGICAL_EXECUTOR_H
#define PGLOGICAL_EXECUTOR_H

#include "executor/executor.h"

extern List *pglogical_truncated_tables;

extern EState *create_estate_for_relation(Relation rel, bool forwrite);
extern ExprContext *prepare_per_tuple_econtext(EState *estate, TupleDesc tupdesc);
extern ExprState *pglogical_prepare_row_filter(Node *row_filter);

extern void pglogical_executor_init(void);

#endif /* PGLOGICAL_EXECUTOR_H */
