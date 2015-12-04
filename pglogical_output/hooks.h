#ifndef PGLOGICAL_OUTPUT_HOOKS_H
#define PGLOGICAL_OUTPUT_HOOKS_H

#include "access/xlogdefs.h"
#include "nodes/pg_list.h"
#include "utils/rel.h"
#include "utils/palloc.h"
#include "replication/reorderbuffer.h"

#include "pglogical_output/compat.h"

/*
 * This header is to be included by extensions that implement pglogical output
 * plugin callback hooks for transaction origin and row filtering, etc. It is
 * installed as "pglogical_output/hooks.h"
 *
 * See the README.md and the example in examples/hooks/ for details on hooks.
 */


struct PGLogicalStartupHookArgs
{
	void	   *private_data;
	List	   *in_params;
	List	   *out_params;
};

typedef void (*pglogical_startup_hook_fn)(struct PGLogicalStartupHookArgs *args);


struct PGLogicalTxnFilterArgs
{
	void 	   *private_data;
	RepOriginId	origin_id;
};

typedef bool (*pglogical_txn_filter_hook_fn)(struct PGLogicalTxnFilterArgs *args);


struct PGLogicalRowFilterArgs
{
	void 	   *private_data;
	Relation	changed_rel;
	enum ReorderBufferChangeType	change_type;
	/* detailed row change event from logical decoding */
	ReorderBufferChange* change;
};

typedef bool (*pglogical_row_filter_hook_fn)(struct PGLogicalRowFilterArgs *args);


struct PGLogicalShutdownHookArgs
{
	void	   *private_data;
};

typedef void (*pglogical_shutdown_hook_fn)(struct PGLogicalShutdownHookArgs *args);

/*
 * This struct is passed to the pglogical_get_hooks_fn as the first argument,
 * typed 'internal', and is unwrapped with `DatumGetPointer`.
 */
struct PGLogicalHooks
{
	pglogical_startup_hook_fn startup_hook;
	pglogical_shutdown_hook_fn shutdown_hook;
	pglogical_txn_filter_hook_fn txn_filter_hook;
	pglogical_row_filter_hook_fn row_filter_hook;
	void *hooks_private_data;
};


#endif /* PGLOGICAL_OUTPUT_HOOKS_H */
