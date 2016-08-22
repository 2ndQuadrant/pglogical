#ifndef PGLOGICAL_OUTPUT_HOOKS_H
#define PGLOGICAL_OUTPUT_HOOKS_H

#include "access/xlogdefs.h"
#include "nodes/pg_list.h"
#include "replication/reorderbuffer.h"
#include "utils/relcache.h"

#include "pglogical_output/compat.h"

/*
 * This header is to be included by extensions that implement pglogical output
 * plugin callback hooks for transaction origin and row filtering, etc. It is
 * installed as "pglogical_output/hooks.h"
 *
 * See the README.md and the example in examples/hooks/ for details on hooks.
 */


typedef struct PGLogicalStartupHookArgs
{
	void	   *private_data;
	List	   *in_params;
	List	   *out_params;
} PGLogicalStartupHookArgs;

typedef void (*pglogical_startup_hook_fn)(PGLogicalStartupHookArgs *args);


typedef struct PGLogicalTxnFilterArgs
{
	void 	   *private_data;
	RepOriginId	origin_id;
} PGLogicalTxnFilterArgs;

typedef bool (*pglogical_txn_filter_hook_fn)(PGLogicalTxnFilterArgs *args);


typedef struct PGLogicalRowFilterArgs
{
	void 	   *private_data;
	Relation	changed_rel;
	ReorderBufferTXN * txn;
	enum ReorderBufferChangeType	change_type;
	/* detailed row change event from logical decoding */
	ReorderBufferChange *change;
} PGLogicalRowFilterArgs;

typedef bool (*pglogical_row_filter_hook_fn)(PGLogicalRowFilterArgs *args);


typedef struct PGLogicalShutdownHookArgs
{
	void	   *private_data;
} PGLogicalShutdownHookArgs;

typedef void (*pglogical_shutdown_hook_fn)(PGLogicalShutdownHookArgs *args);

/*
 * This struct is passed to the pglogical_get_hooks_fn as the first argument,
 * typed 'internal', and is unwrapped with `DatumGetPointer`.
 */
typedef struct PGLogicalHooks
{
	pglogical_startup_hook_fn startup_hook;
	pglogical_shutdown_hook_fn shutdown_hook;
	pglogical_txn_filter_hook_fn txn_filter_hook;
	pglogical_row_filter_hook_fn row_filter_hook;
	void *hooks_private_data;
} PGLogicalHooks;


#endif /* PGLOGICAL_OUTPUT_HOOKS_H */
