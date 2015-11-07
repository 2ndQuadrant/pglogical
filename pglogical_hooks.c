/*-------------------------------------------------------------------------
 *
 * pglogical_hooks.c
 *		pglogical output plugin hooks
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pglogical_hooks.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "pglogical_output/hooks.h"
#include "pglogical_repset.h"
#include "pglogical_queue.h"
#include "pglogical.h"

#include "access/xact.h"

#include "catalog/namespace.h"
#include "catalog/pg_type.h"

#include "nodes/makefuncs.h"

#include "replication/origin.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "fmgr.h"
#include "miscadmin.h"

PGDLLEXPORT extern Datum pglogical_hooks_setupn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pglogical_hooks_setup);

void pglogical_startup_hook(struct PGLogicalStartupHookArgs *startup_args);
void pglogical_shutdown_hook(struct PGLogicalShutdownHookArgs *shutdown_args);
bool pglogical_row_filter_hook(struct PGLogicalRowFilterArgs *rowfilter_args);
bool pglogical_txn_filter_hook(struct PGLogicalTxnFilterArgs *txnfilter_args);

typedef struct PGLogicalHooksPrivate
{
    const char *forward_origin;
	/* List of PGLogicalRepSet */
	List	   *replication_sets;
	RangeVar   *replicate_table;
} PGLogicalHooksPrivate;

void
pglogical_startup_hook(struct PGLogicalStartupHookArgs *startup_args)
{
	PGLogicalHooksPrivate *private;
	ListCell *option;

	/* pglogical_output assures us that we'll be called in a tx */
	Assert(IsTransactionState());

	/* Allocated in hook memory context, scoped to the logical decoding session: */
	startup_args->private_data = private = (PGLogicalHooksPrivate*)palloc0(sizeof(PGLogicalHooksPrivate));

	foreach(option, startup_args->in_params)
	{
		DefElem    *elem = lfirst(option);

		if (pg_strcasecmp("pglogical.forward_origin", elem->defname) == 0)
		{
			if (elem->arg == NULL || strVal(elem->arg) == NULL)
				elog(ERROR, "pglogical.forward_origin may not be NULL");

			private->forward_origin = pstrdup(strVal(elem->arg));

			continue;
		}

		if (pg_strcasecmp("pglogical.replication_set_names", elem->defname) == 0)
		{
			List *replication_set_names;

			if (elem->arg == NULL || strVal(elem->arg) == NULL)
				elog(ERROR, "pglogical.replication_set_names may not be NULL");

			elog(DEBUG2, "pglogical startup hook got replication sets %s", strVal(elem->arg));

			if (!SplitIdentifierString(strVal(elem->arg), ',', &replication_set_names))
				elog(ERROR, "Could not parse replication set name list %s", strVal(elem->arg));

			private->replication_sets = get_replication_sets(replication_set_names, false);

			continue;
		}

		if (pg_strcasecmp("pglogical.table_name", elem->defname) == 0)
		{
			List *table_name;

			if (elem->arg == NULL || strVal(elem->arg) == NULL)
				elog(ERROR, "pglogical.table_name may not be NULL");

			elog(DEBUG2, "pglogical startup hook got table name %s", strVal(elem->arg));

			if (!SplitIdentifierString(strVal(elem->arg), '.', &table_name))
				elog(ERROR, "Could not parse table_name %s", strVal(elem->arg));

			private->replicate_table = makeRangeVar(pstrdup(linitial(table_name)),
													pstrdup(lsecond(table_name)), -1);

			continue;
		}
	}
}

bool
pglogical_row_filter_hook(struct PGLogicalRowFilterArgs *rowfilter_args)
{
	PGLogicalHooksPrivate *private = (PGLogicalHooksPrivate*)rowfilter_args->private_data;
	bool ret;

	if (private->replicate_table)
	{
		/*
		 * Special case - we are catching up just one table.
		 * TODO: performance
		 */
		return strcmp(RelationGetRelationName(rowfilter_args->changed_rel),
					  private->replicate_table->relname) == 0 &&
			RelationGetNamespace(rowfilter_args->changed_rel) ==
			get_namespace_oid(private->replicate_table->schemaname, true);
	}
	else if (RelationGetRelid(rowfilter_args->changed_rel) == get_queue_table_oid())
	{
		/* Special case - queue table */
		return true;
	}

	/* Normal case - use replication sets. */
	ret = relation_is_replicated(rowfilter_args->changed_rel,
		private->replication_sets,
		to_pglogical_changetype(rowfilter_args->change_type));

	return ret;
}

bool
pglogical_txn_filter_hook(struct PGLogicalTxnFilterArgs *txnfilter_args)
{
	PGLogicalHooksPrivate *private = (PGLogicalHooksPrivate*)txnfilter_args->private_data;
	bool ret;

	if (txnfilter_args->origin_id == InvalidRepOriginId)
	    /* Never filter out locally originated tx's */
	    ret = true;

	else
		/*
		 * Otherwise, ignore the origin passed in txnfilter_args->origin_id,
		 * and just forward all or nothing based on the configuration option
		 * 'forward_origin'.
		 */
		ret = strcmp(private->forward_origin, REPLICATION_ORIGIN_ALL) == 0;

	return ret;
}

Datum
pglogical_hooks_setup(PG_FUNCTION_ARGS)
{
	struct PGLogicalHooks *hooks = (struct PGLogicalHooks*) PG_GETARG_POINTER(0);

	/*
	 * Just assign the hook pointers. We're not meant to do much
	 * work here.
	 *
	 * Note that private_data is left untouched, to be set up by the
	 * startup hook.
	 */
	hooks->startup_hook = pglogical_startup_hook;
	hooks->shutdown_hook = NULL;
	hooks->row_filter_hook = pglogical_row_filter_hook;
	hooks->txn_filter_hook = pglogical_txn_filter_hook;

	PG_RETURN_VOID();
}
