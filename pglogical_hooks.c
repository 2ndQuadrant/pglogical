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

/*
 * If you get an error like
 *
 *     pglogical_output/hooks.h: No such file or directory
 *
 * then you forgot to install the pglogical_output extension before building
 * pglogical.
 */
#include "pglogical_output/hooks.h"
#include "pglogical_repset.h"
#include "pglogical_queue.h"
#include "pglogical.h"

#include "access/xact.h"

#include "catalog/namespace.h"
#include "catalog/pg_type.h"

#include "executor/executor.h"

#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"

#include "optimizer/planner.h"

#include "parser/parse_coerce.h"

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
	Oid			local_node_id;
	/* List of PGLogicalRepSet */
	List	   *replication_sets;
	RangeVar   *replicate_only_table;
	/* List of origin names */
    List	   *forward_origins;
} PGLogicalHooksPrivate;

void
pglogical_startup_hook(struct PGLogicalStartupHookArgs *startup_args)
{
	PGLogicalHooksPrivate  *private;
	ListCell			   *option;
	PGLogicalLocalNode	   *node;

	/* pglogical_output assures us that we'll be called in a tx */
	Assert(IsTransactionState());

	node = get_local_node(false, false);

	/* Allocated in hook memory context, scoped to the logical decoding session: */
	startup_args->private_data = private = (PGLogicalHooksPrivate*)palloc0(sizeof(PGLogicalHooksPrivate));

	private->local_node_id = node->node->id;

	foreach(option, startup_args->in_params)
	{
		DefElem    *elem = lfirst(option);

		if (pg_strcasecmp("pglogical.forward_origins", elem->defname) == 0)
		{
			List		   *forward_origin_names;
			ListCell	   *lc;

			if (elem->arg == NULL || strVal(elem->arg) == NULL)
				elog(ERROR, "pglogical.forward_origins may not be NULL");

			elog(DEBUG2, "pglogical startup hook got forward origins %s", strVal(elem->arg));

			if (!SplitIdentifierString(strVal(elem->arg), ',', &forward_origin_names))
				elog(ERROR, "Could not parse forward origin name list %s", strVal(elem->arg));

			foreach (lc, forward_origin_names)
			{
				char	   *origin_name = (char *) lfirst(lc);

				if (strcmp(origin_name, REPLICATION_ORIGIN_ALL) != 0)
					elog(ERROR, "Only \"%s\" is allowed in forward origin name list at the moment, found \"%s\"",
						 REPLICATION_ORIGIN_ALL, origin_name);
			}

			private->forward_origins = forward_origin_names;

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

			private->replication_sets =
				get_replication_sets(private->local_node_id,
									 replication_set_names, false);

			continue;
		}

		if (pg_strcasecmp("pglogical.replicate_only_table", elem->defname) == 0)
		{
			List *replicate_only_table;

			if (elem->arg == NULL || strVal(elem->arg) == NULL)
				elog(ERROR, "pglogical.replicate_only_table may not be NULL");

			elog(DEBUG2, "pglogical startup hook got table name %s", strVal(elem->arg));

			if (!SplitIdentifierString(strVal(elem->arg), '.', &replicate_only_table))
				elog(ERROR, "Could not parse replicate_only_table %s", strVal(elem->arg));

			private->replicate_only_table = makeRangeVar(pstrdup(linitial(replicate_only_table)),
													pstrdup(lsecond(replicate_only_table)), -1);

			continue;
		}
	}
}


bool
pglogical_row_filter_hook(struct PGLogicalRowFilterArgs *rowfilter_args)
{
	PGLogicalHooksPrivate *private = (PGLogicalHooksPrivate*)rowfilter_args->private_data;
	PGLogicalTableRepInfo *tblinfo;
	ListCell	   *lc;

	if (private->replicate_only_table)
	{
		/*
		 * Special case - we are catching up just one table.
		 * TODO: performance
		 */
		return strcmp(RelationGetRelationName(rowfilter_args->changed_rel),
					  private->replicate_only_table->relname) == 0 &&
			RelationGetNamespace(rowfilter_args->changed_rel) ==
			get_namespace_oid(private->replicate_only_table->schemaname, true);
	}
	else if (RelationGetRelid(rowfilter_args->changed_rel) == get_queue_table_oid())
	{
		/* Special case - queue table */
		if (rowfilter_args->change_type == REORDER_BUFFER_CHANGE_INSERT)
		{
			HeapTuple		tup = &rowfilter_args->change->data.tp.newtuple->tuple;
			QueuedMessage  *q = queued_message_from_tuple(tup);
			ListCell	   *qlc;

			/*
			 * No replication set means global message, those are always
			 * replicated.
			 */
			if (q->replication_sets == NULL)
				return true;

			foreach (qlc, q->replication_sets)
			{
				char	   *queue_set = (char *) lfirst(qlc);
				ListCell   *plc;

				foreach (plc, private->replication_sets)
				{
					PGLogicalRepSet	   *rs = lfirst(plc);

					/* TODO: this is somewhat ugly. */
					if (strcmp(queue_set, rs->name) == 0 &&
						(q->message_type != QUEUE_COMMAND_TYPE_TRUNCATE ||
						 rs->replicate_truncate))
						return true;
				}
			}
		}

		return false;
	}
	else if (RelationGetRelid(rowfilter_args->changed_rel) == get_replication_set_table_oid())
	{
		/*
		 * Special case - replication set table.
		 *
		 * We can use this to update our cached replication set info, without
		 * having to deal with cache invalidation callbacks.
		 */
		HeapTuple			tup;
		PGLogicalRepSet	   *replicated_set;
		ListCell		   *plc;

		if (rowfilter_args->change_type == REORDER_BUFFER_CHANGE_UPDATE)
			 tup = &rowfilter_args->change->data.tp.newtuple->tuple;
		else if (rowfilter_args->change_type == REORDER_BUFFER_CHANGE_DELETE)
			 tup = &rowfilter_args->change->data.tp.oldtuple->tuple;
		else
			return false;

		replicated_set = replication_set_from_tuple(tup);
		foreach (plc, private->replication_sets)
		{
			PGLogicalRepSet	   *rs = lfirst(plc);

			/* Check if the changed repset is used by us. */
			if (rs->id == replicated_set->id)
			{
				/*
				 * In case this was delete, somebody deleted one of our
				 * rep sets, bail here and let reconnect logic handle any
				 * potential issues.
				 */
				if (rowfilter_args->change_type == REORDER_BUFFER_CHANGE_DELETE)
					elog(ERROR, "replication set \"%s\" used by this connection was deleted, existing",
						 rs->name);

				/* This was update of our repset, update the cache. */
				rs->replicate_insert = replicated_set->replicate_insert;
				rs->replicate_update = replicated_set->replicate_update;
				rs->replicate_delete = replicated_set->replicate_delete;
				rs->replicate_truncate = replicated_set->replicate_truncate;

				return false;
			}
		}

		return false;
	}

	/* Normal case - use replication set membership. */
	tblinfo = get_table_replication_info(private->local_node_id,
										 rowfilter_args->changed_rel,
										 private->replication_sets);

	/* First try filter out by change type. */
	switch (rowfilter_args->change_type)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			if (!tblinfo->replicate_insert)
				return false;
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			if (!tblinfo->replicate_update)
				return false;
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			if (!tblinfo->replicate_delete)
				return false;
			break;
		default:
			elog(ERROR, "Unhandled reorder buffer change type %d",
				 rowfilter_args->change_type);
			return false; /* shut compiler up */
	}

	/*
	 * Proccess row filters.
	 * XXX: we could probably cache some of the executor stuff.
	 */
	if (list_length(tblinfo->row_filter) > 0)
	{
		EState		   *estate;
		ExprContext	   *econtext;
		TupleDesc		tupdesc = RelationGetDescr(rowfilter_args->changed_rel);
		HeapTuple		oldtup = rowfilter_args->change->data.tp.oldtuple ?
			&rowfilter_args->change->data.tp.oldtuple->tuple : NULL;
		HeapTuple		newtup = rowfilter_args->change->data.tp.newtuple ?
			&rowfilter_args->change->data.tp.newtuple->tuple : NULL;

		/* Skip empty changes. */
		if (!newtup && !oldtup)
		{
			elog(DEBUG1, "pglogical output got empty change");
			return false;
		}

		estate = create_estate_for_relation(rowfilter_args->changed_rel, false);
		econtext = prepare_per_tuple_econtext(estate, tupdesc);

		ExecStoreTuple(newtup ? newtup : oldtup, econtext->ecxt_scantuple,
					   InvalidBuffer, false);

		/* Next try the row_filters if there are any. */
		foreach (lc, tblinfo->row_filter)
		{
			Node	   *row_filter = (Node *) lfirst(lc);
			ExprState  *exprstate = pglogical_prepare_row_filter(row_filter);
			Datum		res;
			bool		isnull;

			res = ExecEvalExpr(exprstate, econtext, &isnull, NULL);

			/* NULL is same as false for our use. */
			if (isnull)
				return false;

			if (!DatumGetBool(res))
				return false;
		}

		ExecDropSingleTupleTableSlot(econtext->ecxt_scantuple);
		FreeExecutorState(estate);
	}

	/* Make sure caller is aware of any attribute filter. */
	rowfilter_args->att_filter = tblinfo->att_filter;

	return true;
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
		 * 'forward_origins'.
		 */
		ret = list_length(private->forward_origins) > 0;

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
