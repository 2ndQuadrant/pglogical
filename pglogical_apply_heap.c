/*-------------------------------------------------------------------------
 *
 * pglogical_apply_heap.c
 * 		pglogical apply functions using heap api
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_apply_heap.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "libpq-fe.h"
#include "pgstat.h"

#include "access/htup_details.h"
#include "access/xact.h"

#include "catalog/namespace.h"

#include "commands/dbcommands.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "commands/trigger.h"

#include "executor/executor.h"

#include "libpq/pqformat.h"

#include "mb/pg_wchar.h"

#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"

#include "optimizer/clauses.h"
#include "optimizer/planner.h"

#include "replication/origin.h"

#include "rewrite/rewriteHandler.h"

#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/proc.h"

#include "tcop/pquery.h"
#include "tcop/utility.h"

#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

#include "pglogical_conflict.h"
#include "pglogical_executor.h"
#include "pglogical_node.h"
#include "pglogical_proto_native.h"
#include "pglogical_queue.h"
#include "pglogical_relcache.h"
#include "pglogical_repset.h"
#include "pglogical_rpc.h"
#include "pglogical_sync.h"
#include "pglogical_worker.h"
#include "pglogical_apply_heap.h"

typedef struct ApplyExecState {
	EState			   *estate;
	EPQState			epqstate;
	ResultRelInfo	   *resultRelInfo;
	TupleTableSlot	   *slot;
} ApplyExecState;

/* State related to bulk insert */
typedef struct ApplyMIState
{
	PGLogicalRelation  *rel;
	ApplyExecState	   *aestate;

	CommandId			cid;
	BulkInsertState		bistate;

	HeapTuple		   *buffered_tuples;
	int					maxbuffered_tuples;
	int					nbuffered_tuples;
} ApplyMIState;


static ApplyMIState *pglmistate = NULL;

void
pglogical_apply_heap_begin(void)
{
}

void
pglogical_apply_heap_commit(void)
{
}


static List *
UserTableUpdateOpenIndexes(EState *estate, TupleTableSlot *slot)
{
	List	   *recheckIndexes = NIL;

	if (estate->es_result_relation_info->ri_NumIndices > 0)
	{
		recheckIndexes = ExecInsertIndexTuples(slot,
											   &slot->tts_tuple->t_self,
											   estate
#if PG_VERSION_NUM >= 90500
											   , false, NULL, NIL
#endif
											   );

		/* FIXME: recheck the indexes */
		if (recheckIndexes != NIL)
		{
			StringInfoData si;
			ListCell *lc;
			const char *idxname, *relname, *nspname;
			Relation target_rel = estate->es_result_relation_info->ri_RelationDesc;

			relname = RelationGetRelationName(target_rel);
			nspname = get_namespace_name(RelationGetNamespace(target_rel));

			initStringInfo(&si);
			foreach (lc, recheckIndexes)
			{
				Oid idxoid = lfirst_oid(lc);
				idxname = get_rel_name(idxoid);
				if (idxname == NULL)
					elog(ERROR, "cache lookup failed for index oid %u", idxoid);
				if (si.len > 0)
					appendStringInfoString(&si, ", ");
				appendStringInfoString(&si, quote_identifier(idxname));
			}

			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("pglogical doesn't support deferrable indexes"),
					 errdetail("relation %s.%s has deferrable indexes: %s",
								quote_identifier(nspname),
								quote_identifier(relname),
								si.data)));
		}

		list_free(recheckIndexes);
	}

	return recheckIndexes;
}

static bool
physatt_in_attmap(PGLogicalRelation *rel, int attid)
{
	AttrNumber	i;

	for (i = 0; i < rel->natts; i++)
		if (rel->attmap[i] == attid)
			return true;

	return false;
}

/*
 * Executes default values for columns for which we didn't get any data.
 *
 * TODO: this needs caching, it's not exactly fast.
 */
static void
fill_missing_defaults(PGLogicalRelation *rel, EState *estate,
					  PGLogicalTupleData *tuple)
{
	TupleDesc	desc = RelationGetDescr(rel->rel);
	AttrNumber	num_phys_attrs = desc->natts;
	int			i;
	AttrNumber	attnum,
				num_defaults = 0;
	int		   *defmap;
	ExprState **defexprs;
	ExprContext *econtext;

	econtext = GetPerTupleExprContext(estate);

	/* We got all the data via replication, no need to evaluate anything. */
	if (num_phys_attrs == rel->natts)
		return;

	defmap = (int *) palloc(num_phys_attrs * sizeof(int));
	defexprs = (ExprState **) palloc(num_phys_attrs * sizeof(ExprState *));

	for (attnum = 0; attnum < num_phys_attrs; attnum++)
	{
		Expr	   *defexpr;

		if (TupleDescAttr(desc,attnum)->attisdropped)
			continue;

		if (physatt_in_attmap(rel, attnum))
			continue;

		defexpr = (Expr *) build_column_default(rel->rel, attnum + 1);

		if (defexpr != NULL)
		{
			/* Run the expression through planner */
			defexpr = expression_planner(defexpr);

			/* Initialize executable expression in copycontext */
			defexprs[num_defaults] = ExecInitExpr(defexpr, NULL);
			defmap[num_defaults] = attnum;
			num_defaults++;
		}

	}

	for (i = 0; i < num_defaults; i++)
		tuple->values[defmap[i]] = ExecEvalExpr(defexprs[i],
												econtext,
												&tuple->nulls[defmap[i]],
												NULL);
}

static ApplyExecState *
init_apply_exec_state(PGLogicalRelation *rel)
{
	ApplyExecState	   *aestate = palloc0(sizeof(ApplyExecState));

	/* Initialize the executor state. */
	aestate->estate = create_estate_for_relation(rel->rel, true);
	aestate->resultRelInfo = aestate->estate->es_result_relation_info;

	aestate->slot = ExecInitExtraTupleSlot(aestate->estate);
	ExecSetSlotDescriptor(aestate->slot, RelationGetDescr(rel->rel));

	if (aestate->resultRelInfo->ri_TrigDesc)
		EvalPlanQualInit(&aestate->epqstate, aestate->estate, NULL, NIL, -1);

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	return aestate;
}


static void
finish_apply_exec_state(ApplyExecState *aestate)
{
	/* Close indexes */
	ExecCloseIndices(aestate->resultRelInfo);

	/* Handle queued AFTER triggers. */
	AfterTriggerEndQuery(aestate->estate);

	/* Terminate EPQ execution if active. */
	EvalPlanQualEnd(&aestate->epqstate);

	/* Cleanup tuple table. */
	ExecResetTupleTable(aestate->estate->es_tupleTable, true);

	/* Free the memory. */
	FreeExecutorState(aestate->estate);
	pfree(aestate);
}

/*
 * Handle insert via low level api.
 */
void
pglogical_apply_heap_insert(PGLogicalRelation *rel, PGLogicalTupleData *newtup)
{
	ApplyExecState	   *aestate;
	Oid					conflicts_idx_id;
	TupleTableSlot	   *localslot;
	HeapTuple			remotetuple;
	HeapTuple			applytuple;
	PGLogicalConflictResolution resolution;
	List			   *recheckIndexes = NIL;
	MemoryContext		oldctx;
	bool				has_before_triggers = false;

	/* Initialize the executor state. */
	aestate = init_apply_exec_state(rel);
	localslot = ExecInitExtraTupleSlot(aestate->estate);
	ExecSetSlotDescriptor(localslot, RelationGetDescr(rel->rel));

	/* Get snapshot */
	PushActiveSnapshot(GetTransactionSnapshot());

	ExecOpenIndices(aestate->resultRelInfo
#if PG_VERSION_NUM >= 90500
					, false
#endif
					);

	/*
	 * Check for existing tuple with same key in any unique index containing
	 * only normal columns. This doesn't just check the replica identity index,
	 * but it'll prefer it and use it first.
	 */
	conflicts_idx_id = pglogical_tuple_find_conflict(aestate->estate,
													 newtup,
													 localslot);

	/* Process and store remote tuple in the slot */
	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(aestate->estate));
	fill_missing_defaults(rel, aestate->estate, newtup);
	remotetuple = heap_form_tuple(RelationGetDescr(rel->rel),
								  newtup->values, newtup->nulls);
	MemoryContextSwitchTo(oldctx);
	ExecStoreTuple(remotetuple, aestate->slot, InvalidBuffer, true);

	if (aestate->resultRelInfo->ri_TrigDesc &&
		aestate->resultRelInfo->ri_TrigDesc->trig_insert_before_row)
	{
		has_before_triggers = true;

		aestate->slot = ExecBRInsertTriggers(aestate->estate,
											 aestate->resultRelInfo,
											 aestate->slot);

		if (aestate->slot == NULL)		/* "do nothing" */
		{
			PopActiveSnapshot();
			finish_apply_exec_state(aestate);
			return;
		}

	}

	/* trigger might have changed tuple */
	remotetuple = ExecMaterializeSlot(aestate->slot);

	/* Did we find matching key in any candidate-key index? */
	if (OidIsValid(conflicts_idx_id))
	{
		TransactionId		xmin;
		TimestampTz			local_ts;
		RepOriginId			local_origin;
		bool				apply;
		bool				local_origin_found;

		local_origin_found = get_tuple_origin(localslot->tts_tuple, &xmin,
											  &local_origin, &local_ts);

		/* Tuple already exists, try resolving conflict. */
		apply = try_resolve_conflict(rel->rel, localslot->tts_tuple,
									 remotetuple, &applytuple,
									 &resolution);

		pglogical_report_conflict(CONFLICT_INSERT_INSERT, rel,
								  localslot->tts_tuple, NULL, remotetuple,
								  applytuple, resolution, xmin,
								  local_origin_found, local_origin,
								  local_ts, conflicts_idx_id,
								  has_before_triggers);

		if (apply)
		{
			if (applytuple != remotetuple)
				ExecStoreTuple(applytuple, aestate->slot, InvalidBuffer, false);

			if (aestate->resultRelInfo->ri_TrigDesc &&
				aestate->resultRelInfo->ri_TrigDesc->trig_update_before_row)
			{
				aestate->slot = ExecBRUpdateTriggers(aestate->estate,
													 &aestate->epqstate,
													 aestate->resultRelInfo,
													 &localslot->tts_tuple->t_self,
													 NULL,
													 aestate->slot);

				if (aestate->slot == NULL)		/* "do nothing" */
				{
					PopActiveSnapshot();
					finish_apply_exec_state(aestate);
					return;
				}

			}

			/* trigger might have changed tuple */
			remotetuple = ExecMaterializeSlot(aestate->slot);

			/* Check the constraints of the tuple */
			if (rel->rel->rd_att->constr)
				ExecConstraints(aestate->resultRelInfo, aestate->slot,
								aestate->estate);

			simple_heap_update(rel->rel, &localslot->tts_tuple->t_self,
							   aestate->slot->tts_tuple);

			if (!HeapTupleIsHeapOnly(aestate->slot->tts_tuple))
				recheckIndexes = UserTableUpdateOpenIndexes(aestate->estate,
															aestate->slot);

			/* AFTER ROW UPDATE Triggers */
			ExecARUpdateTriggers(aestate->estate, aestate->resultRelInfo,
								 &localslot->tts_tuple->t_self,
								 NULL, applytuple, recheckIndexes);
		}
	}
	else
	{
		/* Check the constraints of the tuple */
		if (rel->rel->rd_att->constr)
			ExecConstraints(aestate->resultRelInfo, aestate->slot,
							aestate->estate);

		simple_heap_insert(rel->rel, aestate->slot->tts_tuple);
		UserTableUpdateOpenIndexes(aestate->estate, aestate->slot);

		/* AFTER ROW INSERT Triggers */
		ExecARInsertTriggers(aestate->estate, aestate->resultRelInfo,
							 remotetuple, recheckIndexes);
	}

	PopActiveSnapshot();
	finish_apply_exec_state(aestate);

	CommandCounterIncrement();
}


/*
 * Handle update via low level api.
 */
void
pglogical_apply_heap_update(PGLogicalRelation *rel, PGLogicalTupleData *oldtup,
							PGLogicalTupleData *newtup)
{
	ApplyExecState	   *aestate;
	bool				found;
	TupleTableSlot	   *localslot;
	HeapTuple			remotetuple;
	List			   *recheckIndexes = NIL;
	MemoryContext		oldctx;
	Oid					replident_idx_id;
	bool				has_before_triggers = false;

	/* Initialize the executor state. */
	aestate = init_apply_exec_state(rel);
	localslot = ExecInitExtraTupleSlot(aestate->estate);
	ExecSetSlotDescriptor(localslot, RelationGetDescr(rel->rel));

	PushActiveSnapshot(GetTransactionSnapshot());

	/* Search for existing tuple with same key */
	found = pglogical_tuple_find_replidx(aestate->estate, oldtup, localslot,
										 &replident_idx_id);

	/*
	 * Tuple found, update the local tuple.
	 *
	 * Note this will fail if there are other unique indexes and one or more of
	 * them would be violated by the new tuple.
	 */
	if (found)
	{
		TransactionId	xmin;
		TimestampTz		local_ts;
		RepOriginId		local_origin;
		bool			local_origin_found;
		bool			apply;
		HeapTuple		applytuple;

		/* Process and store remote tuple in the slot */
		oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(aestate->estate));
		fill_missing_defaults(rel, aestate->estate, newtup);
		remotetuple = heap_modify_tuple(localslot->tts_tuple,
										RelationGetDescr(rel->rel),
										newtup->values,
										newtup->nulls,
										newtup->changed);
		MemoryContextSwitchTo(oldctx);
		ExecStoreTuple(remotetuple, aestate->slot, InvalidBuffer, true);


		if (aestate->resultRelInfo->ri_TrigDesc &&
			aestate->resultRelInfo->ri_TrigDesc->trig_update_before_row)
		{
			has_before_triggers = true;

			aestate->slot = ExecBRUpdateTriggers(aestate->estate,
												 &aestate->epqstate,
												 aestate->resultRelInfo,
												 &localslot->tts_tuple->t_self,
												 NULL, aestate->slot);

			if (aestate->slot == NULL)		/* "do nothing" */
			{
				PopActiveSnapshot();
				finish_apply_exec_state(aestate);
				return;
			}
		}

		/* trigger might have changed tuple */
		remotetuple = ExecMaterializeSlot(aestate->slot);

		local_origin_found = get_tuple_origin(localslot->tts_tuple, &xmin,
											  &local_origin, &local_ts);

		/*
		 * If the local tuple was previously updated by different transaction
		 * on different server, consider this to be conflict and resolve it.
		 */
		if (local_origin_found &&
			xmin != GetTopTransactionId() &&
			local_origin != replorigin_session_origin)
		{
			PGLogicalConflictResolution resolution;

			apply = try_resolve_conflict(rel->rel, localslot->tts_tuple,
										 remotetuple, &applytuple,
										 &resolution);

			pglogical_report_conflict(CONFLICT_UPDATE_UPDATE, rel,
									  localslot->tts_tuple, oldtup,
									  remotetuple, applytuple, resolution,
									  xmin, local_origin_found, local_origin,
									  local_ts, replident_idx_id,
									  has_before_triggers);

			if (applytuple != remotetuple)
				ExecStoreTuple(applytuple, aestate->slot, InvalidBuffer, false);
		}
		else
		{
			apply = true;
			applytuple = remotetuple;
		}

		if (apply)
		{
			/* Check the constraints of the tuple */
			if (rel->rel->rd_att->constr)
				ExecConstraints(aestate->resultRelInfo, aestate->slot,
								aestate->estate);

			simple_heap_update(rel->rel, &localslot->tts_tuple->t_self,
							   aestate->slot->tts_tuple);

			/* Only update indexes if it's not HOT update. */
			if (!HeapTupleIsHeapOnly(aestate->slot->tts_tuple))
			{
				ExecOpenIndices(aestate->resultRelInfo
#if PG_VERSION_NUM >= 90500
								, false
#endif
							   );
				recheckIndexes = UserTableUpdateOpenIndexes(aestate->estate,
															aestate->slot);
			}

			/* AFTER ROW UPDATE Triggers */
			ExecARUpdateTriggers(aestate->estate, aestate->resultRelInfo,
								 &localslot->tts_tuple->t_self,
								 NULL, applytuple, recheckIndexes);
		}
	}
	else
	{
		/*
		 * The tuple to be updated could not be found.
		 *
		 * We can't do INSERT here because we might not have whole tuple.
		 */
		remotetuple = heap_form_tuple(RelationGetDescr(rel->rel),
									  newtup->values,
									  newtup->nulls);
		pglogical_report_conflict(CONFLICT_UPDATE_DELETE, rel, NULL, oldtup,
								  remotetuple, NULL, PGLogicalResolution_Skip,
								  InvalidTransactionId, false,
								  InvalidRepOriginId, (TimestampTz)0,
								  replident_idx_id, has_before_triggers);
	}

	/* Cleanup. */
	PopActiveSnapshot();
	finish_apply_exec_state(aestate);

	CommandCounterIncrement();
}

/*
 * Handle delete via low level api.
 */
void
pglogical_apply_heap_delete(PGLogicalRelation *rel, PGLogicalTupleData *oldtup)
{
	ApplyExecState	   *aestate;
	TupleTableSlot	   *localslot;
	Oid					replident_idx_id;
	bool				has_before_triggers = false;

	/* Initialize the executor state. */
	aestate = init_apply_exec_state(rel);
	localslot = ExecInitExtraTupleSlot(aestate->estate);
	ExecSetSlotDescriptor(localslot, RelationGetDescr(rel->rel));

	PushActiveSnapshot(GetTransactionSnapshot());

	if (pglogical_tuple_find_replidx(aestate->estate, oldtup, localslot,
									 &replident_idx_id))
	{
		if (aestate->resultRelInfo->ri_TrigDesc &&
			aestate->resultRelInfo->ri_TrigDesc->trig_delete_before_row)
		{
			bool dodelete = ExecBRDeleteTriggers(aestate->estate,
												 &aestate->epqstate,
												 aestate->resultRelInfo,
												 &localslot->tts_tuple->t_self,
												 NULL);

			has_before_triggers = true;

			if (!dodelete)		/* "do nothing" */
			{
				PopActiveSnapshot();
				finish_apply_exec_state(aestate);
				pglogical_relation_close(rel, NoLock);
				return;
			}
		}

		/* Tuple found, delete it. */
		simple_heap_delete(rel->rel, &localslot->tts_tuple->t_self);

		/* AFTER ROW DELETE Triggers */
		ExecARDeleteTriggers(aestate->estate, aestate->resultRelInfo,
							 &localslot->tts_tuple->t_self, NULL);
	}
	else
	{
		/* The tuple to be deleted could not be found. */
		HeapTuple remotetuple = heap_form_tuple(RelationGetDescr(rel->rel),
												oldtup->values, oldtup->nulls);
		pglogical_report_conflict(CONFLICT_DELETE_DELETE, rel, NULL, oldtup,
								  remotetuple, NULL, PGLogicalResolution_Skip,
								  InvalidTransactionId, false,
								  InvalidRepOriginId, (TimestampTz)0,
								  replident_idx_id, has_before_triggers);
	}

	/* Cleanup. */
	PopActiveSnapshot();
	finish_apply_exec_state(aestate);

	CommandCounterIncrement();
}


bool
pglogical_apply_heap_can_mi(PGLogicalRelation *rel)
{
	/* Multi insert is only supported when conflicts result in errors. */
	return pglogical_conflict_resolver == PGLOGICAL_RESOLVE_ERROR;
}

/*
 * MultiInsert initialization.
 */
static void
pglogical_apply_heap_mi_start(PGLogicalRelation *rel)
{
	MemoryContext	oldctx;
	ApplyExecState *aestate;
	ResultRelInfo  *resultRelInfo;
	TupleDesc		desc;
	bool			volatile_defexprs = false;

	if (pglmistate && pglmistate->rel == rel)
		return;

	if (pglmistate && pglmistate->rel != rel)
		pglogical_apply_heap_mi_finish(pglmistate->rel);

	oldctx = MemoryContextSwitchTo(TopTransactionContext);

	/* Initialize new MultiInsert state. */
	pglmistate = palloc0(sizeof(ApplyMIState));

	pglmistate->rel = rel;

	/* Initialize the executor state. */
	pglmistate->aestate = aestate = init_apply_exec_state(rel);
	MemoryContextSwitchTo(TopTransactionContext);
	resultRelInfo = aestate->resultRelInfo;

	ExecOpenIndices(resultRelInfo
#if PG_VERSION_NUM >= 90500
					, false
#endif
					);

	/* Check if table has any volatile default expressions. */
	desc = RelationGetDescr(rel->rel);
	if (desc->natts != rel->natts)
	{
		int			attnum;

		for (attnum = 0; attnum < desc->natts; attnum++)
		{
			Expr	   *defexpr;

			if (TupleDescAttr(desc,attnum)->attisdropped)
				continue;

			defexpr = (Expr *) build_column_default(rel->rel, attnum + 1);

			if (defexpr != NULL)
			{
				/* Run the expression through planner */
				defexpr = expression_planner(defexpr);
				volatile_defexprs = contain_volatile_functions_not_nextval((Node *) defexpr);

				if (volatile_defexprs)
					break;
			}
		}
	}

	/*
	 * Decide if to buffer tuples based on the collected information
	 * about the table.
	 */
	if ((resultRelInfo->ri_TrigDesc != NULL &&
		 (resultRelInfo->ri_TrigDesc->trig_insert_before_row ||
		  resultRelInfo->ri_TrigDesc->trig_insert_instead_row)) ||
		volatile_defexprs)
	{
		pglmistate->maxbuffered_tuples = 1;
	}
	else
	{
		pglmistate->maxbuffered_tuples = 1000;
	}

	pglmistate->cid = GetCurrentCommandId(true);
	pglmistate->bistate = GetBulkInsertState();

	/* Make the space for buffer. */
	pglmistate->buffered_tuples = palloc0(pglmistate->maxbuffered_tuples * sizeof(HeapTuple));
	pglmistate->nbuffered_tuples = 0;

	MemoryContextSwitchTo(oldctx);
}

/* Write the buffered tuples. */
static void
pglogical_apply_heap_mi_flush(void)
{
	MemoryContext	oldctx;
	ResultRelInfo  *resultRelInfo;
	int				i;

	if (!pglmistate || pglmistate->nbuffered_tuples == 0)
		return;

	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(pglmistate->aestate->estate));
	heap_multi_insert(pglmistate->rel->rel,
					  pglmistate->buffered_tuples,
					  pglmistate->nbuffered_tuples,
					  pglmistate->cid,
					  0, /* hi_options */
					  pglmistate->bistate);
	MemoryContextSwitchTo(oldctx);

	resultRelInfo = pglmistate->aestate->resultRelInfo;

	/*
	 * If there are any indexes, update them for all the inserted tuples, and
	 * run AFTER ROW INSERT triggers.
	 */
	if (resultRelInfo->ri_NumIndices > 0)
	{
		for (i = 0; i < pglmistate->nbuffered_tuples; i++)
		{
			List	   *recheckIndexes;

			ExecStoreTuple(pglmistate->buffered_tuples[i],
						   pglmistate->aestate->slot,
						   InvalidBuffer, false);
			recheckIndexes =
				ExecInsertIndexTuples(pglmistate->aestate->slot,
									  &(pglmistate->buffered_tuples[i]->t_self),
									  pglmistate->aestate->estate
#if PG_VERSION_NUM >= 90500
									  , false, NULL, NIL
#endif
									 );
			ExecARInsertTriggers(pglmistate->aestate->estate, resultRelInfo,
								 pglmistate->buffered_tuples[i],
								 recheckIndexes);
			list_free(recheckIndexes);
		}
	}

	/*
	 * There's no indexes, but see if we need to run AFTER ROW INSERT triggers
	 * anyway.
	 */
	else if (resultRelInfo->ri_TrigDesc != NULL &&
			 resultRelInfo->ri_TrigDesc->trig_insert_after_row)
	{
		for (i = 0; i < pglmistate->nbuffered_tuples; i++)
		{
			ExecARInsertTriggers(pglmistate->aestate->estate, resultRelInfo,
								 pglmistate->buffered_tuples[i],
								 NIL);
		}
	}

	pglmistate->nbuffered_tuples = 0;
}

/* Add tuple to the MultiInsert. */
void
pglogical_apply_heap_mi_add_tuple(PGLogicalRelation *rel,
								  PGLogicalTupleData *tup)
{
	MemoryContext	oldctx;
	ApplyExecState *aestate;
	HeapTuple		remotetuple;
	TupleTableSlot *slot;

	pglogical_apply_heap_mi_start(rel);

	/*
	 * If sufficient work is pending, process that first
	 */
	if (pglmistate->nbuffered_tuples >= pglmistate->maxbuffered_tuples)
		pglogical_apply_heap_mi_flush();

	/* Process and store remote tuple in the slot */
	aestate = pglmistate->aestate;

	if (pglmistate->nbuffered_tuples == 0)
	{
		/*
		 * Reset the per-tuple exprcontext. We can only do this if the
		 * tuple buffer is empty. (Calling the context the per-tuple
		 * memory context is a bit of a misnomer now.)
		 */
		ResetPerTupleExprContext(aestate->estate);
	}

	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(aestate->estate));
	fill_missing_defaults(rel, aestate->estate, tup);
	remotetuple = heap_form_tuple(RelationGetDescr(rel->rel),
								  tup->values, tup->nulls);
	MemoryContextSwitchTo(TopTransactionContext);
	slot = aestate->slot;
	/* Store the tuple in slot, but make sure it's not freed. */
	ExecStoreTuple(remotetuple, slot, InvalidBuffer, false);

	if (aestate->resultRelInfo->ri_TrigDesc &&
		aestate->resultRelInfo->ri_TrigDesc->trig_insert_before_row)
	{
		slot = ExecBRInsertTriggers(aestate->estate,
									aestate->resultRelInfo,
									slot);

		if (slot == NULL)
		{
			MemoryContextSwitchTo(oldctx);
			return;
		}
		else
			remotetuple = ExecMaterializeSlot(slot);
	}

	/* Check the constraints of the tuple */
	if (rel->rel->rd_att->constr)
		ExecConstraints(aestate->resultRelInfo, slot,
						aestate->estate);

	pglmistate->buffered_tuples[pglmistate->nbuffered_tuples++] = remotetuple;
	MemoryContextSwitchTo(oldctx);
}

void
pglogical_apply_heap_mi_finish(PGLogicalRelation *rel)
{
	if (!pglmistate)
		return;

	Assert(pglmistate->rel == rel);

	pglogical_apply_heap_mi_flush();

	FreeBulkInsertState(pglmistate->bistate);

	finish_apply_exec_state(pglmistate->aestate);

	pfree(pglmistate->buffered_tuples);
	pfree(pglmistate);

	pglmistate = NULL;
}
