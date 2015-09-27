/*-------------------------------------------------------------------------
 *
 * pg_logical_conflict.c
 * 		Functions for detecting and handling conflicts
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pg_logical_conflict.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq-fe.h"

#include "miscadmin.h"

#include "access/commit_ts.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xact.h"

#include "executor/executor.h"

#include "parser/parse_relation.h"

#include "replication/origin.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

#include "pg_logical_proto.h"
#include "pg_logical_conflict.h"

int      pglogical_conflict_resolver = PGLOGICAL_RESOLVE_ERROR;

/*
 * Setup a ScanKey for a search in the relation 'rel' for a tuple 'key' that
 * is setup to match 'rel' (*NOT* idxrel!).
 *
 * Returns whether any column contains NULLs.
 */
static bool
build_index_scan_key(ScanKey skey, Relation rel, Relation idxrel, PGLogicalTupleData *tup)
{
	int			attoff;
	Datum		indclassDatum;
	Datum		indkeyDatum;
	bool		isnull;
	oidvector  *opclass;
	int2vector  *indkey;
	bool		hasnulls = false;

	indclassDatum = SysCacheGetAttr(INDEXRELID, idxrel->rd_indextuple,
									Anum_pg_index_indclass, &isnull);
	Assert(!isnull);
	opclass = (oidvector *) DatumGetPointer(indclassDatum);

	indkeyDatum = SysCacheGetAttr(INDEXRELID, idxrel->rd_indextuple,
									Anum_pg_index_indkey, &isnull);
	Assert(!isnull);
	indkey = (int2vector *) DatumGetPointer(indkeyDatum);


	for (attoff = 0; attoff < RelationGetNumberOfAttributes(idxrel); attoff++)
	{
		Oid			operator;
		Oid			opfamily;
		RegProcedure regop;
		int			pkattno = attoff + 1;
		int			mainattno = indkey->values[attoff];
		Oid			atttype = attnumTypeId(rel, mainattno);
		Oid			optype = get_opclass_input_type(opclass->values[attoff]);

		opfamily = get_opclass_family(opclass->values[attoff]);

		operator = get_opfamily_member(opfamily, optype,
									   optype,
									   BTEqualStrategyNumber);

		if (!OidIsValid(operator))
			elog(ERROR,
				 "could not lookup equality operator for type %u, optype %u in opfamily %u",
				 atttype, optype, opfamily);

		regop = get_opcode(operator);

		/* FIXME: convert type? */
		ScanKeyInit(&skey[attoff],
					pkattno,
					BTEqualStrategyNumber,
					regop,
					tup->values[mainattno - 1]);

		if (tup->nulls[mainattno - 1])
		{
			hasnulls = true;
			skey[attoff].sk_flags |= SK_ISNULL;
		}
	}

	return hasnulls;
}

/*
 * Search the index 'idxrel' for a tuple identified by 'skey' in 'rel'.
 *
 * If a matching tuple is found lock it with lockmode, fill the slot with its
 * contents and return true, return false is returned otherwise.
 */
static bool
find_index_tuple(ScanKey skey, Relation rel, Relation idxrel,
				 LockTupleMode lockmode, TupleTableSlot *slot)
{
	HeapTuple	scantuple;
	bool		found;
	IndexScanDesc scan;
	SnapshotData snap;
	TransactionId xwait;

	InitDirtySnapshot(snap);
	scan = index_beginscan(rel, idxrel, &snap,
						   RelationGetNumberOfAttributes(idxrel),
						   0);

retry:
	found = false;

	index_rescan(scan, skey, RelationGetNumberOfAttributes(idxrel), NULL, 0);

	if ((scantuple = index_getnext(scan, ForwardScanDirection)) != NULL)
	{
		found = true;
		/* FIXME: Improve TupleSlot to not require copying the whole tuple */
		ExecStoreTuple(scantuple, slot, InvalidBuffer, false);
		ExecMaterializeSlot(slot);

		xwait = TransactionIdIsValid(snap.xmin) ?
			snap.xmin : snap.xmax;

		if (TransactionIdIsValid(xwait))
		{
			XactLockTableWait(xwait, NULL, NULL, XLTW_None);
			goto retry;
		}
	}

	if (found)
	{
		Buffer buf;
		HeapUpdateFailureData hufd;
		HTSU_Result res;
		HeapTupleData locktup;

		ItemPointerCopy(&slot->tts_tuple->t_self, &locktup.t_self);

		PushActiveSnapshot(GetLatestSnapshot());

		res = heap_lock_tuple(rel, &locktup, GetCurrentCommandId(false),
							  lockmode,
							  false /* wait */,
							  false /* don't follow updates */,
							  &buf, &hufd);
		/* the tuple slot already has the buffer pinned */
		ReleaseBuffer(buf);

		PopActiveSnapshot();

		switch (res)
		{
			case HeapTupleMayBeUpdated:
				break;
			case HeapTupleUpdated:
				/* XXX: Improve handling here */
				ereport(LOG,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("concurrent update, retrying")));
				goto retry;
			default:
				elog(ERROR, "unexpected HTSU_Result after locking: %u", res);
				break;
		}
	}

	index_endscan(scan);

	return found;
}

/*
 * Open REPLICA IDENTITY index.
 */
static Relation
replindex_open(Relation rel, LOCKMODE lockmode)
{
	Oid			idxoid;

	if (rel->rd_indexvalid == 0)
		RelationGetIndexList(rel);

	idxoid = rel->rd_replidindex;
	if (!OidIsValid(idxoid))
	{
		elog(ERROR, "could not find primary key for table with oid %u",
			 RelationGetRelid(rel));
	}

	/* Now open the primary key index */
	return index_open(idxoid, lockmode);
}

/*
 * Find tuple using REPLICA IDENTITY index.
 */
bool
pglogical_tuple_find_replidx(EState *estate, PGLogicalTupleData *tuple,
							 TupleTableSlot *oldslot)
{
	ResultRelInfo  *relinfo = estate->es_result_relation_info;
	Relation		idxrel = replindex_open(relinfo->ri_RelationDesc,
											RowExclusiveLock);
	ScanKeyData		index_key;
	bool			found;

	build_index_scan_key(&index_key, relinfo->ri_RelationDesc, idxrel, tuple);

	/* Try to find the row. */
	found = find_index_tuple(&index_key, relinfo->ri_RelationDesc, idxrel,
							 LockTupleExclusive, oldslot);

	/* Don't release lock until commit. */
	index_close(idxrel, NoLock);

	return found;
}

/*
 * Find the tuple in a table using any index.
 */
Oid
pglogical_tuple_find_conflict(EState *estate, PGLogicalTupleData *tuple,
							  TupleTableSlot *oldslot)
{
	Oid		conflict_idx = InvalidOid;
	ScanKeyData	index_key;
	int			i;
	ResultRelInfo *relinfo;
	ItemPointerData conflicting_tid;

	ItemPointerSetInvalid(&conflicting_tid);

	relinfo = estate->es_result_relation_info;

	/* Do a SnapshotDirty search for conflicting tuples. */
	for (i = 0; i < relinfo->ri_NumIndices; i++)
	{
		IndexInfo  *ii = relinfo->ri_IndexRelationInfo[i];
		Relation	idxrel;
		bool found = false;

		/*
		 * Only unique indexes are of interest here, and we can't deal with
		 * expression indexes so far. FIXME: predicates should be handled
		 * better.
		 */
		if (!ii->ii_Unique || ii->ii_Expressions != NIL)
			continue;

		idxrel = relinfo->ri_IndexRelationDescs[i];

		if (build_index_scan_key(&index_key, relinfo->ri_RelationDesc,
							 idxrel, tuple))
			continue;

		/* Try to find conflicting row. */
		found = find_index_tuple(&index_key, relinfo->ri_RelationDesc,
								 idxrel, LockTupleExclusive, oldslot);

		/* Alert if there's more than one conflicting unique key, we can't
		 * currently handle that situation. */
		if (found &&
			ItemPointerIsValid(&conflicting_tid) &&
			!ItemPointerEquals(&oldslot->tts_tuple->t_self,
							   &conflicting_tid))
		{
			/* TODO: Report tuple identity in log */
			ereport(ERROR,
				(errcode(ERRCODE_UNIQUE_VIOLATION),
				errmsg("multiple unique constraints violated by remote tuple"),
				errdetail("Cannot apply transaction because remotely tuple "
					  "conflicts with a local tuple on more than one UNIQUE "
					  "constraint and/or PRIMARY KEY"),
				errhint("Resolve the conflict by removing or changing the conflicting "
					"local tuple")));
		}
		else if (found)
		{
			ItemPointerCopy(&oldslot->tts_tuple->t_self, &conflicting_tid);
			conflict_idx = RelationGetRelid(idxrel);
			break;
		}

		CHECK_FOR_INTERRUPTS();
	}

	return conflict_idx;
}


/*
 * Resolve conflict based on commit timestamp.
 */
static bool
conflict_resolve_by_timestamp(RepOriginId local_node_id,
							  RepOriginId remote_node_id,
							  TimestampTz local_ts,
							  TimestampTz remote_ts,
							  bool last_update_wins,
							  PGLogicalConflictResolution *resolution)
{
	int			cmp;

	cmp = timestamptz_cmp_internal(remote_ts, local_ts);

	/*
	 * The logic bellow assumes last update wins, we invert the logic by
	 * inverting result of timestamp comparison if first update wins was
	 * requested.
	 */
	if (!last_update_wins)
		cmp = -cmp;

	if (cmp > 0)
	{
		/* The remote row wins, update the local one. */
		*resolution = PGLogicalResolution_ApplyRemote;
		return true;
	}
	else if (cmp < 0)
	{
		/* The local row wins, retain it */
		*resolution = PGLogicalResolution_KeepLocal;
		return false;
	}
	else
	{
		/*
		 * The timestamps were equal, break the tie in a manner that is
		 * consistent across all nodes.
		 *
		 * XXX: TODO, for now we just always apply remote change.
		 */

		*resolution = PGLogicalResolution_ApplyRemote;
		return true;
	}
}


/*
 * Try resolving the conflict resolution.
 *
 * Returns true when remote tuple should be applied.
 */
bool
try_resolve_conflict(Relation rel, HeapTuple localtuple, HeapTuple remotetuple,
					 HeapTuple *resulttuple,
					 PGLogicalConflictResolution *resolution)
{
	TimestampTz		local_ts;
	RepOriginId		local_id;
	TransactionId	xmin = HeapTupleHeaderGetXmin(localtuple->t_data);
	bool			apply = false;

	TransactionIdGetCommitTsData(xmin, &local_ts, &local_id);

	/* If tuple was written twice in same transaction, apply row */
	if (replorigin_sesssion_origin == local_id)
	{
		*resolution = PGLogicalResolution_ApplyRemote;
		*resulttuple = remotetuple;
		return true;
	}

	switch (pglogical_conflict_resolver)
	{
		case PGLOGICAL_RESOLVE_ERROR:
			/* TODO: proper error message */
			elog(ERROR, "cannot apply conflicting row");
		case PGLOGICAL_RESOLVE_APPLY_REMOTE:
			apply = true;
		case PGLOGICAL_RESOLVE_KEEP_LOCAL:
			apply = false;
		case PGLOGICAL_RESOLVE_LAST_UPDATE_WINS:
			apply = conflict_resolve_by_timestamp(local_id,
												  replorigin_sesssion_origin,
												  local_ts,
												  replorigin_sesssion_origin_timestamp,
												  true, resolution);
		case PGLOGICAL_RESOLVE_FIRST_UPDATE_WINS:
			apply = conflict_resolve_by_timestamp(local_id,
												  replorigin_sesssion_origin,
												  local_ts,
												  replorigin_sesssion_origin_timestamp,
												  false, resolution);
	}

	if (apply)
		*resulttuple = remotetuple;

	return apply;
}

static char *
conflict_type_to_string(PGLogicalConflictType conflict_type)
{
	switch (conflict_type)
	{
		case CONFLICT_INSERT_INSERT:
			return "insert_insert";
		case CONFLICT_UPDATE_UPDATE:
			return "update_update";
		case CONFLICT_UPDATE_DELETE:
			return "update_delete";
		case CONFLICT_DELETE_DELETE:
			return "delete_delete";
	}

	/* Unreachable */
	return NULL;
}

static char *
conflict_resolution_to_string(PGLogicalConflictResolution resolution)
{
	switch (resolution)
	{
		case PGLogicalResolution_ApplyRemote:
			return "apply_remote";
		case PGLogicalResolution_KeepLocal:
			return "keep_local";
		case PGLogicalResolution_Skip:
			return "skip";
	}

	/* Unreachable */
	return NULL;
}

/*
 * Log the conflict to server log.
 */
void
pglogical_report_conflict(PGLogicalConflictType conflict_type, Relation rel,
						  HeapTuple localtuple, HeapTuple remotetuple,
						  HeapTuple applytuple,
						  PGLogicalConflictResolution resolution)
{
	switch (conflict_type)
	{
		case CONFLICT_INSERT_INSERT:
		case CONFLICT_UPDATE_UPDATE:
			ereport(LOG,
					(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
					 errmsg("CONFLICT: remote %s on relation %s originating at node %s at %s. Resolution: %s.",
							CONFLICT_INSERT_INSERT ? "INSERT" : "UPDATE",
							quote_qualified_identifier(get_namespace_name(RelationGetNamespace(rel)),
													   RelationGetRelationName(rel)),
							"node", "ts",
							conflict_resolution_to_string(resolution))));
			break;
		case CONFLICT_UPDATE_DELETE:
		case CONFLICT_DELETE_DELETE:
			ereport(LOG,
					(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
					 errmsg("CONFLICT: remote %s on relation %s originating at node %s at %s. Resolution: %s.",
							CONFLICT_UPDATE_DELETE ? "UPDATE" : "DELETE",
							quote_qualified_identifier(get_namespace_name(RelationGetNamespace(rel)),
													   RelationGetRelationName(rel)),
							"node", "ts",
							conflict_resolution_to_string(resolution))));
			break;
	}
}

/* Checks validity of pglogical_conflict_resolver GUC */
bool
pglogical_conflict_resolver_check_hook(int *newval, void **extra,
									   GucSource source)
{
	if (!track_commit_timestamp &&
		((*newval) == PGLOGICAL_RESOLVE_LAST_UPDATE_WINS ||
		(*newval) == PGLOGICAL_RESOLVE_FIRST_UPDATE_WINS))
	{
		GUC_check_errdetail("track_commit_timestamp is off");
		return false;
	}

	return true;
}

