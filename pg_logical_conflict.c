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

#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

#include "pg_logical_proto.h"
#include "pg_logical_conflict.h"


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
 * Find the tuple in a table.
 *
 * This uses the replication identity index for search.
 */
bool
pg_logical_tuple_find(Relation rel, Relation idxrel, PGLogicalTupleData *tuple,
					  TupleTableSlot *oldslot)
{
	ScanKeyData	index_key;

	build_index_scan_key(&index_key, rel, idxrel, tuple);

	/* Try to find the row. */
	return find_index_tuple(&index_key, rel, idxrel, LockTupleExclusive, oldslot);
}

/*
 * Detect if remote INSERT or UPDATE conflicts with existing unique constraint.
 */
Oid
pg_logical_tuple_conflict(EState *estate, PGLogicalTupleData *tuple,
						  bool insert, TupleTableSlot *oldslot)
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

		/* REPLICA IDENTITY index conflict is only interesting for INSERTs. */
		idxrel = relinfo->ri_IndexRelationDescs[i];
		if (!insert && relinfo->ri_RelationDesc->rd_replidindex == RelationGetRelid(idxrel))
			continue;

		if (build_index_scan_key(&index_key, relinfo->ri_RelationDesc,
							 idxrel, tuple))
			continue;

		Assert(ii->ii_Expressions == NIL);

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
				errmsg("multiple unique constraints violated by remote %s",
					   insert ? "INSERT" : "UPDATE"),
				errdetail("Cannot apply transaction because remotely %s "
					  "conflicts with a local tuple on more than one UNIQUE "
					  "constraint and/or PRIMARY KEY",
					  insert ? "INSERT" : "UPDATE"),
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
		*resolution = LogicalCR_KeepRemote;
		return true;
	}
	else if (cmp < 0)
	{
		/* The local row wins, retain it */
		*resolution = LogicalCR_KeepLocal;
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

		*resolution = LogicalCR_KeepRemote;
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
					 bool insert, HeapTuple *resulttuple,
					 PGLogicalConflictResolution *resolution)
{
	TimestampTz		local_ts;
	RepOriginId		local_id;
	TransactionId	xmin = HeapTupleHeaderGetXmin(localtuple->t_data);

	TransactionIdGetCommitTsData(xmin, &local_ts, &local_id);

	/* If tuple was written twice in same transaction, apply row */
	if (replorigin_sesssion_origin == local_id)
	{
		*resolution = LogicalCR_KeepRemote;
		return true;
	}

	/* Decide by commit timestamp. */
	return conflict_resolve_by_timestamp(local_id,
										 replorigin_sesssion_origin,
										 local_ts,
										 replorigin_sesssion_origin_timestamp,
										 true, resolution);
}
