/*-------------------------------------------------------------------------
 *
 * pglogical_conflict.c
 * 		Functions for detecting and handling conflicts
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_conflict.c
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

#include "pglogical_proto.h"
#include "pglogical_conflict.h"

int      pglogical_conflict_resolver = PGLOGICAL_RESOLVE_APPLY_REMOTE;

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
 * Find tuple using REPLICA IDENTITY index.
 */
bool
pglogical_tuple_find_replidx(EState *estate, PGLogicalTupleData *tuple,
							 TupleTableSlot *oldslot)
{
	ResultRelInfo  *relinfo = estate->es_result_relation_info;
	Oid				idxoid;
	Relation		idxrel;
	ScanKeyData		index_key[INDEX_MAX_KEYS];
	bool			found;

	/* Open REPLICA IDENTITY index.*/
	idxoid = RelationGetReplicaIndex(relinfo->ri_RelationDesc);
	if (!OidIsValid(idxoid))
	{
		elog(ERROR, "could not find primary key for table with oid %u",
			 RelationGetRelid(relinfo->ri_RelationDesc));
	}
	idxrel = index_open(idxoid, RowExclusiveLock);

	/* Buold scan key for just opened index*/
	build_index_scan_key(index_key, relinfo->ri_RelationDesc, idxrel, tuple);

	/* Try to find the row. */
	found = find_index_tuple(index_key, relinfo->ri_RelationDesc, idxrel,
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
	ScanKeyData	index_key[INDEX_MAX_KEYS];
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

		if (build_index_scan_key(index_key, relinfo->ri_RelationDesc,
								 idxrel, tuple))
			continue;

		/* Try to find conflicting row. */
		found = find_index_tuple(index_key, relinfo->ri_RelationDesc,
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
				errdetail("cannot apply transaction because remotely tuple "
						  "conflicts with a local tuple on more than one "
						  "UNIQUE constraint and/or PRIMARY KEY"),
				errhint("Resolve the conflict by removing or changing the "
						"conflicting local tuple")));
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
conflict_resolve_by_timestamp(RepOriginId local_origin_id,
							  RepOriginId remote_origin_id,
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
 * Get the origin of the local tuple.
 *
 * If the track_commit_timestamp is off, we return remote origin info since
 * there is no way to get any meaningful info locally. This means that
 * the caller will assume that all the local tuples came from remote site when
 * track_commit_timestamp is off.
 *
 * This function is used by UPDATE conflict detection so the above means that
 * UPDATEs will not be recognized as conflict even if they change locally
 * modified row.
 *
 * Returns true if local origin data was found, false if not.
 */
bool
get_tuple_origin(HeapTuple local_tuple, TransactionId *xmin,
				 RepOriginId *local_origin, TimestampTz *local_ts)
{

	if (!track_commit_timestamp)
	{
		*xmin = HeapTupleHeaderGetXmin(local_tuple->t_data);
		*local_origin = replorigin_session_origin;
		*local_ts = replorigin_session_origin_timestamp;
		return false;
	}
	else
	{
		*xmin = HeapTupleHeaderGetXmin(local_tuple->t_data);
		if (TransactionIdIsValid(*xmin) && !TransactionIdIsNormal(*xmin))
		{
			/*
			 * Pg emits an ERROR if you try to pass FrozenTransactionId (2)
			 * or BootstrapTransactionId (1) to TransactionIdGetCommitTsData,
			 * per RT#46983 . This seems like an oversight in the core function,
			 * but we can work around it here by setting it to the same thing
			 * we'd get if the xid's commit timestamp was trimmed already.
			 */
			*local_origin = InvalidRepOriginId;
			*local_ts = 0;
			return false;
		}
		else
			return TransactionIdGetCommitTsData(*xmin, local_ts, local_origin);
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
	TransactionId	xmin;
	TimestampTz		local_ts;
	RepOriginId		local_origin;
	bool			apply = false;

	switch (pglogical_conflict_resolver)
	{
		case PGLOGICAL_RESOLVE_ERROR:
			/* TODO: proper error message */
			elog(ERROR, "cannot apply conflicting row");
			break;
		case PGLOGICAL_RESOLVE_APPLY_REMOTE:
			apply = true;
			*resolution = PGLogicalResolution_ApplyRemote;
			break;
		case PGLOGICAL_RESOLVE_KEEP_LOCAL:
			apply = false;
			*resolution = PGLogicalResolution_KeepLocal;
			break;
		case PGLOGICAL_RESOLVE_LAST_UPDATE_WINS:
			get_tuple_origin(localtuple, &xmin, &local_origin, &local_ts);
			apply = conflict_resolve_by_timestamp(local_origin,
												  replorigin_session_origin,
												  local_ts,
												  replorigin_session_origin_timestamp,
												  true, resolution);
			break;
		case PGLOGICAL_RESOLVE_FIRST_UPDATE_WINS:
			get_tuple_origin(localtuple, &xmin, &local_origin, &local_ts);
			apply = conflict_resolve_by_timestamp(local_origin,
												  replorigin_session_origin,
												  local_ts,
												  replorigin_session_origin_timestamp,
												  false, resolution);
			break;
		default:
			elog(ERROR, "unrecognized pglogical_conflict_resolver setting %d",
				 pglogical_conflict_resolver);
	}

	if (apply)
		*resulttuple = remotetuple;
	else
		*resulttuple = localtuple;

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
 *
 * TODO: provide more detail.
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
					 errmsg("CONFLICT: remote %s on relation %s. Resolution: %s.",
							CONFLICT_INSERT_INSERT ? "INSERT" : "UPDATE",
							quote_qualified_identifier(get_namespace_name(RelationGetNamespace(rel)),
													   RelationGetRelationName(rel)),
							conflict_resolution_to_string(resolution))));
			break;
		case CONFLICT_UPDATE_DELETE:
		case CONFLICT_DELETE_DELETE:
			ereport(LOG,
					(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
					 errmsg("CONFLICT: remote %s on relation %s (tuple not found). Resolution: %s.",
							CONFLICT_UPDATE_DELETE ? "UPDATE" : "DELETE",
							quote_qualified_identifier(get_namespace_name(RelationGetNamespace(rel)),
													   RelationGetRelationName(rel)),
							conflict_resolution_to_string(resolution))));
			break;
	}
}

/* Checks validity of pglogical_conflict_resolver GUC */
bool
pglogical_conflict_resolver_check_hook(int *newval, void **extra,
									   GucSource source)
{
	/*
	 * Only allow PGLOGICAL_RESOLVE_APPLY_REMOTE when track_commit_timestamp
	 * is off, because there is no way to know where the local tuple
	 * originated from.
	 */
	if (!track_commit_timestamp &&
		*newval != PGLOGICAL_RESOLVE_APPLY_REMOTE)
	{
		GUC_check_errdetail("track_commit_timestamp is off");
		return false;
	}

	return true;
}

