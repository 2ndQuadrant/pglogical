/*-------------------------------------------------------------------------
 *
 * pglogical_compat.c
 *              compatibility functions (mainly with different PG versions)
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *              pglogical_compat.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"

#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_database.h"
#include "catalog/pg_type.h"

#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

#include "postmaster/bgworker_internals.h"

#include "pglogical_compat.h"
#include "replication/origin.h"
#include "access/commit_ts.h"

#define InvalidRepNodeId 0

XLogRecPtr XactLastCommitEnd = 0;

RepOriginId replorigin_session_origin = InvalidRepNodeId;
XLogRecPtr replorigin_session_origin_lsn = InvalidXLogRecPtr;
TimestampTz replorigin_session_origin_timestamp = 0;

bool track_commit_timestamp = false;

#define Natts_pg_replication_origin				3
#define Anum_pg_replication_origin_roident		1
#define Anum_pg_replication_origin_roname		2
#define Anum_pg_replication_origin_roremote_lsn	3

static Oid ReplicationOriginRelationId = InvalidOid;
static Oid ReplicationOriginIdentIndex = InvalidOid;
static Oid ReplicationOriginNameIndex = InvalidOid;

/*
 * Replay progress of a single remote node.
 */
typedef struct ReplicationState
{
	/*
	 * Local identifier for the remote node.
	 */
	RepOriginId roident;

	/*
	 * Location of the latest commit from the remote side.
	 */
	XLogRecPtr	remote_lsn;

	/*
	 * Remember the local lsn of the commit record so we can XLogFlush() to it
	 * during a checkpoint so we know the commit record actually is safe on
	 * disk.
	 */
	XLogRecPtr	local_lsn;

	/*
	 * Slot is setup in backend?
	 */
	pid_t		acquired_by;

	/*
	 * Lock protecting remote_lsn and local_lsn.
	 */
/*	LWLock		lock;*/
} ReplicationState;

static ReplicationState *session_replication_state = NULL;

static void session_origin_xact_cb(XactEvent event, void *arg);
static void ensure_replication_origin_relid(void);


/*
 * Create a replication origin.
 *
 * Needs to be called in a transaction.
 */
RepOriginId
replorigin_create(char *roname)
{
	Oid			roident;
	HeapTuple	tuple = NULL;
	Relation	rel;
	SnapshotData SnapshotDirty;
	SysScanDesc scan;
	ScanKeyData key;

	Assert(IsTransactionState());

	ensure_replication_origin_relid();

	/*
	 * We need the numeric replication origin to be 16bit wide, so we cannot
	 * rely on the normal oid allocation. Instead we simply scan
	 * pg_replication_origin for the first unused id. That's not particularly
	 * efficient, but this should be a fairly infrequent operation - we can
	 * easily spend a bit more code on this when it turns out it needs to be
	 * faster.
	 *
	 * We handle concurrency by taking an exclusive lock (allowing reads!)
	 * over the table for the duration of the search. Because we use a "dirty
	 * snapshot" we can read rows that other in-progress sessions have
	 * written, even though they would be invisible with normal snapshots. Due
	 * to the exclusive lock there's no danger that new rows can appear while
	 * we're checking.
	 */
	InitDirtySnapshot(SnapshotDirty);

	rel = heap_open(ReplicationOriginRelationId, ExclusiveLock);

	for (roident = InvalidOid + 1; roident < UINT16_MAX; roident++)
	{
		bool		nulls[Natts_pg_replication_origin];
		Datum		values[Natts_pg_replication_origin];
		bool		collides;

		CHECK_FOR_INTERRUPTS();

		ScanKeyInit(&key,
					Anum_pg_replication_origin_roident,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(roident));

		scan = systable_beginscan(rel, ReplicationOriginIdentIndex,
								  true /* indexOK */ ,
								  &SnapshotDirty,
								  1, &key);

		collides = HeapTupleIsValid(systable_getnext(scan));

		systable_endscan(scan);

		if (!collides)
		{
			/*
			 * Ok, found an unused roident, insert the new row and do a CCI,
			 * so our callers can look it up if they want to.
			 */
			memset(&nulls, 0, sizeof(nulls));

			values[Anum_pg_replication_origin_roident - 1] =
				ObjectIdGetDatum(roident);
			values[Anum_pg_replication_origin_roname - 1] =
				CStringGetTextDatum(roname);
			values[Anum_pg_replication_origin_roremote_lsn - 1] =
				LSNGetDatum(InvalidXLogRecPtr);

			tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
			simple_heap_insert(rel, tuple);
			CatalogUpdateIndexes(rel, tuple);
			CommandCounterIncrement();
			break;
		}
	}

	/* now release lock again,	*/
	heap_close(rel, ExclusiveLock);

	if (tuple == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("could not find free replication origin OID")));

	heap_freetuple(tuple);
	return roident;
}

/*
 * Drop replication origin.
 *
 * Needs to be called in a transaction.
 */
void
replorigin_drop(RepOriginId roident)
{
	HeapTuple	tuple = NULL;
	Relation	rel;
	SnapshotData SnapshotDirty;
	SysScanDesc scan;
	ScanKeyData key;

	Assert(IsTransactionState());

	ensure_replication_origin_relid();

	InitDirtySnapshot(SnapshotDirty);

	rel = heap_open(ReplicationOriginRelationId, ExclusiveLock);

	/* Find and delete tuple from name table */
	ScanKeyInit(&key,
				Anum_pg_replication_origin_roident,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(roident));

	scan = systable_beginscan(rel, ReplicationOriginIdentIndex,
							  true /* indexOK */,
							  &SnapshotDirty,
							  1, &key);

	tuple = systable_getnext(scan);

	if (HeapTupleIsValid(tuple))
		simple_heap_delete(rel, &tuple->t_self);

	systable_endscan(scan);

	CommandCounterIncrement();

	/* now release lock again,	*/
	heap_close(rel, ExclusiveLock);
}

RepOriginId
replorigin_by_name(char *name, bool missing_ok)
{
	HeapTuple	tuple = NULL;
	Relation	rel;
	Snapshot	snap;
	SysScanDesc scan;
	ScanKeyData key;
	Oid			roident = InvalidOid;

	ensure_replication_origin_relid();

	snap = RegisterSnapshot(GetLatestSnapshot());
	rel = heap_open(ReplicationOriginRelationId, RowExclusiveLock);

	ScanKeyInit(&key,
				Anum_pg_replication_origin_roname,
				BTEqualStrategyNumber, F_TEXTEQ,
				CStringGetTextDatum(name));

	scan = systable_beginscan(rel, ReplicationOriginNameIndex,
							  true /* indexOK */,
							  snap,
							  1, &key);

	tuple = systable_getnext(scan);

	if (HeapTupleIsValid(tuple))
	{
		Datum		values[Natts_pg_replication_origin];
		bool		nulls[Natts_pg_replication_origin];

		heap_deform_tuple(tuple, RelationGetDescr(rel),
						  values, nulls);
		roident = DatumGetObjectId(values[Anum_pg_replication_origin_roident - 1]);
	}
	else if (!missing_ok)
		elog(ERROR, "cache lookup failed for replication origin named %s",
			 name);

	systable_endscan(scan);
	UnregisterSnapshot(snap);
	heap_close(rel, RowExclusiveLock);

	return roident;
}

void
replorigin_session_setup(RepOriginId node)
{
	Relation		rel;
	SysScanDesc		scan;
	ScanKeyData		key;
	HeapTuple		tuple;
	bool			start_transaction = !IsTransactionState();
	XLogRecPtr		remote_lsn = InvalidXLogRecPtr,
					local_lsn = InvalidXLogRecPtr;

	Assert(node != InvalidRepNodeId);

	if (session_replication_state != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
		errmsg("cannot setup replication origin when one is already setup")));

	if (start_transaction)
		StartTransactionCommand();

	ensure_replication_origin_relid();

	rel = heap_open(ReplicationOriginRelationId, RowExclusiveLock);

	ScanKeyInit(&key,
				Anum_pg_replication_origin_roident,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(node));

	scan = systable_beginscan(rel, ReplicationOriginIdentIndex,
							  true, NULL, 1, &key);
	tuple = systable_getnext(scan);

	if (HeapTupleIsValid(tuple))
	{
		Datum		values[Natts_pg_replication_origin];
		bool		nulls[Natts_pg_replication_origin];

		heap_deform_tuple(tuple, RelationGetDescr(rel),
						  values, nulls);
		remote_lsn =
			DatumGetLSN(values[Anum_pg_replication_origin_roremote_lsn - 1]);
		local_lsn = XactLastCommitEnd;
	}

	systable_endscan(scan);
	heap_close(rel, RowExclusiveLock);

	if (start_transaction)
		CommitTransactionCommand();

	session_replication_state = (ReplicationState *) palloc(sizeof(ReplicationState));
	session_replication_state->roident = node;
	session_replication_state->remote_lsn = remote_lsn;
	session_replication_state->local_lsn = local_lsn;

	RegisterXactCallback(session_origin_xact_cb, NULL);
}

void
replorigin_session_reset(void)
{
	if (session_replication_state == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("no replication origin is configured")));

	UnregisterXactCallback(session_origin_xact_cb, NULL);

	session_replication_state->acquired_by = 0;
	session_replication_state = NULL;
}

/*
 * Ask the machinery about the point up to which we successfully replayed
 * changes from an already setup replication origin.
 */
XLogRecPtr
replorigin_session_get_progress(bool flush)
{
	XLogRecPtr	remote_lsn;
	XLogRecPtr	local_lsn;

	Assert(session_replication_state != NULL);

	remote_lsn = session_replication_state->remote_lsn;
	local_lsn = session_replication_state->local_lsn;

	if (flush && local_lsn != InvalidXLogRecPtr)
		XLogFlush(local_lsn);

	return remote_lsn;
}

void
replorigin_advance(RepOriginId node,
				   XLogRecPtr remote_commit,
				   XLogRecPtr local_commit,
				   bool go_backward, bool wal_log)
{
	HeapTuple	tuple = NULL;
	Relation	rel;
	SnapshotData SnapshotDirty;
	SysScanDesc scan;
	ScanKeyData key;

	Assert(node != InvalidRepOriginId);
	Assert(IsTransactionState());

	if (node == DoNotReplicateId)
		return;

	ensure_replication_origin_relid();

	InitDirtySnapshot(SnapshotDirty);

	rel = heap_open(ReplicationOriginRelationId, ExclusiveLock);

	/* Find and delete tuple from name table */
	ScanKeyInit(&key,
				Anum_pg_replication_origin_roident,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(node));

	scan = systable_beginscan(rel, ReplicationOriginIdentIndex,
							  true /* indexOK */,
							  &SnapshotDirty,
							  1, &key);

	tuple = systable_getnext(scan);

	if (HeapTupleIsValid(tuple))
	{
		HeapTuple	newtuple;
		Datum		values[Natts_pg_replication_origin];
		bool		nulls[Natts_pg_replication_origin];

		heap_deform_tuple(tuple, RelationGetDescr(rel),
							  values, nulls);

		values[Anum_pg_replication_origin_roremote_lsn - 1] =
			LSNGetDatum(remote_commit);

		newtuple = heap_form_tuple(RelationGetDescr(rel),
								   values, nulls);
		simple_heap_update(rel, &tuple->t_self, newtuple);
		CatalogUpdateIndexes(rel, newtuple);
	}

	systable_endscan(scan);

	CommandCounterIncrement();

	/* now release lock again,	*/
	heap_close(rel, ExclusiveLock);

	return;
}

static void
replorigin_session_advance(XLogRecPtr remote_commit, XLogRecPtr local_commit)
{
	Assert(session_replication_state != NULL);
	Assert(session_replication_state->roident != InvalidRepOriginId);

	if (session_replication_state->local_lsn < local_commit)
		session_replication_state->local_lsn = local_commit;
	if (session_replication_state->remote_lsn < remote_commit)
		session_replication_state->remote_lsn = remote_commit;

	replorigin_advance(session_replication_state->roident, remote_commit,
					   local_commit, false, true);
}

static void
session_origin_xact_cb(XactEvent event, void *arg)
{
	if (event == XACT_EVENT_PRE_COMMIT &&
		session_replication_state != NULL &&
		replorigin_session_origin != InvalidRepOriginId &&
		replorigin_session_origin != DoNotReplicateId)
	{
		replorigin_session_advance(replorigin_session_origin_lsn,
								   XactLastCommitEnd);
	}
}

static void
ensure_replication_origin_relid(void)
{
	if (ReplicationOriginRelationId == InvalidOid)
	{
		Oid	schema_oid = get_namespace_oid("pglogical_origin", true);

		if (schema_oid == InvalidOid)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("pglogical_origin extension not found")));

		ReplicationOriginRelationId =
			get_relname_relid("replication_origin", schema_oid);
		ReplicationOriginIdentIndex =
			get_relname_relid("replication_origin_roident_index", schema_oid);
		ReplicationOriginNameIndex =
			get_relname_relid("replication_origin_roname_index", schema_oid);
	}
}

/*
 * Connect background worker to a database using OIDs.
 */
void
BackgroundWorkerInitializeConnectionByOid(Oid dboid, Oid useroid)
{
	BackgroundWorker *worker = MyBgworkerEntry;

	/* XXX is this the right errcode? */
	if (!(worker->bgw_flags & BGWORKER_BACKEND_DATABASE_CONNECTION))
		ereport(FATAL,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("database connection requirement not indicated during registration")));

	InitPostgres(NULL, dboid, NULL, NULL);

	/* it had better not gotten out of "init" mode yet */
	if (!IsInitProcessingMode())
		ereport(ERROR,
				(errmsg("invalid processing mode in background worker")));
	SetProcessingMode(NormalProcessing);
}

bool
TransactionIdGetCommitTsData(TransactionId xid,
							 TimestampTz *ts, RepOriginId *nodeid)
{
	elog(ERROR, "TransactionIdGetCommitTsData is not implemented yet");
	return false;
}


/*
 * Auxiliary function to return a TEXT array out of a list of C-strings.
 */
ArrayType *
strlist_to_textarray(List *list)
{
	ArrayType  *arr;
	Datum	   *datums;
	int			j = 0;
	ListCell   *cell;
	MemoryContext memcxt;
	MemoryContext oldcxt;

	memcxt = AllocSetContextCreate(CurrentMemoryContext,
								   "strlist to array",
								   ALLOCSET_DEFAULT_MINSIZE,
								   ALLOCSET_DEFAULT_INITSIZE,
								   ALLOCSET_DEFAULT_MAXSIZE);
	oldcxt = MemoryContextSwitchTo(memcxt);

	datums = palloc(sizeof(text *) * list_length(list));
	foreach(cell, list)
	{
		char	   *name = lfirst(cell);

		datums[j++] = CStringGetTextDatum(name);
	}

	MemoryContextSwitchTo(oldcxt);

	arr = construct_array(datums, list_length(list),
						  TEXTOID, -1, false, 'i');
	MemoryContextDelete(memcxt);

	return arr;
}
