/*-------------------------------------------------------------------------
 *
 * pglogical_apply.c
 * 		pglogical apply logic
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "libpq-fe.h"
#include "pgstat.h"

#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "access/xlogdefs.h"

#include "commands/dbcommands.h"

#include "executor/executor.h"

#include "lib/stringinfo.h"

#include "libpq/pqformat.h"

#include "mb/pg_wchar.h"

#include "postmaster/bgworker.h"

#include "replication/origin.h"

#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/shm_toc.h"
#include "storage/spin.h"

#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "pglogical_proto.h"
#include "pglogical_relcache.h"
#include "pglogical_node.h"
#include "pglogical_repset.h"
#include "pglogical_conflict.h"
#include "pglogical.h"


volatile sig_atomic_t got_SIGTERM = false;
static bool			in_remote_transaction = false;
static XLogRecPtr	remote_origin_lsn = InvalidXLogRecPtr;
static RepOriginId	remote_origin_id = InvalidRepOriginId;

static PGLogicalApplyWorker *MyApplyWorker;
static PGLogicalDBState	   *MyDBState;


static void
ensure_transaction(void)
{
	if (IsTransactionState())
		return;

	StartTransactionCommand();
}

static void
handle_begin(StringInfo s)
{
	XLogRecPtr		commit_lsn;
	TimestampTz		commit_time;
	TransactionId	remote_xid;

	pglogical_read_begin(s, &commit_lsn, &commit_time, &remote_xid);

	replorigin_session_origin_timestamp = commit_time;
	replorigin_session_origin_lsn = commit_lsn;

	in_remote_transaction = true;
}

/*
 * Handle COMMIT message.
 */
static void
handle_commit(StringInfo s)
{
	XLogRecPtr		commit_lsn;
	XLogRecPtr		end_lsn;
	TimestampTz		commit_time;

	pglogical_read_commit(s, &commit_lsn, &end_lsn, &commit_time);

	Assert(commit_lsn == replorigin_session_origin_lsn);
	Assert(commit_time == replorigin_session_origin_timestamp);

	if (IsTransactionState())
		CommitTransactionCommand();

	/*
	 * Advance the local replication identifier's lsn, so we don't replay this
	 * transaction again.
	 */
	replorigin_session_advance(end_lsn, XactLastCommitEnd);

	/*
	 * If the row isn't from the immediate upstream; advance the slot of the
	 * node it originally came from so we start replay of that node's
	 * change data at the right place.
	 */
	if (remote_origin_id != InvalidRepOriginId &&
		remote_origin_id != replorigin_session_origin)
	{
		replorigin_advance(remote_origin_id, remote_origin_lsn,
						   XactLastCommitEnd, false, false /* XXX ? */);
	}

	in_remote_transaction = false;
}

/*
 * Handle ORIGIN message.
 */
static void
handle_origin(StringInfo s)
{
	char		   *origin;

	/*
	 * ORIGIN message can only come inside remote transaction and before
	 * any actual writes.
	 */
	if (!in_remote_transaction || IsTransactionState())
		elog(ERROR, "ORIGIN message sent out of order");

	origin = pglogical_read_origin(s, &remote_origin_lsn);
	remote_origin_id = replorigin_by_name(origin, false);
}

/*
 * Handle RELATION message.
 *
 * Note we don't do validation against local schema here. The validation is
 * posponed until first change for given relation comes.
 */
static void
handle_relation(StringInfo s)
{
	(void) pglogical_read_rel(s);
}


static EState *
create_estate_for_relation(Relation rel)
{
	EState	   *estate;
	ResultRelInfo *resultRelInfo;

	estate = CreateExecutorState();

	resultRelInfo = makeNode(ResultRelInfo);
	resultRelInfo->ri_RangeTableIndex = 1;		/* dummy */
	resultRelInfo->ri_RelationDesc = rel;
	resultRelInfo->ri_TrigInstrument = NULL;

	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;

	return estate;
}

static void
UserTableUpdateOpenIndexes(EState *estate, TupleTableSlot *slot)
{
	/* HOT update does not require index inserts */
	if (HeapTupleIsHeapOnly(slot->tts_tuple))
		return;

	if (estate->es_result_relation_info->ri_NumIndices > 0)
	{
		List	   *recheckIndexes = NIL;
		recheckIndexes = ExecInsertIndexTuples(slot,
											   &slot->tts_tuple->t_self,
											   estate, false, NULL, NIL);

		if (recheckIndexes != NIL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("pglogical doesn't support index rechecks")));

		/* FIXME: recheck the indexes */
		list_free(recheckIndexes);
	}
}


static void
handle_insert(StringInfo s)
{
	PGLogicalTupleData	newtup;
	PGLogicalRelation  *rel;
	EState			   *estate;
	Oid					conflicts;
	TupleTableSlot	   *localslot,
					   *applyslot;
	HeapTuple			remotetuple;
	HeapTuple			applytuple;
	PGLogicalConflictResolution resolution;

	ensure_transaction();

	rel = pglogical_read_insert(s, RowExclusiveLock, &newtup);

	/* Initialize the executor state. */
	estate = create_estate_for_relation(rel->rel);
	localslot = ExecInitExtraTupleSlot(estate);
	applyslot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(localslot, RelationGetDescr(rel->rel));
	ExecSetSlotDescriptor(applyslot, RelationGetDescr(rel->rel));

	ExecOpenIndices(estate->es_result_relation_info, false);

	PushActiveSnapshot(GetTransactionSnapshot());

	conflicts = pglogical_tuple_find_conflict(estate, &newtup, localslot);

	remotetuple = heap_form_tuple(RelationGetDescr(rel->rel),
								  newtup.values, newtup.nulls);

	if (OidIsValid(conflicts))
	{
		/* Tuple already exists, try resolving conflict. */
		bool apply = try_resolve_conflict(rel->rel, localslot->tts_tuple,
										  remotetuple, &applytuple,
										  &resolution);

		pglogical_report_conflict(CONFLICT_INSERT_INSERT, rel->rel,
								  localslot->tts_tuple, remotetuple,
								  applytuple, resolution);

		if (apply)
		{
			ExecStoreTuple(applytuple, applyslot, InvalidBuffer, true);
			simple_heap_update(rel->rel, &localslot->tts_tuple->t_self,
							   applytuple);
			/* TODO: check for HOT update? */
			UserTableUpdateOpenIndexes(estate, applyslot);
		}
	}
	else
	{
		/* No conflict, insert the tuple. */
		ExecStoreTuple(remotetuple, applyslot, InvalidBuffer, true);
		simple_heap_insert(rel->rel, remotetuple);
		UserTableUpdateOpenIndexes(estate, applyslot);
	}

	/* Cleanup */
	ExecCloseIndices(estate->es_result_relation_info);
	PopActiveSnapshot();
	pglogical_relation_close(rel, NoLock);
	ExecResetTupleTable(estate->es_tupleTable, true);
	FreeExecutorState(estate);

	CommandCounterIncrement();
}

static void
handle_update(StringInfo s)
{
	PGLogicalTupleData	oldtup;
	PGLogicalTupleData	newtup;
	PGLogicalTupleData *searchtup;
	PGLogicalRelation  *rel;
	EState			   *estate;
	bool				found;
	bool				hasoldtup;
	TupleTableSlot	   *localslot,
					   *applyslot;
	HeapTuple			remotetuple;

	ensure_transaction();

	rel = pglogical_read_update(s, RowExclusiveLock, &hasoldtup, &oldtup,
								&newtup);

	/* Initialize the executor state. */
	estate = create_estate_for_relation(rel->rel);
	localslot = ExecInitExtraTupleSlot(estate);
	applyslot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(localslot, RelationGetDescr(rel->rel));
	ExecSetSlotDescriptor(applyslot, RelationGetDescr(rel->rel));

	PushActiveSnapshot(GetTransactionSnapshot());

	searchtup = hasoldtup ? &oldtup : &newtup;
	found = pglogical_tuple_find_replidx(estate, searchtup, localslot);

	remotetuple = heap_form_tuple(RelationGetDescr(rel->rel),
								  newtup.values, newtup.nulls);

	if (found)
	{
		/*
		 * Tuple found.
		 *
		 * TODO: handle conflicts.
		 */
		ExecStoreTuple(remotetuple, applyslot, InvalidBuffer, true);
		simple_heap_update(rel->rel, &localslot->tts_tuple->t_self,
						   remotetuple);
		/* Only update indexes if it's not HOT update. */
		if (!HeapTupleIsHeapOnly(applyslot->tts_tuple))
		{
			ExecOpenIndices(estate->es_result_relation_info, false);
			UserTableUpdateOpenIndexes(estate, applyslot);
			ExecCloseIndices(estate->es_result_relation_info);
		}
	}
	else
	{
		/* The tuple to be updated could not be found. */
		pglogical_report_conflict(CONFLICT_UPDATE_DELETE, rel->rel, NULL,
								  remotetuple, NULL, PGLogicalResolution_Skip);
	}

	/* Cleanup. */
	PopActiveSnapshot();
	pglogical_relation_close(rel, NoLock);
	ExecResetTupleTable(estate->es_tupleTable, true);
	FreeExecutorState(estate);

	CommandCounterIncrement();
}

static void
handle_delete(StringInfo s)
{
	PGLogicalTupleData	oldtup;
	PGLogicalRelation  *rel;
	EState			   *estate;
	TupleTableSlot	   *localslot;

	ensure_transaction();

	rel = pglogical_read_delete(s, RowExclusiveLock, &oldtup);

	/* Initialize the executor state. */
	estate = create_estate_for_relation(rel->rel);
	localslot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(localslot, RelationGetDescr(rel->rel));

	PushActiveSnapshot(GetTransactionSnapshot());

	if (pglogical_tuple_find_replidx(estate, &oldtup, localslot))
	{
		/* Tuple found, delete it. */
		simple_heap_delete(rel->rel, &localslot->tts_tuple->t_self);
	}
	else
	{
		/* The tuple to be deleted could not be found. */
		HeapTuple remotetuple = heap_form_tuple(RelationGetDescr(rel->rel),
												oldtup.values, oldtup.nulls);
		pglogical_report_conflict(CONFLICT_DELETE_DELETE, rel->rel, NULL,
								  remotetuple, NULL, PGLogicalResolution_Skip);
	}

	PopActiveSnapshot();

	/* Cleanup. */
	pglogical_relation_close(rel, NoLock);
	ExecResetTupleTable(estate->es_tupleTable, true);
	FreeExecutorState(estate);

	CommandCounterIncrement();
}


static void
replication_handler(StringInfo s)
{
	char action = pq_getmsgbyte(s);

	switch (action)
	{
		/* BEGIN */
		case 'B':
			handle_begin(s);
			break;
		/* COMMIT */
		case 'C':
			handle_commit(s);
			break;
		/* ORIGIN */
		case 'O':
			handle_origin(s);
			break;
		/* RELATION */
		case 'R':
			handle_relation(s);
			break;
		/* INSERT */
		case 'I':
			handle_insert(s);
			break;
		/* UPDATE */
		case 'U':
			handle_update(s);
			break;
		/* DELETE */
		case 'D':
			handle_delete(s);
			break;
		/* STARTUP MESSAGE */
		case 'S':
			break;
		default:
			elog(ERROR, "unknown action of type %c", action);
	}
}

/*
 * Send a Standby Status Update message to server.
 *
 * 'recvpos' is the latest LSN we've received data to, force is set if we need
 * to send a response to avoid timeouts.
 */
static bool
send_feedback(PGconn *conn, XLogRecPtr recvpos, int64 now, bool force)
{
	static StringInfo	reply_message = NULL;

	static XLogRecPtr last_recvpos = InvalidXLogRecPtr;
	static XLogRecPtr last_writepos = InvalidXLogRecPtr;
	static XLogRecPtr last_flushpos = InvalidXLogRecPtr;

	XLogRecPtr writepos;
	XLogRecPtr flushpos;

	/* It's legal to not pass a recvpos */
	if (recvpos < last_recvpos)
		recvpos = last_recvpos;

	flushpos = writepos = recvpos;

	if (writepos < last_writepos)
		writepos = last_writepos;

	if (flushpos < last_flushpos)
		flushpos = last_flushpos;

	/* if we've already reported everything we're good */
	if (!force &&
		writepos == last_writepos &&
		flushpos == last_flushpos)
		return true;

	if (!reply_message)
		reply_message = makeStringInfo();
	else
		resetStringInfo(reply_message);

	pq_sendbyte(reply_message, 'r');
	pq_sendint64(reply_message, recvpos);		/* write */
	pq_sendint64(reply_message, flushpos);		/* flush */
	pq_sendint64(reply_message, writepos);		/* apply */
	pq_sendint64(reply_message, now);			/* sendTime */
	pq_sendbyte(reply_message, false);			/* replyRequested */

	elog(DEBUG2, "sending feedback (force %d) to recv %X/%X, write %X/%X, flush %X/%X",
		 force,
		 (uint32) (recvpos >> 32), (uint32) recvpos,
		 (uint32) (writepos >> 32), (uint32) writepos,
		 (uint32) (flushpos >> 32), (uint32) flushpos
		);


	if (PQputCopyData(conn, reply_message->data, reply_message->len) <= 0 ||
		PQflush(conn))
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not send feedback packet: %s",
						PQerrorMessage(conn))));
		return false;
	}

	if (recvpos > last_recvpos)
		last_recvpos = recvpos;
	if (writepos > last_writepos)
		last_writepos = writepos;
	if (flushpos > last_flushpos)
		last_flushpos = flushpos;

	return true;
}

/*
 * Apply main loop.
 */
static void
apply_work(PGconn *streamConn)
{
	int			fd;
	char	   *copybuf = NULL;
	XLogRecPtr	last_received = InvalidXLogRecPtr;

	fd = PQsocket(streamConn);

	/* mark as idle, before starting to loop */
	pgstat_report_activity(STATE_IDLE, NULL);

	while (!got_SIGTERM)
	{
		int			rc;
		int			r;

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = WaitLatchOrSocket(&MyProc->procLatch,
							   WL_SOCKET_READABLE | WL_LATCH_SET |
							   WL_TIMEOUT | WL_POSTMASTER_DEATH,
							   fd, 1000L);

		ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		if (PQstatus(streamConn) == CONNECTION_BAD)
		{
			elog(ERROR, "connection to other side has died");
		}

		if (rc & WL_SOCKET_READABLE)
			PQconsumeInput(streamConn);

		for (;;)
		{
			if (got_SIGTERM)
				break;

			if (copybuf != NULL)
			{
				PQfreemem(copybuf);
				copybuf = NULL;
			}

			r = PQgetCopyData(streamConn, &copybuf, 1);

			if (r == -1)
			{
				elog(ERROR, "data stream ended");
			}
			else if (r == -2)
			{
				elog(ERROR, "could not read COPY data: %s",
					 PQerrorMessage(streamConn));
			}
			else if (r < 0)
				elog(ERROR, "invalid COPY status %d", r);
			else if (r == 0)
			{
				/* need to wait for new data */
				break;
			}
			else
			{
				int c;
				StringInfoData s;

//				MemoryContextSwitchTo(MessageContext);

				initStringInfo(&s);
				s.data = copybuf;
				s.len = r;
				s.maxlen = -1;

				c = pq_getmsgbyte(&s);

				if (c == 'w')
				{
					XLogRecPtr	start_lsn;
					XLogRecPtr	end_lsn;

					start_lsn = pq_getmsgint64(&s);
					end_lsn = pq_getmsgint64(&s);
					pq_getmsgint64(&s); /* sendTime */

					if (last_received < start_lsn)
						last_received = start_lsn;

					if (last_received < end_lsn)
						last_received = end_lsn;

					replication_handler(&s);
				}
				else if (c == 'k')
				{
					XLogRecPtr endpos;
					bool reply_requested;

					endpos = pq_getmsgint64(&s);
					/* timestamp = */ pq_getmsgint64(&s);
					reply_requested = pq_getmsgbyte(&s);

					send_feedback(streamConn, endpos,
								  GetCurrentTimestamp(),
								  reply_requested);
				}
				/* other message types are purposefully ignored */
			}

		}
	}
}

void
pglogical_apply_main(Datum main_arg)
{
	PGconn		   *streamConn;
	PGresult	   *res;
	char		   *sqlstate;
	StringInfoData	conninfo_repl;
	StringInfoData	command;
	RepOriginId		originid;
	XLogRecPtr		origin_startpos;
	NameData		slot_name;
	dsm_segment	   *seg;
	shm_toc		   *toc;
	int				applyworkernr = -1;
	PGLogicalDBState	   *state;
	PGLogicalApplyWorker   *apply;
	PGLogicalConnection	   *conn;
	PGLogicalNode		   *origin_node;

	/* Establish signal handlers. */
//	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/* Attach to dsm segment. */
	Assert(CurrentResourceOwner == NULL);
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "pglogical apply");

	seg = dsm_attach(DatumGetUInt32(main_arg));
	if (seg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("unable to map dynamic shared memory segment")));
	toc = shm_toc_attach(PGLOGICAL_MASTER_TOC_MAGIC, dsm_segment_address(seg));
	if (toc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			   errmsg("bad magic number in dynamic shared memory segment")));

	state = shm_toc_lookup(toc, PGLOGICAL_MASTER_TOC_STATE);
	apply = shm_toc_lookup(toc, PGLOGICAL_MASTER_TOC_APPLY);
	SpinLockAcquire(&state->mutex);
	if (state->apply_attached < state->apply_total)
		applyworkernr = state->apply_attached++;
	SpinLockRelease(&state->mutex);
	if (applyworkernr < 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("too many apply workers already attached")));

	MyDBState = state;
	MyApplyWorker = &apply[applyworkernr];

	/* TODO */
	BackgroundWorkerInitializeConnection("postgres", NULL);

	StartTransactionCommand();
	conn = get_node_connection(MyApplyWorker->connid);
	origin_node = conn->origin;

	elog(DEBUG1, "conneting to node %d (%s), dsn %s",
		 origin_node->id, origin_node->name, origin_node->dsn);

	initStringInfo(&conninfo_repl);
	appendStringInfo(&conninfo_repl, "%s replication=database fallback_application_name='%s_apply'",
					 origin_node->dsn, origin_node->name);

	streamConn = PQconnectdb(conninfo_repl.data);
	if (PQstatus(streamConn) != CONNECTION_OK)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not connect to the upstream server: %s",
						PQerrorMessage(streamConn)),
				 errdetail("Connection string is '%s'", conninfo_repl.data)));
	}

	/* Setup the origin and get the starting position for the replication. */
	gen_slot_name(&slot_name, get_database_name(MyDatabaseId),
				  conn->origin, conn->target);

	originid = replorigin_by_name(NameStr(slot_name), false);
	replorigin_session_setup(originid);
	origin_startpos = replorigin_session_get_progress(false);

	/* Start the replication. */
	initStringInfo(&command);
	appendStringInfo(&command, "START_REPLICATION SLOT \"%s\" LOGICAL %X/%X (",
					 NameStr(slot_name),
					 (uint32) (origin_startpos >> 32),
					 (uint32) origin_startpos);

	appendStringInfo(&command, "expected_encoding '%s'", GetDatabaseEncodingName());
	appendStringInfo(&command, ", min_proto_version '1'");
	appendStringInfo(&command, ", max_proto_version '1'");
	appendStringInfo(&command, ", pg_version '%u'", PG_VERSION_NUM);

	/* Binary protocol compatibility. */
	appendStringInfo(&command, ", binary.want_binary_basetypes '1'");
	appendStringInfo(&command, ", binary.want_sendrecv_basetypes '1'");
	appendStringInfo(&command, ", binary.basetypes_major_version '%u'", PG_VERSION_NUM/100);
	appendStringInfo(&command, ", binary.sizeof_datum '%zu'", sizeof(Datum));
	appendStringInfo(&command, ", binary.sizeof_int '%zu'", sizeof(int));
	appendStringInfo(&command, ", binary.sizeof_long '%zu'", sizeof(long));
	appendStringInfo(&command, ", binary.bigendian '%d'",
#ifdef WORDS_BIGENDIAN
					 true
#else
					 false
#endif
					 );
	appendStringInfo(&command, ", binary.float4_byval '%d'",
#ifdef USE_FLOAT4_BYVAL
					 true
#else
					 false
#endif
					 );
	appendStringInfo(&command, ", binary.float8_byval '%d'",
#ifdef USE_FLOAT8_BYVAL
					 true
#else
					 false
#endif
					 );
	appendStringInfo(&command, ", binary.integer_datetimes '%d'",
#ifdef USE_INTEGER_DATETIMES
					 true
#else
					 false
#endif
					 );

	/* Table filter hook (replication set handling). */
	appendStringInfo(&command, ", hooks.origin_filter 'pglogical.origin_filter'");
	/* Currently we forward all changes. */
	appendStringInfo(&command, ", hooks.origin_filter_arg '%s'",
					 REPLICATION_ORIGIN_ALL);

	/* Table filter hook (replication set handling). */
	appendStringInfo(&command, ", hooks.table_filter 'pglogical.table_filter'");
	appendStringInfo(&command, ", hooks.table_filter_arg '%s'",
					 conn->target->name); /* No need to escape, it's Name. */

	appendStringInfoChar(&command, ')');

	res = PQexec(streamConn, command.data);
	sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
	if (PQresultStatus(res) != PGRES_COPY_BOTH)
		elog(FATAL, "could not send replication command \"%s\": %s\n, sqlstate: %s",
			 command.data, PQresultErrorMessage(res), sqlstate);
	PQclear(res);

	CommitTransactionCommand();

	apply_work(streamConn);

	/*
	 * never exit gracefully (as that'd unregister the worker) unless
	 * explicitly asked to do so.
	 */
	proc_exit(1);
}
