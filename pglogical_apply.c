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

#include "access/htup_details.h"
#include "access/xact.h"

#include "catalog/namespace.h"

#include "commands/dbcommands.h"
#include "commands/tablecmds.h"

#include "executor/executor.h"

#include "libpq/pqformat.h"

#include "mb/pg_wchar.h"

#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"

#include "replication/origin.h"

#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/proc.h"

#include "tcop/pquery.h"
#include "tcop/utility.h"

#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

#include "pglogical_conflict.h"
#include "pglogical_node.h"
#include "pglogical_proto.h"
#include "pglogical_queue.h"
#include "pglogical_relcache.h"
#include "pglogical_repset.h"
#include "pglogical_worker.h"
#include "pglogical.h"


void pglogical_apply_main(Datum main_arg);

static bool			in_remote_transaction = false;
static XLogRecPtr	remote_origin_lsn = InvalidXLogRecPtr;
static RepOriginId	remote_origin_id = InvalidRepOriginId;

static Oid			QueueRelid = InvalidOid;

static void handle_queued_message(HeapTuple msgtup, bool tx_just_started);


static bool
ensure_transaction(void)
{
	if (IsTransactionState())
		return false;

	StartTransactionCommand();
	return true;
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
	bool				started_tx = ensure_transaction();

	rel = pglogical_read_insert(s, RowExclusiveLock, &newtup);

	/* Initialize the executor state. */
	estate = create_estate_for_relation(rel->rel);
	localslot = ExecInitExtraTupleSlot(estate);
	applyslot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(localslot, RelationGetDescr(rel->rel));
	ExecSetSlotDescriptor(applyslot, RelationGetDescr(rel->rel));

	ExecOpenIndices(estate->es_result_relation_info, false);

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

	ExecCloseIndices(estate->es_result_relation_info);

	/* if INSERT was into our queue, process the message. */
	if (RelationGetRelid(rel->rel) == QueueRelid)
	{
		HeapTuple		ht;
		LockRelId		lockid = rel->rel->rd_lockInfo.lockRelId;
		TransactionId	oldxid = GetTopTransactionId();
		Relation		qrel;

		/*
		 * Release transaction bound resources for CONCURRENTLY support.
		 */
		MemoryContextSwitchTo(MessageContext);
		ht = heap_copytuple(applyslot->tts_tuple);

		LockRelationIdForSession(&lockid, RowExclusiveLock);
		pglogical_relation_close(rel, NoLock);

		ExecResetTupleTable(estate->es_tupleTable, true);
		FreeExecutorState(estate);

		handle_queued_message(ht, started_tx);

		qrel = heap_open(QueueRelid, RowExclusiveLock);

		UnlockRelationIdForSession(&lockid, RowExclusiveLock);

		heap_close(qrel, NoLock);

		if (oldxid != GetTopTransactionId())
			CommitTransactionCommand();
	}
	else
	{
		/* Otherwise do normal cleanup. */
		pglogical_relation_close(rel, NoLock);
		ExecResetTupleTable(estate->es_tupleTable, true);
		FreeExecutorState(estate);
	}

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

	/*
	 * Tuple found.
	 *
	 * Note this will fail if there are other conflicting unique indexes.
	 */
	if (found)
	{
		TransactionId	xmin;
		TimestampTz		local_ts;
		RepOriginId		local_origin;
		bool			apply;
		HeapTuple		applytuple;

		get_tuple_origin(localslot->tts_tuple, &xmin, &local_origin,
						 &local_ts);

		/*
		 * If the local tuple was previously updated by different transaction
		 * on different server, consider this to be conflict and resolve it.
		 */
		if (xmin != GetTopTransactionId() &&
			local_origin != replorigin_session_origin)
		{
			PGLogicalConflictResolution resolution;

			apply = try_resolve_conflict(rel->rel, localslot->tts_tuple,
										 remotetuple, &applytuple,
										 &resolution);

			pglogical_report_conflict(CONFLICT_INSERT_INSERT, rel->rel,
									  localslot->tts_tuple, remotetuple,
									  applytuple, resolution);
		}
		else
		{
			apply = true;
			applytuple = remotetuple;
		}

		if (apply)
		{
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
	}
	else
	{
		/*
		 * The tuple to be updated could not be found.
		 *
		 * We can't do INSERT here because we might not have whole tuple.
		 */
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

/*
 * Handle TRUNCATE message comming via queue table.
 *
 * Note we do truncate filtering here in rhe downstream for performance
 * reasons.
 */
static void
handle_truncate(QueuedMessage *queued_message, bool tx_just_started)
{
	JsonbIterator  *it;
	JsonbValue		v;
	int				r;
	int				level = 0;
	char		   *key = NULL;
	char		  **parse_res = NULL;
	char		   *nspname = NULL;
	char		   *relname = NULL;
	RangeVar	   *rv;
	Oid				relid;
	TruncateStmt   *truncate;

	/* Parse and validate the json message. */
	if (!JB_ROOT_IS_OBJECT(queued_message->message))
		elog(ERROR, "malformed message in queued message tuple: root is not object");

	it = JsonbIteratorInit(&queued_message->message->root);
	while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
	{
		if (level == 0 && r != WJB_BEGIN_OBJECT)
			elog(ERROR, "root element needs to be an object");
		else if (level == 0 && r == WJB_BEGIN_OBJECT)
		{
			level++;
		}
		else if (level == 1 && r == WJB_KEY)
		{
			if (strncmp(v.val.string.val, "schema_name", v.val.string.len) == 0)
				parse_res = &nspname;
			else if (strncmp(v.val.string.val, "table_name", v.val.string.len) == 0)
				parse_res = &relname;
			else
				elog(ERROR, "unexpected key: %s",
					 pnstrdup(v.val.string.val, v.val.string.len));

			key = v.val.string.val;
		}
		else if (level == 1 && r == WJB_VALUE)
		{
			if (!key)
				elog(ERROR, "in wrong state when parsing key");

			if (v.type != jbvString)
				elog(ERROR, "unexpected type for key '%s': %u", key, v.type);

			*parse_res = pnstrdup(v.val.string.val, v.val.string.len);
		}
		else if (level == 1 && r != WJB_END_OBJECT)
		{
			elog(ERROR, "unexpected content: %u at level %d", r, level);
		}
		else if (r == WJB_END_OBJECT)
		{
			level--;
			parse_res = NULL;
			key = NULL;
		}
		else
			elog(ERROR, "unexpected content: %u at level %d", r, level);

	}

	/* Check if we got both schema and table names. */
	if (!nspname)
		elog(ERROR, "missing schema_name in TRUNCATE message");

	if (!relname)
		elog(ERROR, "missing table_name in TRUNCATE message");

	/*
	 * If table doesn't exist locally, it can't be subscribed.
	 *
	 * TODO: error?
	 */
	rv = makeRangeVar(nspname, relname, -1);
	relid = RangeVarGetRelid(rv, AccessExclusiveLock, true);
	if (relid == InvalidOid)
		return;

	/* Truncate the table. */
	truncate = makeNode(TruncateStmt);
	truncate->relations = list_make1(rv);
	truncate->restart_seqs = false;
	truncate->behavior = DROP_RESTRICT;

	ExecuteTruncate(truncate);

	CommandCounterIncrement();
}

/*
 * Handle SQL message comming via queue table.
 */
static void
handle_sql(QueuedMessage *queued_message, bool tx_just_started)
{
	JsonbIterator *it;
	JsonbValue	v;
	int			r;
	char	   *sql;

	/* Validate the json and extract the SQL string from it. */
	if (!JB_ROOT_IS_SCALAR(queued_message->message))
		elog(ERROR, "malformed message in queued message tuple: root is not scalar");

	it = JsonbIteratorInit(&queued_message->message->root);
	r = JsonbIteratorNext(&it, &v, false);
	if (r != WJB_BEGIN_ARRAY)
		elog(ERROR, "malformed message in queued message tuple, item type %d expected %d", r, WJB_BEGIN_ARRAY);

	r = JsonbIteratorNext(&it, &v, false);
	if (r != WJB_ELEM)
		elog(ERROR, "malformed message in queued message tuple, item type %d expected %d", r, WJB_ELEM);

	if (v.type != jbvString)
		elog(ERROR, "malformed message in queued message tuple, expected value type %d got %d", jbvString, v.type);

	sql = pnstrdup(v.val.string.val, v.val.string.len);

	r = JsonbIteratorNext(&it, &v, false);
	if (r != WJB_END_ARRAY)
		elog(ERROR, "malformed message in queued message tuple, item type %d expected %d", r, WJB_END_ARRAY);

	r = JsonbIteratorNext(&it, &v, false);
	if (r != WJB_DONE)
		elog(ERROR, "malformed message in queued message tuple, item type %d expected %d", r, WJB_DONE);

	/* Run the extracted SQL. */
	pglogical_execute_sql_command(sql, queued_message->role, tx_just_started);
}

/*
 * Handles messages comming from the queue.
 */
static void
handle_queued_message(HeapTuple msgtup, bool tx_just_started)
{
	QueuedMessage  *queued_message = queued_message_from_tuple(msgtup);

	switch (queued_message->message_type)
	{
		case QUEUE_COMMAND_TYPE_SQL:
			handle_sql(queued_message, tx_just_started);
			break;
		case QUEUE_COMMAND_TYPE_TRUNCATE:
			handle_truncate(queued_message, tx_just_started);
			break;
		default:
			elog(ERROR, "unknown message type '%c'",
				 queued_message->message_type);
	}
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
	{
		MemoryContext	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		reply_message = makeStringInfo();
		MemoryContextSwitchTo(oldcontext);
	}
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

	/* Init the MessageContext which we use for easier cleanup. */
	MessageContext = AllocSetContextCreate(TopMemoryContext,
										   "MessageContext",
										   ALLOCSET_DEFAULT_MINSIZE,
										   ALLOCSET_DEFAULT_INITSIZE,
										   ALLOCSET_DEFAULT_MAXSIZE);

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

		MemoryContextSwitchTo(MessageContext);

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

		/* confirm all writes at once */
		send_feedback(streamConn, last_received, GetCurrentTimestamp(), false);

		/* Cleanup the memory. */
		MemoryContextResetAndDeleteChildren(MessageContext);
	}
}

/*
 * Add context to the errors produced by pglogical_execute_sql_command().
 */
static void
execute_sql_command_error_cb(void *arg)
{
	errcontext("during executions of queued SQL statement: %s", (char *) arg);
}

/*
 * Execute an SQL command. This can be multiple multiple queries.
 */
void
pglogical_execute_sql_command(char *cmdstr, char *role, bool isTopLevel)
{
	List	   *commands;
	ListCell   *command_i;
	MemoryContext oldcontext;
	ErrorContextCallback errcallback;

	oldcontext = MemoryContextSwitchTo(MessageContext);

	errcallback.callback = execute_sql_command_error_cb;
	errcallback.arg = cmdstr;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	commands = pg_parse_query(cmdstr);

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Do a limited amount of safety checking against CONCURRENTLY commands
	 * executed in situations where they aren't allowed. The sender side should
	 * provide protection, but better be safe than sorry.
	 */
	isTopLevel = isTopLevel & (list_length(commands) == 1);

	foreach(command_i, commands)
	{
		List	   *plantree_list;
		List	   *querytree_list;
		Node	   *command = (Node *) lfirst(command_i);
		const char *commandTag;
		Portal		portal;
		DestReceiver *receiver;

		/* temporarily push snapshot for parse analysis/planning */
		PushActiveSnapshot(GetTransactionSnapshot());

		oldcontext = MemoryContextSwitchTo(MessageContext);

		/*
		 * Set the current role to the user that executed the command on the
		 * origin server.  NB: there is no need to reset this afterwards, as
		 * the value will be gone with our transaction.
		 */
		SetConfigOption("role", role, PGC_INTERNAL, PGC_S_OVERRIDE);

		commandTag = CreateCommandTag(command);

		querytree_list = pg_analyze_and_rewrite(
			command, cmdstr, NULL, 0);

		plantree_list = pg_plan_queries(
			querytree_list, 0, NULL);

		PopActiveSnapshot();

		portal = CreatePortal("bdr", true, true);
		PortalDefineQuery(portal, NULL,
						  cmdstr, commandTag,
						  plantree_list, NULL);
		PortalStart(portal, NULL, 0, InvalidSnapshot);

		receiver = CreateDestReceiver(DestNone);

		(void) PortalRun(portal, FETCH_ALL,
						 isTopLevel,
						 receiver, receiver,
						 NULL);
		(*receiver->rDestroy) (receiver);

		PortalDrop(portal, false);

		CommandCounterIncrement();

		MemoryContextSwitchTo(oldcontext);
	}

	/* protect against stack resets during CONCURRENTLY processing */
	if (error_context_stack == &errcallback)
		error_context_stack = errcallback.previous;
}

/*
 * Given a List of PGLogicalRepSet, return the names of the
 * sets as a comma-separated list, quoting identifiers
 * if needed.
 *
 * This is essentially the reverse of SplitIdentifierString
 * but with a PGLogicalRepSet input.
 *
 * The caller should free the result.
 */
static char*
repsets_to_identifierstr(List *repsets)
{
	ListCell *lc;
	StringInfoData repsetnames;
	bool first = true;

	initStringInfo(&repsetnames);

	foreach (lc, repsets)
	{
		PGLogicalRepSet *rs;
		if (!first)
			appendStringInfoChar(&repsetnames, ',');
		rs = (PGLogicalRepSet*)lfirst(lc);
		appendStringInfoString(&repsetnames, quote_identifier(rs->name));
	}

	return repsetnames.data;
}

void
pglogical_apply_main(Datum main_arg)
{
	int				slot = DatumGetInt32(main_arg);
	PGconn		   *streamConn;
	PGresult	   *res;
	char		   *sqlstate;
	StringInfoData	conninfo_repl;
	StringInfoData	command;
	RepOriginId		originid;
	XLogRecPtr		origin_startpos;
	NameData		slot_name;
	PGLogicalSubscriber	   *sub;
	char		   *repsets;

	/* Setup shmem. */
	pglogical_worker_attach(slot);

	/* Establish signal handlers. */
	pqsignal(SIGTERM, handle_sigterm);
	BackgroundWorkerUnblockSignals();

	/* Attach to dsm segment. */
	Assert(CurrentResourceOwner == NULL);
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "pglogical apply");

	/* Connect to our database. */
	BackgroundWorkerInitializeConnectionByOid(MyPGLogicalWorker->dboid, InvalidOid);

	StartTransactionCommand();
	sub = get_subscriber(MyPGLogicalWorker->worker.apply.subscriberid);

	elog(DEBUG1, "starting apply for subscriber %s", sub->name);
	elog(DEBUG1, "conneting to provider %s, dsn %s",
		 sub->provider_name, sub->provider_dsn);

	initStringInfo(&conninfo_repl);
	appendStringInfo(&conninfo_repl, "%s replication=database fallback_application_name='%s_apply'",
					 sub->provider_dsn, sub->name);

	/*
	 * Cache tne queue relation id.
	 * TODO: invalidation
	 */
	QueueRelid = get_queue_table_oid();

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
				  sub->provider_name, sub->name);

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
	appendStringInfo(&command, ", startup_params_format '1'");
	appendStringInfo(&command, ", pg_version '%u'", PG_VERSION_NUM);

	/* Binary protocol compatibility. */
	appendStringInfo(&command, ", \"binary.want_internal_basetypes\" '1'");
	appendStringInfo(&command, ", \"binary.want_binary_basetypes\" '1'");
	appendStringInfo(&command, ", \"binary.basetypes_major_version\" '%u'", PG_VERSION_NUM/100);
	appendStringInfo(&command, ", \"binary.sizeof_datum\" '%zu'", sizeof(Datum));
	appendStringInfo(&command, ", \"binary.sizeof_int\" '%zu'", sizeof(int));
	appendStringInfo(&command, ", \"binary.sizeof_long\" '%zu'", sizeof(long));
	appendStringInfo(&command, ", \"binary.bigendian\" '%d'",
#ifdef WORDS_BIGENDIAN
					 true
#else
					 false
#endif
					 );
	appendStringInfo(&command, ", \"binary.float4_byval\" '%d'",
#ifdef USE_FLOAT4_BYVAL
					 true
#else
					 false
#endif
					 );
	appendStringInfo(&command, ", \"binary.float8_byval\" '%d'",
#ifdef USE_FLOAT8_BYVAL
					 true
#else
					 false
#endif
					 );
	appendStringInfo(&command, ", \"binary.integer_datetimes\" '%d'",
#ifdef USE_INTEGER_DATETIMES
					 true
#else
					 false
#endif
					 );

	appendStringInfoString(&command, ", \"hooks.setup_function\" 'pglogical.pglogical_hooks_setup'");

	/* TODO: Allow forwarding mode control. Currently hardcoded. */
	appendStringInfo(&command, ", \"pglogical.forward_origin\" '%s'", "all");

	/* Send the replication set names we want to the upstream */
	appendStringInfoString(&command, ", \"pglogical.replication_set_names\" ");
	repsets = repsets_to_identifierstr(sub->replication_sets);
	appendStringInfoString(&command, quote_literal_cstr(repsets));
	pfree(repsets);
	repsets=NULL;

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
