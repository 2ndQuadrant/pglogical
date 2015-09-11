/*-------------------------------------------------------------------------
 *
 * pg_logical_apply.c
 * 		pg_logical apply logic
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pg_logical.c
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

#include "executor/executor.h"

#include "lib/stringinfo.h"

#include "libpq/pqformat.h"

#include "mb/pg_wchar.h"

#include "postmaster/bgworker.h"

#include "replication/origin.h"

#include "storage/ipc.h"
#include "storage/proc.h"

#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "pg_logical_proto.h"
#include "pg_logical_relcache.h"
#include "pg_logical_conflict.h"
#include "pg_logical_node.h"


volatile sig_atomic_t got_SIGTERM = false;
static bool			in_remote_transaction = false;
static XLogRecPtr	remote_origin_lsn = InvalidXLogRecPtr;
static RepOriginId	remote_origin_id = InvalidRepOriginId;

typedef enum PGLogicalConflictType
{
	CONFLICT_INSERT,
	CONFLICT_UPDATE,
	CONFLICT_DELETE
} PGLogicalConflictType;

typedef struct PGLogicalApply
{
	const char *node_name;
	const char *slot_name;
	const char *origin_name;
	const char *origin_dsn;
	const char **replication_sets;
} PGLogicalApply;

/*
 * Ensure string is not longer than maxlen.
 *
 * The way we do this is we if the string is longer we return prefix from that
 * string and hash of the string which will together be exatly maxlen.
 *
 * Maxlen can't be less than 11 because hash produces uint32 which in text form
 * can have up to 10 characters.
 */
static char *
shorten_hash(const char *str, int maxlen)
{
	char   *ret;
	int		len = strlen(str);

	Assert(maxlen > 10);

	if (len <= maxlen)
		return pstrdup(str);

	ret = (char *) palloc(maxlen + 1);
	snprintf(ret, maxlen, "%*s%u", maxlen - 10, /* uint32 max length is 10 */
			 str, DatumGetUInt32(hash_any((unsigned char *) str, len)));
	ret[maxlen] = '\0';
}

/*
 * Generate slot name (used also for origin identifier)
 *
 * TODO: make the slot name more unique for different PG instances to avoid
 * unintentional connections between different clusters.
 */
static void
gen_slot_name(Name slot_name, Oid dboid, PGLogicalNode *origin_node,
		  PGLogicalNode *target_node)
{
	snprintf(NameStr(*slot_name), NAMEDATALEN,
			 "pgl_%u_%s_%s", dboid,
			 shorten_hash(origin_node->name, 20),
			 shorten_hash(target_node->name, 20));
	NameStr(*slot_name)[NAMEDATALEN-1] = '\0';
}

static void
pg_logical_ensure_transaction(void)
{
	if (IsTransactionState())
		return;

	StartTransactionCommand();
}


static void
handle_begin(StringInfo s)
{
	XLogRecPtr		remote_lsn;
	TimestampTz		committime;
	TransactionId	remote_xid;

	pg_logical_read_begin(s, &remote_lsn, &committime, &remote_xid);

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
   	TimestampTz		committime;

	pg_logical_read_commit(s, &commit_lsn, &end_lsn, &committime);

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
		remote_origin_id != replorigin_sesssion_origin)
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

	origin = pg_logical_read_origin(s, &remote_origin_lsn);
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
	(void) pg_logical_read_rel(s);
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
handle_insert(StringInfo s)
{
	PGLogicalTupleData	newtup;
	PGLogicalRelation  *rel;
	EState			   *estate;
	Oid					conflicts;
	TupleTableSlot	   *localslot;
	HeapTuple			remotetuple;
	HeapTuple			applytuple;
	PGLogicalConflictResolution resolution;

	pg_logical_ensure_transaction();

	rel = pg_logical_read_insert(s, RowExclusiveLock, &newtup);

	estate = create_estate_for_relation(rel->rel);
	localslot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(localslot, RelationGetDescr(rel->rel));
	ExecOpenIndices(estate->es_result_relation_info, false);

	conflicts = pg_logical_tuple_conflict(estate, &newtup, CONFLICT_INSERT,
										  localslot);

	remotetuple = heap_form_tuple(RelationGetDescr(rel->rel),
								  newtup.values, newtup.nulls);

	if (OidIsValid(conflicts))
	{
		bool apply = try_resolve_conflict(rel->rel, localslot->tts_tuple,
										  remotetuple, CONFLICT_INSERT,
										  &applytuple, &resolution);

//		report_conflict(CONFLICT_INSERT, rel->rel, localslot->tts_tuple,
//						remotetuple, applytuple, resolution);

		if (apply)
			simple_heap_update(rel->rel, &localslot->tts_tuple->t_self,
							   applytuple);
	}
	else
	{
		simple_heap_insert(rel->rel, remotetuple);
	}

	ExecCloseIndices(estate->es_result_relation_info);

	pg_logical_relation_close(rel, RowExclusiveLock);
	ExecResetTupleTable(estate->es_tupleTable, true);
	FreeExecutorState(estate);

	CommandCounterIncrement();
}

static void
handle_update(StringInfo s)
{
	PGLogicalTupleData	newtup;
	PGLogicalRelation  *rel;
	EState			   *estate;
	Relation			idxrel;
	bool				found;
	TupleTableSlot	   *localslot;
	HeapTuple			remotetuple;

	pg_logical_ensure_transaction();

	rel = pg_logical_read_insert(s, RowExclusiveLock, &newtup);

	estate = create_estate_for_relation(rel->rel);
	localslot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(localslot, RelationGetDescr(rel->rel));

	idxrel = replindex_open(rel->rel, RowExclusiveLock);

	found = pg_logical_tuple_find(rel->rel, idxrel, &newtup, localslot);

	remotetuple = heap_form_tuple(RelationGetDescr(rel->rel),
								  newtup.values, newtup.nulls);

	/* TODO: handle conflicts */
	if (found)
	{
		simple_heap_update(rel->rel, &localslot->tts_tuple->t_self,
						   remotetuple);
	}
	else
	{
		/*
		 * The tuple to be updated could not be found.
		 *
		 */
	}
}

static void
handle_delete(StringInfo s)
{
	PGLogicalTupleData	oldtup;
	PGLogicalRelation  *rel;
	EState			   *estate;
	Relation			idxrel;
	TupleTableSlot	   *localslot;

	pg_logical_ensure_transaction();

	rel = pg_logical_read_delete(s, RowExclusiveLock, &oldtup);

	estate = create_estate_for_relation(rel->rel);
	localslot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(localslot, RelationGetDescr(rel->rel));

	idxrel = replindex_open(rel->rel, RowExclusiveLock);

	PushActiveSnapshot(GetTransactionSnapshot());

	if (pg_logical_tuple_find(rel->rel, idxrel, &oldtup, localslot))
	{
		simple_heap_delete(rel->rel, &localslot->tts_tuple->t_self);
	}
	else
	{
		/*
		 * The tuple to be deleted could not be found.
		 */
	}

	PopActiveSnapshot();

	index_close(idxrel, NoLock);
	pg_logical_relation_close(rel, NoLock);

	ExecResetTupleTable(estate->es_tupleTable, true);
	FreeExecutorState(estate);

	CommandCounterIncrement();
}


static void
replication_handler(StringInfo s)
{
	char action = pq_getmsgbyte(s);

	elog(WARNING, "ACTION %c", action);
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
		default:
			elog(ERROR, "unknown action of type %c", action);
	}
}

static void
apply_work(PGconn *streamConn)
{
	int			fd;
	char	   *copybuf = NULL;

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

					replication_handler(&s);
				}
				else if (c == 'k')
				{
					/* TODO */
				}
				/* other message types are purposefully ignored */
			}

		}
	}
}

void
pg_logical_apply_main(Datum main_arg)
{
	int			connid = DatumGetUInt32(main_arg);
	PGLogicalConnection *conn;
	PGLogicalNode	    *origin_node;
	PGconn	   *streamConn;
	PGresult   *res;
	char	   *sqlstate;
	StringInfoData conninfo_repl;
	StringInfoData command;
	RepOriginId originid;
	XLogRecPtr	origin_startpos;
	NameData	slot_name;

	/* TODO */
	BackgroundWorkerInitializeConnection("postgres", NULL);

	StartTransactionCommand();
	conn = get_node_connection_by_id(connid);
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
	gen_slot_name(&slot_name, MyDatabaseId, conn->origin, conn->target);

	originid = replorigin_by_name(NameStr(slot_name), false);
	replorigin_session_setup(originid);
	origin_startpos = replorigin_session_get_progress(false);

	/* Start the replication. */
	initStringInfo(&command);
	appendStringInfo(&command, "START_REPLICATION SLOT \"%s\" LOGICAL %X/%X (",
					 NameStr(slot_name),
					 (uint32) (origin_startpos >> 32),
					 (uint32) origin_startpos);

	appendStringInfo(&command, "client_encoding '%s'", GetDatabaseEncodingName());
	appendStringInfo(&command, ", replication_sets '%s'", conn->replication_sets);

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
