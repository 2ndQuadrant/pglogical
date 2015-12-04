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

#include "catalog/pg_database.h"
#include "catalog/pg_type.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "miscadmin.h"

#include "postmaster/bgworker_internals.h"

#include "pglogical_compat.h"
#include "replication/origin.h"
#include "access/commit_ts.h"


XLogRecPtr XactLastCommitEnd = 0;

RepOriginId replorigin_session_origin = 0;
XLogRecPtr replorigin_session_origin_lsn = 0;
TimestampTz replorigin_session_origin_timestamp = 0;

bool track_commit_timestamp = false;

RepOriginId replorigin_create(char *name)
{
	elog(ERROR, "replorigin_create is not implemented yet");
}

void
replorigin_drop(RepOriginId roident)
{
	elog(ERROR, "replorigin_drop is not implemented yet");
}

RepOriginId replorigin_by_name(char *name, bool missing_ok)
{
	elog(ERROR, "replorigin_by_name is not implemented yet");

	return 0;
}

void replorigin_session_setup(RepOriginId node)
{
	elog(ERROR, "replorigin_session_setup is not implemented yet");
}

void
replorigin_session_reset(void)
{
	elog(ERROR, "replorigin_session_reset is not implemented yet");
}

XLogRecPtr replorigin_session_get_progress(bool flush)
{
	elog(ERROR, "replorigin_session_get_progress is not implemented yet");
	return 0;
}

void replorigin_advance(RepOriginId node,
				   XLogRecPtr remote_commit,
				   XLogRecPtr local_commit,
				   bool go_backward, bool wal_log)
{
	elog(ERROR, "replorigin_advance is not implemented yet");
	return;
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

bool TransactionIdGetCommitTsData(TransactionId xid,
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
