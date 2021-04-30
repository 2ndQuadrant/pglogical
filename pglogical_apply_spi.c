/*-------------------------------------------------------------------------
 *
 * pglogical_apply_spi.c
 * 		pglogical apply functions using SPI
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_apply_spi.c
 *
 * NOTES
 *	  The multi-insert support is not done through SPI but using binary
 *	  COPY through a pipe (virtual or file).
 *
 *-------------------------------------------------------------------------
 */
#include <stdio.h>
#include <unistd.h>

#include "postgres.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"

#include "commands/copy.h"

#include "executor/executor.h"
#include "executor/spi.h"

#include "replication/reorderbuffer.h"

#include "storage/fd.h"

#include "tcop/pquery.h"
#include "tcop/utility.h"

#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/builtins.h"

#include "pglogical.h"
#include "pglogical_apply_spi.h"
#include "pglogical_conflict.h"

/* State related to bulk insert */
typedef struct pglogical_copyState
{
	PGLogicalRelation  *rel;

	StringInfo		copy_stmt;
	List		   *copy_parsetree;
	File			copy_file;
	char			copy_mechanism;
	FILE		   *copy_read_file;
	FILE		   *copy_write_file;
	StringInfo		msgbuf;
	MemoryContext	rowcontext;
	FmgrInfo	   *out_functions;
	List		   *attnumlist;
	int				copy_buffered_tuples;
	size_t			copy_buffered_size;
} pglogical_copyState;

static pglogical_copyState *pglcstate = NULL;

static const char BinarySignature[11] = "PGCOPY\n\377\r\n\0";

static void pglogical_start_copy(PGLogicalRelation *rel);
static void pglogical_proccess_copy(pglogical_copyState *pglcstate);

static void pglogical_copySendData(pglogical_copyState *pglcstate,
								   const void *databuf, int datasize);
static void pglogical_copySendEndOfRow(pglogical_copyState *pglcstate);
static void pglogical_copySendInt32(pglogical_copyState *pglcstate, int32 val);
static void pglogical_copySendInt16(pglogical_copyState *pglcstate, int16 val);
static void pglogical_copyOneRowTo(pglogical_copyState *pglcstate,
								   Datum *values, bool *nulls);

/*
 * Handle begin (connect SPI).
 */
void
pglogical_apply_spi_begin(void)
{
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");
	MemoryContextSwitchTo(MessageContext);
}

/*
 * Handle commit (finish SPI).
 */
void
pglogical_apply_spi_commit(void)
{
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");
	MemoryContextSwitchTo(MessageContext);
}

/*
 * Handle insert via SPI.
 */
void
pglogical_apply_spi_insert(PGLogicalRelation *rel, PGLogicalTupleData *newtup)
{
	TupleDesc		desc = RelationGetDescr(rel->rel);
	Oid				argtypes[MaxTupleAttributeNumber];
	Datum			values[MaxTupleAttributeNumber];
	char			nulls[MaxTupleAttributeNumber];
	StringInfoData	cmd;
	int	att,
		narg;

	initStringInfo(&cmd);
	appendStringInfo(&cmd, "INSERT INTO %s (",
					 quote_qualified_identifier(rel->nspname, rel->relname));

	for (att = 0, narg = 0; att < desc->natts; att++)
	{
		if (TupleDescAttr(desc,att)->attisdropped)
			continue;

		if (!newtup->changed[att])
			continue;

		if (narg > 0)
			appendStringInfo(&cmd, ", %s",
					quote_identifier(NameStr(TupleDescAttr(desc,att)->attname)));
		else
			appendStringInfo(&cmd, "%s",
					quote_identifier(NameStr(TupleDescAttr(desc,att)->attname)));
		narg++;
	}

	appendStringInfoString(&cmd, ") VALUES (");

	for (att = 0, narg = 0; att < desc->natts; att++)
	{
		if (TupleDescAttr(desc,att)->attisdropped)
			continue;

		if (!newtup->changed[att])
			continue;

		if (narg > 0)
			appendStringInfo(&cmd, ", $%u", narg + 1);
		else
			appendStringInfo(&cmd, "$%u", narg + 1);

		argtypes[narg] = TupleDescAttr(desc,att)->atttypid;
		values[narg] = newtup->values[att];
		nulls[narg] = newtup->nulls[att] ? 'n' : ' ';
		narg++;
	}

	appendStringInfoString(&cmd, ")");

	if (SPI_execute_with_args(cmd.data, narg, argtypes, values, nulls, false,
							  0) != SPI_OK_INSERT)
		elog(ERROR, "SPI_execute_with_args failed");
	MemoryContextSwitchTo(MessageContext);

	pfree(cmd.data);
}

/*
 * Handle update via SPI.
 */
void
pglogical_apply_spi_update(PGLogicalRelation *rel, PGLogicalTupleData *oldtup,
						   PGLogicalTupleData *newtup)
{
	TupleDesc		desc = RelationGetDescr(rel->rel);
	Oid				argtypes[MaxTupleAttributeNumber];
	Datum			values[MaxTupleAttributeNumber];
	char			nulls[MaxTupleAttributeNumber];
	StringInfoData	cmd;
	Bitmapset	   *id_attrs;
	int	att,
		narg,
		firstarg;

	id_attrs = RelationGetIndexAttrBitmap(rel->rel,
										  INDEX_ATTR_BITMAP_IDENTITY_KEY);

	initStringInfo(&cmd);
	appendStringInfo(&cmd, "UPDATE %s SET ",
					 quote_qualified_identifier(rel->nspname, rel->relname));

	for (att = 0, narg = 0; att < desc->natts; att++)
	{
		if (TupleDescAttr(desc,att)->attisdropped)
			continue;

		if (!newtup->changed[att])
			continue;

		if (narg > 0)
			appendStringInfo(&cmd, ", %s = $%u",
							 quote_identifier(NameStr(TupleDescAttr(desc,att)->attname)),
							 narg + 1);
		else
			appendStringInfo(&cmd, "%s = $%u",
							 quote_identifier(NameStr(TupleDescAttr(desc,att)->attname)),
							 narg + 1);

		argtypes[narg] = TupleDescAttr(desc,att)->atttypid;
		values[narg] = newtup->values[att];
		nulls[narg] = newtup->nulls[att] ? 'n' : ' ';
		narg++;
	}

	appendStringInfoString(&cmd, " WHERE");

	firstarg = narg;
	for (att = 0; att < desc->natts; att++)
	{
		if (!bms_is_member(TupleDescAttr(desc,att)->attnum - FirstLowInvalidHeapAttributeNumber,
						   id_attrs))
			continue;

		if (narg > firstarg)
			appendStringInfo(&cmd, " AND %s = $%u",
							 quote_identifier(NameStr(TupleDescAttr(desc,att)->attname)),
							 narg + 1);
		else
			appendStringInfo(&cmd, " %s = $%u",
							 quote_identifier(NameStr(TupleDescAttr(desc,att)->attname)),
							 narg + 1);

		argtypes[narg] = TupleDescAttr(desc,att)->atttypid;
		values[narg] = oldtup->values[att];
		nulls[narg] = oldtup->nulls[att] ? 'n' : ' ';
		narg++;
	}

	if (SPI_execute_with_args(cmd.data, narg, argtypes, values, nulls, false,
							  0) != SPI_OK_UPDATE)
		elog(ERROR, "SPI_execute_with_args failed");
	MemoryContextSwitchTo(MessageContext);

	pfree(cmd.data);
}

/*
 * Handle delete via SPI.
 */
void
pglogical_apply_spi_delete(PGLogicalRelation *rel, PGLogicalTupleData *oldtup)
{
	TupleDesc		desc = RelationGetDescr(rel->rel);
	Oid				argtypes[MaxTupleAttributeNumber];
	Datum			values[MaxTupleAttributeNumber];
	char			nulls[MaxTupleAttributeNumber];
	StringInfoData	cmd;
	Bitmapset	   *id_attrs;
	int	att,
		narg;

	id_attrs = RelationGetIndexAttrBitmap(rel->rel,
										  INDEX_ATTR_BITMAP_IDENTITY_KEY);

	initStringInfo(&cmd);
	appendStringInfo(&cmd, "DELETE FROM %s WHERE",
					 quote_qualified_identifier(rel->nspname, rel->relname));

	for (att = 0, narg = 0; att < desc->natts; att++)
	{
		if (!bms_is_member(TupleDescAttr(desc,att)->attnum - FirstLowInvalidHeapAttributeNumber,
						   id_attrs))
			continue;

		if (narg > 0)
			appendStringInfo(&cmd, " AND %s = $%u",
							 quote_identifier(NameStr(TupleDescAttr(desc,att)->attname)),
							 narg + 1);
		else
			appendStringInfo(&cmd, " %s = $%u",
							 quote_identifier(NameStr(TupleDescAttr(desc,att)->attname)),
							 narg + 1);

		argtypes[narg] = TupleDescAttr(desc,att)->atttypid;
		values[narg] = oldtup->values[att];
		nulls[narg] = oldtup->nulls[att] ? 'n' : ' ';
		narg++;
	}

	if (SPI_execute_with_args(cmd.data, narg, argtypes, values, nulls, false,
							  0) != SPI_OK_DELETE)
		elog(ERROR, "SPI_execute_with_args failed");
	MemoryContextSwitchTo(MessageContext);

	pfree(cmd.data);
}


/* We currently can't support multi insert using COPY on windows. */
#if !defined(WIN32) && !defined(PGL_NO_STDIN_ASSIGN)

bool
pglogical_apply_spi_can_mi(PGLogicalRelation *rel)
{
	/* Multi insert is only supported when conflicts result in errors. */
	return pglogical_conflict_resolver == PGLOGICAL_RESOLVE_ERROR;
}

void
pglogical_apply_spi_mi_add_tuple(PGLogicalRelation *rel,
								 PGLogicalTupleData *tup)
{
	Datum	*values;
	bool	*nulls;

	/* Start COPY if not already done so */
	pglogical_start_copy(rel);

#define MAX_BUFFERED_TUPLES		10000
#define MAX_BUFFER_SIZE			60000
	/*
	 * If sufficient work is pending, process that first
	 */
	if (pglcstate->copy_buffered_tuples > MAX_BUFFERED_TUPLES ||
		pglcstate->copy_buffered_size > MAX_BUFFER_SIZE)
	{
		pglogical_apply_spi_mi_finish(rel);
		pglogical_start_copy(rel);
	}

	/*
	 * Write the tuple to the COPY stream.
	 */
	values = (Datum *) tup->values;
	nulls = (bool *) tup->nulls;
	pglogical_copyOneRowTo(pglcstate, values, nulls);
}


/*
 * Initialize copy state for reation.
 */
static void
pglogical_start_copy(PGLogicalRelation *rel)
{
	MemoryContext oldcontext;
	TupleDesc		desc;
	ListCell	   *cur;
	int				num_phys_attrs;
	char		   *delim;
	StringInfoData  attrnames;
	int				i;

	/* We are already doing COPY for requested relation, nothing to do. */
	if (pglcstate && pglcstate->rel == rel)
		return;

	/* We are in COPY but for different relation, finish it first. */
	if (pglcstate && pglcstate->rel != rel)
		pglogical_apply_spi_mi_finish(pglcstate->rel);

	oldcontext = MemoryContextSwitchTo(TopTransactionContext);

	/* Initialize new COPY state. */
	pglcstate = palloc0(sizeof(pglogical_copyState));

	pglcstate->copy_file = -1;
	pglcstate->msgbuf = makeStringInfo();
	pglcstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
												  "COPY TO",
												  ALLOCSET_DEFAULT_SIZES);

	pglcstate->rel = rel;

	for (i = 0; i < rel->natts; i++)
		pglcstate->attnumlist = lappend_int(pglcstate->attnumlist,
											rel->attmap[i]);

	desc = RelationGetDescr(rel->rel);
	num_phys_attrs = desc->natts;

	/* Get info about the columns we need to process. */
	pglcstate->out_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));

	/* Get attribute list in a CSV form */
	initStringInfo(&attrnames);
	delim = "";

	/*
	 * Now that we have a list of attributes from the remote side and their
	 * mapping to our side, build a COPY statement that can be parsed and
	 * executed later to bulk load the incoming tuples.
	 */
	foreach(cur, pglcstate->attnumlist)
	{
		int         attnum = lfirst_int(cur);
		Oid         out_func_oid;
		bool        isvarlena;

		getTypeBinaryOutputInfo(TupleDescAttr(desc,attnum)->atttypid,
								&out_func_oid,
								&isvarlena);
		fmgr_info(out_func_oid, &pglcstate->out_functions[attnum]);
		appendStringInfo(&attrnames, "%s %s",
						 delim,
						 quote_identifier(NameStr(TupleDescAttr(desc,attnum)->attname)));
		delim = ", ";

	}

	pglcstate->copy_stmt = makeStringInfo();
	appendStringInfo(pglcstate->copy_stmt, "COPY %s.%s (%s) FROM STDIN "
					 "WITH (FORMAT BINARY)",
					 quote_identifier(rel->nspname),
					 quote_identifier(rel->relname),
					 attrnames.data);
	pfree(attrnames.data);


	/*
	 * This is a bit of kludge to let COPY FROM read from the STDIN. In
	 * pglogical, the apply worker is accumulating tuples received from the
	 * publisher and queueing them for a bulk load. But the COPY API can only
	 * deal with either a file or a PROGRAM or STDIN.
	 *
	 * We could either use pipe-based implementation where the apply worker
	 * first writes to one end of the pipe and later reads from the other end.
	 * But pipe's internal buffer is limited in size and hence we cannot
	 * accumulate much data without writing it out to the table.
	 *
	 * The temporary file based implementation is more flexible. The only
	 * disadvantage being that the data may get written to the disk and that
	 * may cause performance issues.
	 *
	 * A more ideal solution would be to teach COPY to write to and read from a
	 * buffer. But that will require changes to the in-core COPY
	 * infrastructure. Instead, we setup things such that a pipe is created
	 * between STDIN and a unnamed stream. The tuples are written to the one
	 * end of the pipe and read back from the other end. Since we can fiddle
	 * with the existing STDIN, we assign the read end of the pipe to STDIN.
	 *
	 * This seems ok since the apply worker being a background worker is not
	 * going to read anything from the STDIN normally. So our highjacking of
	 * the stream seems ok.
	 */
	if (pglcstate->copy_file == -1)
		pglcstate->copy_file = OpenTemporaryFile(true);

	Assert(pglcstate->copy_file > 0);

	pglcstate->copy_write_file = fopen(FilePathName(pglcstate->copy_file), "w");
	pglcstate->copy_read_file = fopen(FilePathName(pglcstate->copy_file), "r");
	pglcstate->copy_mechanism = 'f';

	pglcstate->copy_parsetree = pg_parse_query(pglcstate->copy_stmt->data);
	MemoryContextSwitchTo(oldcontext);

	pglogical_copySendData(pglcstate, BinarySignature,
						   sizeof(BinarySignature));
	pglogical_copySendInt32(pglcstate, 0);
	pglogical_copySendInt32(pglcstate, 0);
}

static void
pglogical_proccess_copy(pglogical_copyState *pglcstate)
{
	uint64	processed;
	FILE	*save_stdin;

	if (!pglcstate->copy_parsetree || !pglcstate->copy_buffered_tuples)
		return;

	/*
	 * First send a file trailer so that when DoCopy is run below, it sees an
	 * end of the file marker and terminates COPY once all queued tuples are
	 * processed. We also close the file descriptor because DoCopy expects to
	 * see a real EOF too
	 */
	pglogical_copySendInt16(pglcstate, -1);

	/* Also ensure that the data is flushed to the stream */
	pglogical_copySendEndOfRow(pglcstate);

	/*
	 * Now close the write end of the pipe so that DoCopy sees end of the
	 * stream.
	 *
	 * XXX This is really sad because ideally we would have liked to keep the
	 * pipe open and use that for next batch of bulk copy. But given the way
	 * COPY protocol currently works, we don't have any other option but to
	 * close the stream.
	 */
	fflush(pglcstate->copy_write_file);
	fclose(pglcstate->copy_write_file);
	pglcstate->copy_write_file = NULL;

	/*
	 * The COPY statement previously crafted will read from STDIN. So we
	 * override the 'stdin' stream to point to the read end of the pipe created
	 * for this relation. Before that we save the current 'stdin' stream and
	 * restore it back when the COPY is done
	 */
	save_stdin = stdin;
	stdin = pglcstate->copy_read_file;

	/* COPY may call into SPI (triggers, ...) and we already are in SPI. */
	SPI_push();

	/* Initiate the actual COPY */
#if PG_VERSION_NUM >= 100000
	PGLDoCopy((CopyStmt*)((RawStmt *)linitial(pglcstate->copy_parsetree))->stmt,
		pglcstate->copy_stmt->data, &processed);
#else
	PGLDoCopy((CopyStmt *) linitial(pglcstate->copy_parsetree),
			  pglcstate->copy_stmt->data, &processed);
#endif

	/* Clean up SPI state */
	SPI_pop();

	fclose(pglcstate->copy_read_file);
	pglcstate->copy_read_file = NULL;
	stdin = save_stdin;

	/* Ensure we processed correct number of tuples */
	Assert(processed == pglcstate->copy_buffered_tuples);

	list_free_deep(pglcstate->copy_parsetree);
	pglcstate->copy_parsetree = NIL;

	pglcstate->copy_buffered_tuples = 0;
	pglcstate->copy_buffered_size = 0;

	CommandCounterIncrement();
}

void
pglogical_apply_spi_mi_finish(PGLogicalRelation *rel)
{
	if (!pglcstate)
		return;

	Assert(pglcstate->rel == rel);

	pglogical_proccess_copy(pglcstate);

	if (pglcstate->copy_stmt)
	{
		pfree(pglcstate->copy_stmt->data);
		pfree(pglcstate->copy_stmt);
	}

	if (pglcstate->attnumlist)
		list_free(pglcstate->attnumlist);

	if (pglcstate->copy_file != -1)
		FileClose(pglcstate->copy_file);

	if (pglcstate->copy_write_file)
		fclose(pglcstate->copy_write_file);

	if (pglcstate->copy_read_file)
		fclose(pglcstate->copy_read_file);

	if (pglcstate->msgbuf)
	{
		pfree(pglcstate->msgbuf->data);
		pfree(pglcstate->msgbuf);
	}

	if (pglcstate->rowcontext)
	{
		MemoryContextDelete(pglcstate->rowcontext);
		pglcstate->rowcontext = NULL;
	}

	pfree(pglcstate);

	pglcstate = NULL;
}

/*
 * pglogical_copySendInt32 sends an int32 in network byte order
 */
static void
pglogical_copySendInt32(pglogical_copyState *pglcstate, int32 val)
{
	uint32		buf;

	buf = htonl((uint32) val);
	pglogical_copySendData(pglcstate, &buf, sizeof(buf));
}

/*
 * pglogical_copySendInt16 sends an int16 in network byte order
 */
static void
pglogical_copySendInt16(pglogical_copyState *pglcstate, int16 val)
{
	uint16		buf;

	buf = htons((uint16) val);
	pglogical_copySendData(pglcstate, &buf, sizeof(buf));
}

/*----------
 * pglogical_copySendData sends output data to the destination (file or frontend)
 * pglogical_copySendEndOfRow does the appropriate thing at end of each data row
 *	(data is not actually flushed except by pglogical_copySendEndOfRow)
 *
 * NB: no data conversion is applied by these functions
 *----------
 */
static void
pglogical_copySendData(pglogical_copyState *pglcstate, const void *databuf,
					   int datasize)
{
	appendBinaryStringInfo(pglcstate->msgbuf, databuf, datasize);
}

static void
pglogical_copySendEndOfRow(pglogical_copyState *pglcstate)
{
	StringInfo	msgbuf = pglcstate->msgbuf;

	if (fwrite(msgbuf->data, msgbuf->len, 1,
				pglcstate->copy_write_file) != 1 ||
			ferror(pglcstate->copy_write_file))
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to COPY file: %m")));
	}

	resetStringInfo(msgbuf);
}

/*
 * Emit one row during CopyTo().
 */
static void
pglogical_copyOneRowTo(pglogical_copyState *pglcstate, Datum *values,
					   bool *nulls)
{
	FmgrInfo   *out_functions = pglcstate->out_functions;
	MemoryContext oldcontext;
	ListCell   *cur;

	MemoryContextReset(pglcstate->rowcontext);
	oldcontext = MemoryContextSwitchTo(pglcstate->rowcontext);

	/* Binary per-tuple header */
	pglogical_copySendInt16(pglcstate, list_length(pglcstate->attnumlist));

	foreach(cur, pglcstate->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		Datum		value = values[attnum];
		bool		isnull = nulls[attnum];

		if (isnull)
			pglogical_copySendInt32(pglcstate, -1);
		else
		{
			bytea	   *outputbytes;

			outputbytes = SendFunctionCall(&out_functions[attnum],
										   value);
			pglogical_copySendInt32(pglcstate, VARSIZE(outputbytes) - VARHDRSZ);
			pglogical_copySendData(pglcstate, VARDATA(outputbytes),
						 VARSIZE(outputbytes) - VARHDRSZ);
		}
	}

	pglcstate->copy_buffered_tuples++;
	pglcstate->copy_buffered_size += pglcstate->msgbuf->len;

	pglogical_copySendEndOfRow(pglcstate);

	MemoryContextSwitchTo(oldcontext);
}

#else /* WIN32 */

bool
pglogical_apply_spi_can_mi(PGLogicalRelation *rel)
{
	return false;
}

void
pglogical_apply_spi_mi_add_tuple(PGLogicalRelation *rel,
								 PGLogicalTupleData *tup)
{
	elog(ERROR, "pglogical_apply_spi_mi_add_tuple called unexpectedly");
}

void
pglogical_apply_spi_mi_finish(PGLogicalRelation *rel)
{
	elog(ERROR, "pglogical_apply_spi_mi_finish called unexpectedly");
}

#endif /* WIN32 */
