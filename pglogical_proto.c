/*-------------------------------------------------------------------------
 *
 * pglogical_proto.c
 * 		pglogical protocol functions
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_proto.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"

#include "access/htup_details.h"
#include "access/heapam.h"

#include "access/sysattr.h"
#include "access/tuptoaster.h"
#include "access/xact.h"

#include "catalog/catversion.h"
#include "catalog/index.h"

#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"

#include "commands/dbcommands.h"

#include "executor/spi.h"

#include "libpq/pqformat.h"

#include "mb/pg_wchar.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"

#include "pglogical_proto.h"
#include "pglogical_relcache.h"

#define IS_REPLICA_IDENTITY 1

static void pglogical_read_attrs(StringInfo in, char ***attrnames,
								  int *nattrnames);
static void pglogical_read_tuple(StringInfo in, PGLogicalRelation *rel,
					  PGLogicalTupleData *tuple);

/*
 * Read functions.
 */

/*
 * Read transaction BEGIN from the stream.
 */
void
pglogical_read_begin(StringInfo in, XLogRecPtr *remote_lsn,
					  TimestampTz *committime, TransactionId *remote_xid)
{
	/* read flags */
	uint8	flags = pq_getmsgbyte(in);
	Assert(flags == 0);

	/* read fields */
	*remote_lsn = pq_getmsgint64(in);
	Assert(*remote_lsn != InvalidXLogRecPtr);
	*committime = pq_getmsgint64(in);
	*remote_xid = pq_getmsgint(in, 4);
}

/*
 * Read transaction COMMIT from the stream.
 */
void
pglogical_read_commit(StringInfo in, XLogRecPtr *commit_lsn,
					   XLogRecPtr *end_lsn, TimestampTz *committime)
{
	/* read flags */
	uint8	flags = pq_getmsgbyte(in);
	Assert(flags == 0);

	/* read fields */
	*commit_lsn = pq_getmsgint64(in);
	*end_lsn = pq_getmsgint64(in);
	*committime = pq_getmsgint64(in);
}

/*
 * Read ORIGIN from the output stream.
 */
char *
pglogical_read_origin(StringInfo in, XLogRecPtr *origin_lsn)
{
	uint8	flags;
	uint8	len;

	/* read the flags */
	flags = pq_getmsgbyte(in);
	Assert(flags == 0);

	/* fixed fields */
	*origin_lsn = pq_getmsgint64(in);

	/* origin */
	len = pq_getmsgbyte(in);
	return pnstrdup(pq_getmsgbytes(in, len), len);
}


/*
 * Read INSERT from stream.
 *
 * Fills the new tuple.
 */
PGLogicalRelation *
pglogical_read_insert(StringInfo in, LOCKMODE lockmode,
					   PGLogicalTupleData *newtup)
{
	char		action;
	uint32		relid;
	uint8		flags;
	PGLogicalRelation *rel;

	/* read the flags */
	flags = pq_getmsgbyte(in);
	Assert(flags == 0);

	/* read the relation id */
	relid = pq_getmsgint(in, 4);

	action = pq_getmsgbyte(in);
	if (action != 'N')
		elog(ERROR, "expected new tuple but got %d",
			 action);

	rel = pglogical_relation_open(relid, lockmode);

	pglogical_read_tuple(in, rel, newtup);

	return rel;
}

/*
 * Read UPDATE from stream.
 */
PGLogicalRelation *
pglogical_read_update(StringInfo in, LOCKMODE lockmode, bool *hasoldtup,
					   PGLogicalTupleData *oldtup, PGLogicalTupleData *newtup)
{
	char		action;
	Oid			relid;
	uint8		flags;
	PGLogicalRelation *rel;

	/* read the flags */
	flags = pq_getmsgbyte(in);
	Assert(flags == 0);

	/* read the relation id */
	relid = pq_getmsgint(in, 4);

	/* read and verify action */
	action = pq_getmsgbyte(in);
	if (action != 'K' && action != 'O' && action != 'N')
		elog(ERROR, "expected action 'N', 'O' or 'K', got %c",
			 action);

	rel = pglogical_relation_open(relid, lockmode);

	/* check for old tuple */
	if (action == 'K' || action == 'O')
	{
		pglogical_read_tuple(in, rel, oldtup);
		*hasoldtup = true;
		action = pq_getmsgbyte(in);
	}
	else
		*hasoldtup = false;

	/* check for new  tuple */
	if (action != 'N')
		elog(ERROR, "expected action 'N', got %c",
			 action);

	pglogical_read_tuple(in, rel, newtup);

	return rel;
}

/*
 * Read DELETE from stream.
 *
 * Fills the old tuple.
 */
PGLogicalRelation *
pglogical_read_delete(StringInfo in, LOCKMODE lockmode,
					   PGLogicalTupleData *oldtup)
{
	char		action;
	Oid			relid;
	uint8		flags;
	PGLogicalRelation *rel;

	/* read the flags */
	flags = pq_getmsgbyte(in);
	Assert(flags == 0);

	/* read the relation id */
	relid = pq_getmsgint(in, 4);

	/* read and verify action */
	action = pq_getmsgbyte(in);
	if (action != 'K' && action != 'O')
		elog(ERROR, "expected action 'O' or 'K' %c", action);

	rel = pglogical_relation_open(relid, lockmode);

	pglogical_read_tuple(in, rel, oldtup);

	return rel;
}


/*
 * Read tuple in remote format from stream.
 *
 * The returned tuple is converted to the local relation tuple format.
 */
static void
pglogical_read_tuple(StringInfo in, PGLogicalRelation *rel,
					  PGLogicalTupleData *tuple)
{
	int			i;
	int			natts;
	char		action;
	TupleDesc	desc;

	action = pq_getmsgbyte(in);
	if (action != 'T')
		elog(ERROR, "expected TUPLE, got %c", action);

	memset(tuple->nulls, 1, sizeof(tuple->nulls));
	memset(tuple->changed, 0, sizeof(tuple->changed));

	natts = pq_getmsgint(in, 2);
	if (rel->natts != natts)
		elog(ERROR, "tuple natts mismatch, %u vs %u", rel->natts, natts);

	desc = RelationGetDescr(rel->rel);

	/* Read the data */
	for (i = 0; i < natts; i++)
	{
		int			attid = rel->attmap[i];
		Form_pg_attribute att = desc->attrs[attid];
		char		kind = pq_getmsgbyte(in);
		const char *data;
		int			len;

		switch (kind)
		{
			case 'n': /* null */
				/* already marked as null */
				tuple->values[attid] = 0xdeadbeef;
				tuple->changed[attid] = true;
				break;
			case 'u': /* unchanged column */
				tuple->values[attid] = 0xfbadbeef; /* make bad usage more obvious */
				break;
			case 'i': /* internal binary format */
				tuple->nulls[attid] = false;
				tuple->changed[attid] = true;

				len = pq_getmsgint(in, 4); /* read length */
				data = pq_getmsgbytes(in, len);

				/* and data */
				if (att->attbyval)
					tuple->values[attid] = fetch_att(data, true, len);
				else
					tuple->values[attid] = PointerGetDatum(data);
				break;
			case 'b': /* binary send/recv format */
				{
					Oid typreceive;
					Oid typioparam;
					StringInfoData buf;

					tuple->nulls[attid] = false;
					tuple->changed[attid] = true;

					len = pq_getmsgint(in, 4); /* read length */

					getTypeBinaryInputInfo(att->atttypid,
										   &typreceive, &typioparam);

					/* create StringInfo pointing into the bigger buffer */
					initStringInfo(&buf);
					/* and data */
					buf.data = (char *) pq_getmsgbytes(in, len);
					buf.len = len;
					tuple->values[attid] = OidReceiveFunctionCall(
						typreceive, &buf, typioparam, att->atttypmod);

					if (buf.len != buf.cursor)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
								 errmsg("incorrect binary data format")));
					break;
				}
			case 't': /* text format */
				{
					Oid typinput;
					Oid typioparam;

					tuple->nulls[attid] = false;
					tuple->changed[attid] = true;

					len = pq_getmsgint(in, 4); /* read length */

					getTypeInputInfo(att->atttypid, &typinput, &typioparam);
					/* and data */
					data = (char *) pq_getmsgbytes(in, len);
					tuple->values[attid] = OidInputFunctionCall(
						typinput, (char *) data, typioparam, att->atttypmod);
				}
				break;
			default:
				elog(ERROR, "unknown data representation type '%c'", kind);
		}
	}
}

/*
 * Read schema.relation from stream and return as PGLogicalRelation opened in
 * lockmode.
 */
uint32
pglogical_read_rel(StringInfo in)
{
	uint8		flags;
	uint32		relid;
	int			len;
	char	   *schemaname;
	char	   *relname;
	int			natts;
	char	  **attrnames;

	/* read the flags */
	flags = pq_getmsgbyte(in);
	Assert(flags == 0);

	relid = pq_getmsgint(in, 4);

	/* Read relation from stream */
	len = pq_getmsgbyte(in);
	schemaname = (char *) pq_getmsgbytes(in, len);

	len = pq_getmsgbyte(in);
	relname = (char *) pq_getmsgbytes(in, len);

	/* Get attribute description */
	pglogical_read_attrs(in, &attrnames, &natts);

	pglogical_relation_cache_update(relid, schemaname, relname, natts, attrnames);

	return relid;
}

/*
 * Read relation attributes from the outputstream.
 *
 * TODO handle flags.
 */
static void
pglogical_read_attrs(StringInfo in, char ***attrnames, int *nattrnames)
{
	int			i;
	uint16		nattrs;
	char	  **attrs;
	char		blocktype;

	blocktype = pq_getmsgbyte(in);
	if (blocktype != 'A')
		elog(ERROR, "expected ATTRS, got %c", blocktype);

	nattrs = pq_getmsgint(in, 2);
	attrs = palloc(nattrs * sizeof(char *));

	/* read the attributes */
	for (i = 0; i < nattrs; i++)
	{
		uint8			flags;
		uint16			len;

		blocktype = pq_getmsgbyte(in);		/* column definition follows */
		if (blocktype != 'C')
			elog(ERROR, "expected COLUMN, got %c", blocktype);
		flags = pq_getmsgbyte(in);
//		Assert(flags == 0);

		blocktype = pq_getmsgbyte(in);		/* column name block follows */
		if (blocktype != 'N')
			elog(ERROR, "expected NAME, got %c", blocktype);

		/* attribute name */
		len = pq_getmsgint(in, 2);
		/* the string is NULL terminated */
		attrs[i] = (char *) pq_getmsgbytes(in, len);
	}

	*attrnames = attrs;
	*nattrnames = nattrs;
}
