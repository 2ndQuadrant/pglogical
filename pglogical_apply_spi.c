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
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/sysattr.h"

#include "executor/executor.h"
#include "executor/spi.h"

#include "tcop/pquery.h"
#include "tcop/utility.h"

#include "utils/rel.h"
#include "utils/builtins.h"

#include "pglogical_apply_spi.h"

/*
 * Handle begin (connect SPI).
 */
void
pglogical_apply_spi_begin(void)
{
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");
}

/*
 * Handle commit (finish SPI).
 */
void
pglogical_apply_spi_commit(void)
{
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");
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
		if (desc->attrs[att]->attisdropped)
			continue;

		if (!newtup->changed[att])
			continue;

		if (narg > 0)
			appendStringInfo(&cmd, ", %s", NameStr(desc->attrs[att]->attname));
		else
			appendStringInfo(&cmd, "%s", NameStr(desc->attrs[att]->attname));
		narg++;
	}

	appendStringInfoString(&cmd, ") VALUES (");

	for (att = 0, narg = 0; att < desc->natts; att++)
	{
		if (desc->attrs[att]->attisdropped)
			continue;

		if (!newtup->changed[att])
			continue;

		if (narg > 0)
			appendStringInfo(&cmd, ", $%u", narg + 1);
		else
			appendStringInfo(&cmd, "$%u", narg + 1);

		argtypes[narg] = desc->attrs[att]->atttypid;
		values[narg] = newtup->values[att];
		nulls[narg] = newtup->nulls[att] ? 'n' : ' ';
		narg++;
	}

	appendStringInfoString(&cmd, ")");

	if (SPI_execute_with_args(cmd.data, narg, argtypes, values, nulls, false,
							  0) != SPI_OK_INSERT)
		elog(ERROR, "SPI_execute_with_args failed");

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
		if (desc->attrs[att]->attisdropped)
			continue;

		if (!newtup->changed[att])
			continue;

		if (narg > 0)
			appendStringInfo(&cmd, ", %s = $%u",
							 NameStr(desc->attrs[att]->attname), narg + 1);
		else
			appendStringInfo(&cmd, "%s = $%u",
							 NameStr(desc->attrs[att]->attname), narg + 1);

		argtypes[narg] = desc->attrs[att]->atttypid;
		values[narg] = newtup->values[att];
		nulls[narg] = newtup->nulls[att] ? 'n' : ' ';
		narg++;
	}

	appendStringInfoString(&cmd, " WHERE");

	firstarg = narg;
	for (att = 0; att < desc->natts; att++)
	{
		if (!bms_is_member(desc->attrs[att]->attnum - FirstLowInvalidHeapAttributeNumber,
						   id_attrs))
			continue;

		if (narg > firstarg)
			appendStringInfo(&cmd, " AND %s = $%u",
							 NameStr(desc->attrs[att]->attname), narg + 1);
		else
			appendStringInfo(&cmd, " %s = $%u",
							 NameStr(desc->attrs[att]->attname), narg + 1);

		argtypes[narg] = desc->attrs[att]->atttypid;
		values[narg] = oldtup->values[att];
		nulls[narg] = oldtup->nulls[att] ? 'n' : ' ';
		narg++;
	}

	if (SPI_execute_with_args(cmd.data, narg, argtypes, values, nulls, false,
							  0) != SPI_OK_UPDATE)
		elog(ERROR, "SPI_execute_with_args failed");

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
		if (!bms_is_member(desc->attrs[att]->attnum - FirstLowInvalidHeapAttributeNumber,
						   id_attrs))
			continue;

		if (narg > 0)
			appendStringInfo(&cmd, " AND %s = $%u",
							 NameStr(desc->attrs[att]->attname), narg + 1);
		else
			appendStringInfo(&cmd, " %s = $%u",
							 NameStr(desc->attrs[att]->attname), narg + 1);

		argtypes[narg] = desc->attrs[att]->atttypid;
		values[narg] = oldtup->values[att];
		nulls[narg] = oldtup->nulls[att] ? 'n' : ' ';
		narg++;
	}

	if (SPI_execute_with_args(cmd.data, narg, argtypes, values, nulls, false,
							  0) != SPI_OK_DELETE)
		elog(ERROR, "SPI_execute_with_args failed");

	pfree(cmd.data);
}
