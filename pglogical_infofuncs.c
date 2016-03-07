/*-------------------------------------------------------------------------
 *
 * pglogical_infofuncs.c
 *		  Logical Replication output plugin
 *
 * Copyright (c) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_infofuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "pglogical_output.h"

#include "fmgr.h"
#include "utils/builtins.h"


Datum pglogical_output_version(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pglogical_output_version);

Datum pglogical_output_version_num(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pglogical_output_version_num);

Datum pglogical_output_proto_version(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pglogical_output_proto_version);

Datum pglogical_output_min_proto_version(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pglogical_output_min_proto_version);

Datum
pglogical_output_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(PGLOGICAL_OUTPUT_VERSION));
}

Datum
pglogical_output_version_num(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(PGLOGICAL_OUTPUT_VERSION_NUM);
}

Datum
pglogical_output_proto_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(PGLOGICAL_PROTO_VERSION_NUM);
}

Datum
pglogical_output_min_proto_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(PGLOGICAL_PROTO_MIN_VERSION_NUM);
}
