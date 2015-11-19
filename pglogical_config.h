#ifndef PG_LOGICAL_CONFIG_H
#define PG_LOGICAL_CONFIG_H

#ifndef PG_VERSION_NUM
#error <postgres.h> must be included first
#endif

#include "nodes/pg_list.h"
#include "pglogical_output.h"

inline static bool
server_float4_byval(void)
{
#ifdef USE_FLOAT4_BYVAL
	return true;
#else
	return false;
#endif
}

inline static bool
server_float8_byval(void)
{
#ifdef USE_FLOAT8_BYVAL
	return true;
#else
	return false;
#endif
}

inline static bool
server_integer_datetimes(void)
{
#ifdef USE_INTEGER_DATETIMES
	return true;
#else
	return false;
#endif
}

inline static bool
server_bigendian(void)
{
#ifdef WORDS_BIGENDIAN
	return true;
#else
	return false;
#endif
}

extern int process_parameters(List *options, PGLogicalOutputData *data);

extern List * prepare_startup_message(PGLogicalOutputData *data);

#endif
