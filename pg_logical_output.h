/*-------------------------------------------------------------------------
 *
 * pg_logical_output.h
 *		pg_logical output plugin
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pg_logical_output.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_LOGICAL_OUTPUT_H
#define PG_LOGICAL_OUTPUT_H

#include "nodes/parsenodes.h"

#include "replication/logical.h"
#include "replication/output_plugin.h"

#include "storage/lock.h"

#define PG_LOGICAL_PROTO_VERSION_NUM 1
#define PG_LOGICAL_PROTO_MIN_VERSION_NUM 1

/*
 * The name of a hook function. This is used instead of the usual List*
 * because can serve as a hash key.
 *
 * Must be zeroed on allocation if used as a hash key since padding is
 * *not* ignored on compare.
 */
typedef struct HookFuncName
{
	/* funcname is more likely to be unique, so goes first */
	char    function[NAMEDATALEN];
	char    schema[NAMEDATALEN];
} HookFuncName;

typedef struct
{
	MemoryContext context;

	/* protocol */
	bool	allow_binary_protocol;
	bool	allow_sendrecv_protocol;
	bool	forward_changesets;

	/*
	 * client info
	 *
	 * TODO: Lots of this should move to a separate
	 * shorter-lived struct used only during parameter
	 * reading.
	 */
	uint32	client_pg_version;
	uint32	client_max_proto_version;
	uint32	client_min_proto_version;
	const char *client_expected_encoding;
	uint32  client_binary_basetypes_major_version;
	bool	client_want_binary_basetypes;
	bool	client_want_sendrecv_basetypes;
	bool	client_binary_bigendian_set;
	bool	client_binary_bigendian;
	bool	client_binary_sizeofdatum_set;
	bool	client_binary_sizeofdatum;
	bool	client_binary_sizeofint_set;
	bool	client_binary_sizeofint;
	bool	client_binary_sizeoflong_set;
	bool	client_binary_sizeoflong;
	bool	client_binary_float4byval_set;
	bool	client_binary_float4byval;
	bool	client_binary_float8byval_set;
	bool	client_binary_float8byval;
	bool	client_binary_intdatetimes_set;
	bool	client_binary_intdatetimes;

	/* hooks */
	HookFuncName	*table_change_filter;	/* Table filter hook function, if any */
	const char	*node_id;		/* hooks need to identify this node somehow */
} PGLogicalOutputData;

typedef struct PGLogicalTupleData
{
	Datum	values[MaxTupleAttributeNumber];
	bool	nulls[MaxTupleAttributeNumber];
	bool	changed[MaxTupleAttributeNumber];
} PGLogicalTupleData;

extern int
process_parameters(List *options, PGLogicalOutputData *data);

#endif /* PG_LOGICAL_OUTPUT_H */
