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
	const char *node_id;				/* hooks need to identify this node somehow */
	List   *table_change_filter;		/* name of the hook function */
	Oid		table_change_filter_oid;	/* cached oid of the hook function */
	uint32	table_change_filter_hash;	/* hash of the oid from the syscache */
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
