/*-------------------------------------------------------------------------
 *
 * pglogical_output.h
 *		pglogical output plugin
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pglogical_output.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_LOGICAL_OUTPUT_H
#define PG_LOGICAL_OUTPUT_H

#include "nodes/parsenodes.h"

#include "replication/logical.h"
#include "replication/output_plugin.h"

#include "storage/lock.h"

#include "pglogical_output/hooks.h"

#include "pglogical_proto.h"

/* XXYYZZ format version number and human readable version */
#define PGLOGICAL_OUTPUT_VERSION_NUM 10000
#define PGLOGICAL_OUTPUT_VERSION "1.0.0"

/*
 * Protocol capabilities
 *
 * PGLOGICAL_PROTO_VERSION_NUM is our native protocol and the greatest version
 * we can support. PGLOGICAL_PROTO_MIN_VERSION_NUM is the oldest version we
 * have backwards compatibility for. We negotiate protocol versions during the
 * startup handshake. See the protocol documentation for details.
 */
#define PGLOGICAL_PROTO_VERSION_NUM 1
#define PGLOGICAL_PROTO_MIN_VERSION_NUM 1

struct PGLogicalProtoAPI;

typedef struct PGLogicalOutputData
{
	MemoryContext context;

	struct PGLogicalProtoAPI *api;

	/* protocol */
	bool	allow_internal_basetypes;
	bool	allow_binary_basetypes;
	bool	forward_changeset_origins;
	int		field_datum_encoding;
	int		relmeta_cache_size;

	/*
	 * client info
	 *
	 * Lots of this should move to a separate shorter-lived struct used only
	 * during parameter reading, since it contains what the client asked for.
	 * Once we've processed this during startup we don't refer to it again.
	 */
	uint32	client_pg_version;
	uint32	client_max_proto_version;
	uint32	client_min_proto_version;
	const char *client_expected_encoding;
	const char *client_protocol_format;
	uint32  client_binary_basetypes_major_version;
	bool	client_want_internal_basetypes_set;
	bool	client_want_internal_basetypes;
	bool	client_want_binary_basetypes_set;
	bool	client_want_binary_basetypes;
	bool	client_binary_bigendian_set;
	bool	client_binary_bigendian;
	uint32	client_binary_sizeofdatum;
	uint32	client_binary_sizeofint;
	uint32	client_binary_sizeoflong;
	bool	client_binary_float4byval_set;
	bool	client_binary_float4byval;
	bool	client_binary_float8byval_set;
	bool	client_binary_float8byval;
	bool	client_binary_intdatetimes_set;
	bool	client_binary_intdatetimes;
	bool	client_no_txinfo;
	int   client_relmeta_cache_size;

	/* hooks */
	List *hooks_setup_funcname;
	struct PGLogicalHooks hooks;
	MemoryContext hooks_mctxt;

	/* DefElem<String> list populated by startup hook */
	List *extra_startup_params;
} PGLogicalOutputData;

#endif /* PG_LOGICAL_OUTPUT_H */
