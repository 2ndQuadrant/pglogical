/*-------------------------------------------------------------------------
 *
 * pglogical_output_internal.h
 *		pglogical output internal definitions
 *
 * Copyright (c) 2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_output_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGLOGICAL_OUTPUT_INTERNAL_H
#define PGLOGICAL_OUTPUT_INTERNAL_H

#include "nodes/pg_list.h"

#include "pglogical_hooks.h"
#include "pglogical_proto_internal.h"

/* typedef appears in pglogical_output.h */
struct PGLogicalOutputData
{
	MemoryContext context;

	PGLogicalProtoAPI *api;

	/* protocol */
	bool		allow_internal_basetypes;
	bool		allow_binary_basetypes;
	bool		forward_changeset_origins;
	int			field_datum_encoding;

	/*
	 * client info
	 *
	 * Lots of this should move to a separate shorter-lived struct used only
	 * during parameter reading, since it contains what the client asked for.
	 * Once we've processed this during startup we don't refer to it again.
	 */
	uint32		client_pg_version;
	uint32		client_max_proto_version;
	uint32		client_min_proto_version;
	const char *client_expected_encoding;
	const char *client_protocol_format;
	uint32		client_binary_basetypes_major_version;
	bool		client_want_internal_basetypes_set;
	bool		client_want_internal_basetypes;
	bool		client_want_binary_basetypes_set;
	bool		client_want_binary_basetypes;
	bool		client_binary_bigendian_set;
	bool		client_binary_bigendian;
	uint32		client_binary_sizeofdatum;
	uint32		client_binary_sizeofint;
	uint32		client_binary_sizeoflong;
	bool		client_binary_float4byval_set;
	bool		client_binary_float4byval;
	bool		client_binary_float8byval_set;
	bool		client_binary_float8byval;
	bool		client_binary_intdatetimes_set;
	bool		client_binary_intdatetimes;
	bool		client_no_txinfo;

	/* hooks */
	List	   *hooks_setup_funcname;
	PGLogicalHooks hooks;
	/*
	 * The hooks_session_mctxt has the same lifetime as the decoding
	 * session's memory context. It's mainly there so that it's easier
	 * to keep track of memory used by hooks when debugging.
	 *
	 * Individual hook callbacks are called in a shorter lived context. If
	 * they want to allocate memory that persists longer than the callback
	 * they must switch to the hooks memory context.
	 */
	MemoryContext hooks_session_mctxt;

	/* DefElem<String> list populated by startup hook */
	List	   *extra_startup_params;
};

#endif   /* PGLOGICAL_OUTPUT_INTERNAL_H */
