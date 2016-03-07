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

/* summon cross-PG-version compatibility voodoo */
#include "pglogical_output/compat.h"


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

/*
 * The startup parameter format is versioned separately to the rest of the wire
 * protocol because we negotiate the wire protocol version using the startup
 * parameters sent to us. It hopefully won't ever need to change, but this
 * field is present in case we do need to change it, e.g. to a structured json
 * object. We can look at the startup params version to see whether we can
 * understand the startup params sent by the client and to fall back to
 * reading an older format if needed.
 */
#define PGLOGICAL_STARTUP_PARAM_FORMAT_FLAT 1

/* struct definition appears in pglogical_output_internal.h */
typedef struct PGLogicalOutputData PGLogicalOutputData;

#endif /* PG_LOGICAL_OUTPUT_H */
