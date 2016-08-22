/*-------------------------------------------------------------------------
 *
 * pglogical_proto.h
 *		pglogical protocol
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical_proto.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_LOGICAL_PROTO_H
#define PG_LOGICAL_PROTO_H

/* struct definition appears in pglogical_output_proto_internal.h */
typedef struct PGLogicalProtoAPI PGLogicalProtoAPI;

typedef enum PGLogicalProtoType
{
	PGLogicalProtoNative,
	PGLogicalProtoJson
} PGLogicalProtoType;

extern PGLogicalProtoAPI *pglogical_init_api(PGLogicalProtoType typ);

#endif /* PG_LOGICAL_PROTO_H */
