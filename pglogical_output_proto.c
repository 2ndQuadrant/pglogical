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
#include "replication/reorderbuffer.h"
#include "pglogical_output_plugin.h"

#include "pglogical_output_proto.h"
#include "pglogical_proto_native.h"
#include "pglogical_proto_json.h"

PGLogicalProtoAPI *
pglogical_init_api(PGLogicalProtoType typ)
{
	PGLogicalProtoAPI  *res = palloc0(sizeof(PGLogicalProtoAPI));

	if (typ == PGLogicalProtoJson)
	{
		res->write_rel = NULL;
		res->write_begin = pglogical_json_write_begin;
		res->write_commit = pglogical_json_write_commit;
		res->write_origin = NULL;
		res->write_insert = pglogical_json_write_insert;
		res->write_update = pglogical_json_write_update;
		res->write_delete = pglogical_json_write_delete;
		res->write_startup_message = json_write_startup_message;
	}
	else
	{
		res->write_rel = pglogical_write_rel;
		res->write_begin = pglogical_write_begin;
		res->write_commit = pglogical_write_commit;
		res->write_origin = pglogical_write_origin;
		res->write_insert = pglogical_write_insert;
		res->write_update = pglogical_write_update;
		res->write_delete = pglogical_write_delete;
		res->write_startup_message = write_startup_message;
	}

	return res;
}
