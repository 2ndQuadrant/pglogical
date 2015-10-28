/*-------------------------------------------------------------------------
 *
 * pglogical_repset.h
 *		pglogical replication set manipulation functions
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pglogical_repset.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGLOGICAL_REPSET_H
#define PGLOGICAL_REPSET_H

#include "replication/reorderbuffer.h"

typedef struct PGLogicalRepSet
{
	int			id;
	const char *name;
	bool		replicate_insert;
	bool		replicate_update;
	bool		replicate_delete;
	bool		replicate_truncate;
} PGLogicalRepSet;

/* This is only valid within one output plugin instance/walsender. */
typedef struct PGLogicalRepSetRelation
{
	Oid				reloid;				/* key */

	bool			isvalid;			/* is this entry valid? */

	bool			replicate_insert;	/* should insert be replicated? */
	bool			replicate_update;	/* should update be replicated? */
	bool			replicate_delete;	/* should delete be replicated? */
	bool			replicate_truncate; /* should truncate be replicated? */
} PGLogicalRepSetRelation;

/* Change types, can't use ReorderBufferChangeType as it's missing TRUNCATE. */
typedef enum PGLogicalChangeType
{
	PGLogicalChangeInsert,
	PGLogicalChangeUpdate,
	PGLogicalChangeDelete,
	PGLogicalChangeTruncate
} PGLogicalChangeType;

extern PGLogicalRepSet *get_replication_set(int setid);
extern PGLogicalRepSet *get_replication_set_by_name(const char *setname,
													bool missing_ok);

extern List *get_replication_sets(List *replication_set_names, bool missing_ok);

extern bool relation_is_replicated(Relation rel, List *replication_set_names,
								   PGLogicalChangeType change_type);

extern void create_replication_set(PGLogicalRepSet *repset);
extern void drop_replication_set(int setid);

extern void replication_set_add_table(int setid, Oid reloid);
extern void replication_set_remove_table(int setid, Oid reloid);

extern PGLogicalChangeType to_pglogical_changetype(
		enum ReorderBufferChangeType change);

#endif /* PGLOGICAL_REPSET_H */

