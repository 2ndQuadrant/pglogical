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
	bool		replicate_inserts;
	bool		replicate_updates;
	bool		replicate_deletes;
} PGLogicalRepSet;

/* This is only valid within one output plugin instance/walsender. */
typedef struct PGLogicalRepSetRelation
{
	Oid				reloid;				/* key */

	bool			isvalid;			/* is this entry valid? */

	bool			replicate_inserts;	/* should inserts be replicated? */
	bool			replicate_updates;	/* should updates be replicated? */
	bool			replicate_deletes;	/* should deletes be replicated? */
} PGLogicalRepSetRelation;

extern PGLogicalRepSet *get_replication_set(int setid);
extern PGLogicalRepSet *get_replication_set_by_name(const char *setname,
													bool missing_ok);

extern List *get_replication_sets(List *replication_set_names);

extern bool relation_is_replicated(Relation rel, PGLogicalConnection *conn,
								   enum ReorderBufferChangeType action);

extern void create_replication_set(PGLogicalRepSet *repset);
extern void drop_replication_set(int setid);

extern void replication_set_add_table(int setid, Oid reloid);
extern void replication_set_remove_table(int setid, Oid reloid);

#endif /* PGLOGICAL_REPSET_H */

