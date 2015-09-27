/*-------------------------------------------------------------------------
 *
 * pglogical_relcache.h
 *		pglogical relation cache
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pglogical_relcache.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGLOGICAL_RELCACHE_H
#define PGLOGICAL_RELCACHE_H

typedef struct PGLogicalRelation
{
	/* Info coming from the remote side. */
	uint32		remoteid;
	const char *nspname;
	const char *relname;
	int			natts;
	char	  **attnames;

	/* Mapping to local relation, filled as needed. */
	Oid			reloid;
	Relation	rel;
	int		   *attmap;
} PGLogicalRelation;

extern void pglogical_relation_cache_update(uint32 remoteid,
											 char *schemaname, char *relname,
											 int natts, char **attnames);

extern PGLogicalRelation *pglogical_relation_open(uint32 remoteid,
												   LOCKMODE lockmode);
extern void pglogical_relation_close(PGLogicalRelation * rel,
									  LOCKMODE lockmode);
extern void pglogical_relation_invalidate_cb(Datum arg, Oid reloid);

#endif /* PGLOGICAL_RELCACHE_H */
