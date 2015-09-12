/*-------------------------------------------------------------------------
 *
 * pg_logical_relcache.h
 *		pg_logical relation cache
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pg_logical_relcache.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_LOGICAL_RELCACHE_H
#define PG_LOGICAL_RELCACHE_H

typedef struct PGLogicalRelation
{
	/* Info coming from the remote side */
	uint32		remote_relid;
	const char *nspname;
	const char *relname;
	int			natts;
	char	  **attnames;

	/* internal used marker for cache handling */
	bool		used;

	/* Filled as needed */
	Oid			reloid;
	Relation	rel;
	int		   *attmap;
} PGLogicalRelation;

extern void pg_logical_relation_cache_update(uint32 remote_relid,
											 char *schemaname, char *relname,
											 int natts, char **attnames);

extern PGLogicalRelation *pg_logical_relation_open(uint32 remote_relid,
												   LOCKMODE lockmode);
extern void pg_logical_relation_close(PGLogicalRelation * rel,
									  LOCKMODE lockmode);
extern void pg_logical_invalidate_callback(Datum arg, Oid reloid);

#endif /* PG_LOGICAL_RELCACHE_H */
