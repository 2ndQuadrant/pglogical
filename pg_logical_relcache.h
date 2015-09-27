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

extern void pg_logical_relation_cache_update(uint32 remoteid,
											 char *schemaname, char *relname,
											 int natts, char **attnames);

extern PGLogicalRelation *pg_logical_relation_open(uint32 remoteid,
												   LOCKMODE lockmode);
extern void pg_logical_relation_close(PGLogicalRelation * rel,
									  LOCKMODE lockmode);
extern void pg_logical_relation_invalidate_cb(Datum arg, Oid reloid);

#endif /* PG_LOGICAL_RELCACHE_H */
