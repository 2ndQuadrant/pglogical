#ifndef PGLOGICAL_RELMETA_CACHE_H
#define PGLOGICAL_RELMETA_CACHE_H

struct PGLRelMetaCacheEntry
{
	Oid relid;
	/* Does the client have this relation cached? */
	bool is_cached;
	/* Field for API plugin use, must be alloc'd in decoding context */
	void *api_private;
};

struct PGLogicalOutputData;

extern void pglogical_init_relmetacache(void);
extern bool pglogical_cache_relmeta(struct PGLogicalOutputData *data, Relation rel, struct PGLRelMetaCacheEntry **entry);
extern void pglogical_destroy_relmetacache(void);

#endif /* PGLOGICAL_RELMETA_CACHE_H */
