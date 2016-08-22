#ifndef PGLOGICAL_RELMETA_CACHE_H
#define PGLOGICAL_RELMETA_CACHE_H

#include "pglogical_output.h"

#include "utils/memutils.h"
#include "utils/relcache.h"


typedef struct PGLRelMetaCacheEntry
{
	Oid relid;
	/* Does the client have this relation cached? */
	bool is_cached;
	/* Entry is valid and not due to be purged */
	bool is_valid;
} PGLRelMetaCacheEntry;

extern void pglogical_init_relmetacache(MemoryContext decoding_context);
extern bool pglogical_cache_relmeta(PGLogicalOutputData *data, Relation rel, PGLRelMetaCacheEntry **entry);
extern void pglogical_destroy_relmetacache(void);
extern void pglogical_prune_relmetacache(void);

#endif /* PGLOGICAL_RELMETA_CACHE_H */
