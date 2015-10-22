#ifndef PG_LOGICAL_COMPAT_H
#define PG_LOGICAL_COMPAT_H

#include "pg_config.h"

/* 9.4 lacks replication origins */
#if PG_VERSION_NUM >= 90500
#define HAVE_REPLICATION_ORIGINS
#else
/* To allow the same signature on hooks in 9.4 */
typedef uint16 RepOriginId;
#endif

/* 9.4 lacks PG_UINT32_MAX */
#ifndef PG_UINT32_MAX
#define PG_UINT32_MAX UINT32_MAX
#endif

#endif
