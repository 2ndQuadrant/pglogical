#ifndef PG_LOGICAL_COMPAT_H
#define PG_LOGICAL_COMPAT_H

#include "utils/varlena.h"

#define PGLCreateTrigger CreateTrigger

#define GetCurrentIntegerTimestamp GetCurrentTimestamp

#define	PGLDoCopy(stmt, queryString, stmt_location, stmt_len, processed) DoCopy(NULL, stmt, stmt_location, stmt_len, processed)

#endif
