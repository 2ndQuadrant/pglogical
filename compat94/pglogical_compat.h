#ifndef PG_LOGICAL_COMPAT_H
#define PG_LOGICAL_COMPAT_H

#include <signal.h>

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "nodes/pg_list.h"
#include "storage/lwlock.h"
#include "utils/array.h"

/* 9.4 lacks PG_*_MAX */
#ifndef PG_UINT32_MAX
#define PG_UINT32_MAX	(0xFFFFFFFF)
#endif

#ifndef PG_UINT16_MAX
#define PG_UINT16_MAX	(0xFFFF)
#endif

extern PGDLLIMPORT XLogRecPtr XactLastCommitEnd;

extern void BackgroundWorkerInitializeConnectionByOid(Oid dboid, Oid useroid);

extern ArrayType *strlist_to_textarray(List *list);

extern LWLockPadded *GetNamedLWLockTranche(const char *tranche_name);
extern void RequestNamedLWLockTranche(const char *tranche_name, int num_lwlocks);

#define GetConfigOptionByName(name, varname, missing_ok) \
(\
	AssertMacro(!missing_ok), \
	GetConfigOptionByName(name, varname) \
)

#endif
