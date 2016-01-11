#ifndef PG_LOGICAL_COMPAT_H
#define PG_LOGICAL_COMPAT_H

#include <signal.h>

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "nodes/pg_list.h"
#include "utils/array.h"

/* 9.4 lacks PG_UINT32_MAX */
#ifndef PG_UINT32_MAX
#define PG_UINT32_MAX UINT32_MAX
#endif

extern PGDLLIMPORT XLogRecPtr XactLastCommitEnd;

extern void BackgroundWorkerInitializeConnectionByOid(Oid dboid, Oid useroid);

extern ArrayType *strlist_to_textarray(List *list);

#endif
