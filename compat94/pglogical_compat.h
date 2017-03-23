#ifndef PG_LOGICAL_COMPAT_H
#define PG_LOGICAL_COMPAT_H

#include <signal.h>

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_trigger.h"
#include "commands/trigger.h"
#include "nodes/pg_list.h"
#include "storage/lwlock.h"
#include "utils/array.h"

/* 9.4 lacks PG_*_MAX */
#ifndef PG_UINT32_MAX
#define PG_UINT32_MAX	(0xFFFFFFFF)
#endif

#ifndef PG_INT32_MAX
#define PG_INT32_MAX	(0x7FFFFFFF)
#endif

#ifndef PG_INT32_MIN
#define PG_INT32_MIN	(-0x7FFFFFFF-1)
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

/* missing macros in 9.4 */
#define ObjectAddressSubSet(addr, class_id, object_id, object_sub_id) \
	do { \
		(addr).classId = (class_id); \
		(addr).objectId = (object_id); \
		(addr).objectSubId = (object_sub_id); \
	} while (0)

#define ObjectAddressSet(addr, class_id, object_id) \
	ObjectAddressSubSet(addr, class_id, object_id, 0)

inline ObjectAddress PGLCreateTrigger(CreateTrigStmt *stmt, const char *queryString,
			  Oid relOid, Oid refRelOid, Oid constraintOid, Oid indexOid,
			  bool isInternal)
{
	ObjectAddress myself;
	myself.classId = TriggerRelationId;
	myself.objectId = CreateTrigger(stmt, queryString, relOid, refRelOid, constraintOid, indexOid, isInternal);
	myself.objectSubId = 0;
	return myself;
}

#endif
