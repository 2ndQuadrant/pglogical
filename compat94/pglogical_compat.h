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

/* 9.4 lacks pg_attribute_ macros, so we clone them from c.h */
/* ---- BEGIN COPIED SECTION FROM 9.5 c.h -------- */
/*
 * Attribute macros
 *
 * GCC: https://gcc.gnu.org/onlinedocs/gcc/Function-Attributes.html
 * GCC: https://gcc.gnu.org/onlinedocs/gcc/Type-Attributes.html
 * Sunpro: https://docs.oracle.com/cd/E18659_01/html/821-1384/gjzke.html
 * XLC: http://www-01.ibm.com/support/knowledgecenter/SSGH2K_11.1.0/com.ibm.xlc111.aix.doc/language_ref/function_attributes.html
 * XLC: http://www-01.ibm.com/support/knowledgecenter/SSGH2K_11.1.0/com.ibm.xlc111.aix.doc/language_ref/type_attrib.html
 */

/* only GCC supports the unused attribute */
#ifdef __GNUC__
#define pg_attribute_unused() __attribute__((unused))
#else
#define pg_attribute_unused()
#endif

/* GCC and XLC support format attributes */
#if defined(__GNUC__) || defined(__IBMC__)
#define pg_attribute_format_arg(a) __attribute__((format_arg(a)))
#define pg_attribute_printf(f,a) __attribute__((format(PG_PRINTF_ATTRIBUTE, f, a)))
#else
#define pg_attribute_format_arg(a)
#define pg_attribute_printf(f,a)
#endif

/* GCC, Sunpro and XLC support aligned, packed and noreturn */
#if defined(__GNUC__) || defined(__SUNPRO_C) || defined(__IBMC__)
#define pg_attribute_aligned(a) __attribute__((aligned(a)))
#define pg_attribute_noreturn() __attribute__((noreturn))
#define pg_attribute_packed() __attribute__((packed))
#define HAVE_PG_ATTRIBUTE_NORETURN 1
#else
/*
 * NB: aligned and packed are not given default definitions because they
 * affect code functionality; they *must* be implemented by the compiler
 * if they are to be used.
 */
#define pg_attribute_noreturn()
#endif

/*
 * Mark a point as unreachable in a portable fashion.  This should preferably
 * be something that the compiler understands, to aid code generation.
 * In assert-enabled builds, we prefer abort() for debugging reasons.
 */
#if defined(HAVE__BUILTIN_UNREACHABLE) && !defined(USE_ASSERT_CHECKING)
#define pg_unreachable() __builtin_unreachable()
#elif defined(_MSC_VER) && !defined(USE_ASSERT_CHECKING)
#define pg_unreachable() __assume(0)
#else
#define pg_unreachable() abort()
#endif

/* ---- END COPIED SECTION FROM 9.5 c.h -------- */

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

static inline ObjectAddress
PGLCreateTrigger(CreateTrigStmt *stmt, const char *queryString,
				 Oid relOid, Oid refRelOid, Oid constraintOid, Oid indexOid,
				 bool isInternal)
{
	ObjectAddress myself;
	myself.classId = TriggerRelationId;
	myself.objectId = CreateTrigger(stmt, queryString, relOid, refRelOid, constraintOid, indexOid, isInternal);
	myself.objectSubId = 0;
	return myself;
}

#define RawStmt Node

#define	PGLDoCopy(stmt, queryString, processed) DoCopy(stmt, queryString, processed)

#define PGLstandard_ProcessUtility(pstmt, queryString, context, params, queryEnv, dest, sentToRemote, completionTag) \
	standard_ProcessUtility(pstmt, queryString, context, params, dest, completionTag)

#define PGLnext_ProcessUtility_hook(pstmt, queryString, context, params, queryEnv, dest, sentToRemote, completionTag) \
	next_ProcessUtility_hook(pstmt, queryString, context, params, dest, completionTag)

extern Oid CatalogTupleInsert(Relation heapRel, HeapTuple tup);
extern void CatalogTupleUpdate(Relation heapRel, ItemPointer otid, HeapTuple tup);
extern void CatalogTupleDelete(Relation heapRel, ItemPointer tid);

#define pgl_heap_attisnull(tup, attnum, tupledesc) \
	heap_attisnull(tup, attnum)

#define ALLOCSET_DEFAULT_SIZES \
		ALLOCSET_DEFAULT_MINSIZE, \
		ALLOCSET_DEFAULT_INITSIZE, \
		ALLOCSET_DEFAULT_MAXSIZE

#endif
