/*-------------------------------------------------------------------------
 *
 * pglogical_dependency.h
 *				Dependency handling
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *				pglogical_dependency.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGLOGICAL_DEPENDENCY_H
#define PGLOGICAL_DEPENDENCY_H

extern void pglogical_recordDependencyOn(const ObjectAddress *depender,
				   const ObjectAddress *referenced,
				   DependencyType behavior);

extern void pglogical_recordMultipleDependencies(const ObjectAddress *depender,
						   const ObjectAddress *referenced,
						   int nreferenced,
						   DependencyType behavior);

extern void pglogical_recordDependencyOnSingleRelExpr(const ObjectAddress *depender,
								Node *expr, Oid relId,
								DependencyType behavior,
								DependencyType self_behavior);

extern void pglogical_tryDropDependencies(const ObjectAddress *object,
										  DropBehavior behavior);

extern void pglogical_checkDependency(const ObjectAddress *object,
									  DropBehavior behavior);

#endif /* PGLOGICAL_DEPENDENCY_H */
