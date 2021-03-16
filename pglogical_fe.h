/*-------------------------------------------------------------------------
 *
 * pglogical_fe.h
 *              pglogical replication plugin
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *              pglogical_fe.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGLOGICAL_FE_H
#define PGLOGICAL_FE_H

extern int find_other_exec_version(const char *argv0, const char *target,
								   uint32 *version, char *retpath);

extern char *pgl_get_connstr(char *connstr, char *dbname, char *options, char **errmsg);

#endif /* PGLOGICAL_FE_H */
