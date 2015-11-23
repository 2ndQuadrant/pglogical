/*-------------------------------------------------------------------------
 *
 * pglogical.c
 * 		pglogical initialization and common functionality
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  pglogical.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "pglogical_fe.h"

/*
 * Find another program in our binary's directory,
 * and return its version.
 */
int
find_other_exec_version(const char *argv0, const char *target,
						uint32 *version, char *retpath)
{
	char		cmd[MAXPGPATH];
	char		cmd_output[1024];
	FILE       *output;
	int			pre_dot,
				post_dot;

	if (find_my_exec(argv0, retpath) < 0)
		return -1;

	/* Trim off program name and keep just directory */
	*last_dir_separator(retpath) = '\0';
	canonicalize_path(retpath);

	/* Now append the other program's name */
	snprintf(retpath + strlen(retpath), MAXPGPATH - strlen(retpath),
			 "/%s%s", target, EXE);

	snprintf(cmd, sizeof(cmd), "\"%s\" -V", retpath);

	if ((output = popen(cmd, "r")) == NULL)
		return -1;

	if (fgets(cmd_output, sizeof(cmd_output), output) == NULL)
	{
		pclose(output);
		return -1;
	}
	pclose(output);

	if (sscanf(cmd_output, "%*s %*s %d.%d", &pre_dot, &post_dot) != 2)
		return -2;

	*version = (pre_dot * 100 + post_dot) * 100;

	return 0;
}
