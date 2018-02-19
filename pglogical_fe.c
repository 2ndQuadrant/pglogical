/*-------------------------------------------------------------------------
 *
 * pglogical.c
 * 		pglogical utility functions shared between backend and frontend
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
	int			pre_dot = 0,
				post_dot = 0;

	if (find_my_exec(argv0, retpath) < 0)
		return -1;

	/* Trim off program name and keep just directory */
	*last_dir_separator(retpath) = '\0';
	canonicalize_path(retpath);

	/* Now append the other program's name */
	snprintf(retpath + strlen(retpath), MAXPGPATH - strlen(retpath),
			 "/%s%s", target, EXE);

	/* And request the version from the program */
	snprintf(cmd, sizeof(cmd), "\"%s\" --version", retpath);

	if ((output = popen(cmd, "r")) == NULL)
	{
		fprintf(stderr, "find_other_exec_version: couldn't open cmd: %s\n", strerror(errno));
		return -1;
	}

	if (fgets(cmd_output, sizeof(cmd_output), output) == NULL)
	{
		int ret = pclose(output);
		if (WIFEXITED(ret))
			fprintf(stderr, "find_other_exec_version: couldn't read output of \"%s\": %d (exited with return code %d)\n", cmd, ret, WEXITSTATUS(ret));
		else if (WIFSIGNALED(ret))
			fprintf(stderr, "find_other_exec_version: couldn't read output of \"%s\": %d (exited with signal %d)\n", cmd, ret, WTERMSIG(ret));
		else
			fprintf(stderr, "find_other_exec_version: couldn't read output of \"%s\": %d\n", cmd, ret);
		return -1;
	}
	pclose(output);

	if (sscanf(cmd_output, "%*s %*s %d.%d", &pre_dot, &post_dot) < 1)
	{
		fprintf(stderr, "find_other_exec_version: couldn't scan result \"%s\" as version\n", cmd_output);
		return -2;
	}

	/*
	  similar to version number exposed by "server_version_num" but without
	  the minor :
	  9.6.1 -> 90601  -> 90600
	  10.1  -> 100001 -> 100000)
	*/
	*version = (pre_dot < 10) ?
	  (pre_dot * 100 + post_dot) * 100 : pre_dot * 100 * 100;

	return 0;
}
