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
#include "libpq-fe.h"
#include "postgres_fe.h"

/* Note the order is important for debian here. */
#if !defined(pg_attribute_printf)

/* GCC and XLC support format attributes */
#if defined(__GNUC__) || defined(__IBMC__)
#define pg_attribute_format_arg(a) __attribute__((format_arg(a)))
#define pg_attribute_printf(f,a) __attribute__((format(PG_PRINTF_ATTRIBUTE, f, a)))
#else
#define pg_attribute_format_arg(a)
#define pg_attribute_printf(f,a)
#endif

#endif

#include "pqexpbuffer.h"

#include "pglogical_fe.h"

static char *PQconninfoParamsToConnstr(const char *const * keywords, const char *const * values);
static void appendPQExpBufferConnstrValue(PQExpBuffer buf, const char *str);

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

/*
 * Build connection string from individual parameter.
 *
 * dbname can be specified in connstr parameter
 */
char *
pgl_get_connstr(char *connstr, char *dbname, char *options, char **errmsg)
{
	char		*ret;
	int			argcount = 1;	/* dbname */
	int			i;
	const char **keywords;
	const char **values;
	PQconninfoOption *conn_opts = NULL;
	PQconninfoOption *conn_opt;

	/*
	 * Merge the connection info inputs given in form of connection string
	 * and options
	 */
	i = 0;
	if (connstr &&
		(strncmp(connstr, "postgresql://", 13) == 0 ||
		 strncmp(connstr, "postgres://", 11) == 0 ||
		 strchr(connstr, '=') != NULL))
	{
		conn_opts = PQconninfoParse(connstr, errmsg);
		if (conn_opts == NULL)
			return NULL;

		for (conn_opt = conn_opts; conn_opt->keyword != NULL; conn_opt++)
		{
			if (conn_opt->val != NULL && conn_opt->val[0] != '\0')
				argcount++;
		}

		keywords = malloc((argcount + 2) * sizeof(*keywords));
		memset(keywords, 0, (argcount + 2) * sizeof(*keywords));
		values = malloc((argcount + 2) * sizeof(*values));
		memset(values, 0, (argcount + 2) * sizeof(*values));

		for (conn_opt = conn_opts; conn_opt->keyword != NULL; conn_opt++)
		{
			/* If db* parameters were provided, we'll fill them later. */
			if (dbname && strcmp(conn_opt->keyword, "dbname") == 0)
				continue;

			if (conn_opt->val != NULL && conn_opt->val[0] != '\0')
			{
				keywords[i] = conn_opt->keyword;
				values[i] = conn_opt->val;
				i++;
			}
		}
	}
	else
	{
		keywords = malloc((argcount + 2) * sizeof(*keywords));
		memset(keywords, 0, (argcount + 2) * sizeof(*keywords));
		values = malloc((argcount + 2) * sizeof(*values));
		memset(values, 0, (argcount + 2) * sizeof(*values));

		/*
		 * If connstr was provided but it's not in connection string format and
		 * the dbname wasn't provided then connstr is actually dbname.
		 */
		if (connstr && !dbname)
			dbname = connstr;
	}

	if (dbname)
	{
		keywords[i] = "dbname";
		values[i] = dbname;
		i++;
	}

	if (options)
	{
		keywords[i] = "options";
		values[i] = options;
	}

	ret = PQconninfoParamsToConnstr(keywords, values);

	/* Connection ok! */
	if (values)
		free(values);
	free(keywords);
	if (conn_opts)
		PQconninfoFree(conn_opts);

	return ret;
}

/*
 * Convert PQconninfoOption array into conninfo string
 */
static char *
PQconninfoParamsToConnstr(const char *const * keywords, const char *const * values)
{
	PQExpBuffer	 retbuf = createPQExpBuffer();
	char		*ret;
	int			 i = 0;

	for (i = 0; keywords[i] != NULL; i++)
	{
		if (i > 0)
			appendPQExpBufferChar(retbuf, ' ');
		appendPQExpBuffer(retbuf, "%s=", keywords[i]);
		appendPQExpBufferConnstrValue(retbuf, values[i]);
	}

	ret = strdup(retbuf->data);
	destroyPQExpBuffer(retbuf);

	return ret;
}

/*
 * Escape connection info value
 */
static void
appendPQExpBufferConnstrValue(PQExpBuffer buf, const char *str)
{
	const char *s;
	bool		needquotes;

	/*
	 * If the string consists entirely of plain ASCII characters, no need to
	 * quote it. This is quite conservative, but better safe than sorry.
	 */
	needquotes = false;
	for (s = str; *s; s++)
	{
		if (!((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') ||
			  (*s >= '0' && *s <= '9') || *s == '_' || *s == '.'))
		{
			needquotes = true;
			break;
		}
	}

	if (needquotes)
	{
		appendPQExpBufferChar(buf, '\'');
		while (*str)
		{
			/* ' and \ must be escaped by to \' and \\ */
			if (*str == '\'' || *str == '\\')
				appendPQExpBufferChar(buf, '\\');

			appendPQExpBufferChar(buf, *str);
			str++;
		}
		appendPQExpBufferChar(buf, '\'');
	}
	else
		appendPQExpBufferStr(buf, str);
}
