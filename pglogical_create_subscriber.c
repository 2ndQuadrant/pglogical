/* -------------------------------------------------------------------------
 *
 * pglogical_create_subscriber.c
 *		Initialize a new pglogical subscriber from a physical base backup
 *
 * Copyright (C) 2012-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		pglogical_create_subscriber.c
 *
 * -------------------------------------------------------------------------
 */

#include <dirent.h>
#include <fcntl.h>
#include <locale.h>
#include <signal.h>
#include <time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>

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

#include "libpq-fe.h"
#include "postgres_fe.h"
#include "pqexpbuffer.h"

#include "getopt_long.h"

#include "miscadmin.h"

#include "access/timeline.h"
#include "access/xlog_internal.h"
#include "catalog/pg_control.h"

#include "pglogical_fe.h"

#define MAX_APPLY_DELAY 86400

typedef struct RemoteInfo {
	Oid			nodeid;
	char	   *node_name;
	char	   *sysid;
	char	   *dbname;
	char	   *replication_sets;
} RemoteInfo;

typedef enum {
	VERBOSITY_NORMAL,
	VERBOSITY_VERBOSE,
	VERBOSITY_DEBUG
} VerbosityLevelEnum;

static char		   *argv0 = NULL;
static const char  *progname;
static char		   *data_dir = NULL;
static char			pid_file[MAXPGPATH];
static time_t		start_time;
static VerbosityLevelEnum	verbosity = VERBOSITY_NORMAL;

/* defined as static so that die() can close them */
static PGconn		*subscriber_conn = NULL;
static PGconn		*provider_conn = NULL;

static void signal_handler(int sig);
static void usage(void);
static void die(const char *fmt,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));
static void print_msg(VerbosityLevelEnum level, const char *fmt,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

static int run_pg_ctl(const char *arg);
static void run_basebackup(const char *provider_connstr, const char *data_dir,
	const char *extra_basebackup_args);
static void wait_postmaster_connection(const char *connstr);
static void wait_primary_connection(const char *connstr);
static void wait_postmaster_shutdown(void);

static char *validate_replication_set_input(char *replication_sets);

static void remove_unwanted_data(PGconn *conn);
static void initialize_replication_origin(PGconn *conn, char *origin_name, char *remote_lsn);
static char *create_restore_point(PGconn *conn, char *restore_point_name);
static char *initialize_replication_slot(PGconn *conn, char *dbname,
							char *provider_node_name, char *subscription_name,
							bool drop_slot_if_exists);
static void pglogical_subscribe(PGconn *conn, char *subscriber_name,
								char *subscriber_dsn,
								char *provider_connstr,
								char *replication_sets,
								int apply_delay,
								bool force_text_transfer);

static RemoteInfo *get_remote_info(PGconn* conn);

static bool extension_exists(PGconn *conn, const char *extname);
static void install_extension(PGconn *conn, const char *extname);

static void initialize_data_dir(char *data_dir, char *connstr,
					char *postgresql_conf, char *pg_hba_conf,
					char *extra_basebackup_args);
static bool check_data_dir(char *data_dir, RemoteInfo *remoteinfo);

static char *read_sysid(const char *data_dir);

static void WriteRecoveryConf(PQExpBuffer contents);
static void CopyConfFile(char *fromfile, char *tofile, bool append);

static char *get_connstr_dbname(char *connstr);
static char *get_connstr(char *connstr, char *dbname);
static char *PQconninfoParamsToConnstr(const char *const * keywords, const char *const * values);
static void appendPQExpBufferConnstrValue(PQExpBuffer buf, const char *str);

static bool file_exists(const char *path);
static bool is_pg_dir(const char *path);
static void copy_file(char *fromfile, char *tofile, bool append);
static char *find_other_exec_or_die(const char *argv0, const char *target);
static bool postmaster_is_alive(pid_t pid);
static long get_pgpid(void);
static char **get_database_list(char *databases, int *n_databases);
static char *generate_restore_point_name(void);

static PGconn *
connectdb(const char *connstr)
{
	PGconn *conn;

	conn = PQconnectdb(connstr);
	if (PQstatus(conn) != CONNECTION_OK)
		die(_("Connection to database failed: %s, connection string was: %s\n"), PQerrorMessage(conn), connstr);

	return conn;
}

void signal_handler(int sig)
{
	if (sig == SIGINT)
	{
		die(_("\nCanceling...\n"));
	}
}


int
main(int argc, char **argv)
{
	int	i;
	int	c;
	PQExpBuffer recoveryconfcontents = createPQExpBuffer();
	RemoteInfo *remote_info;
	char	   *remote_lsn;
	bool		stop = false;
	bool		drop_slot_if_exists = false;
	int			optindex;
	char	   *subscriber_name = NULL;
	char	   *base_sub_connstr = NULL;
	char	   *base_prov_connstr = NULL;
	char	   *replication_sets = NULL;
	char       *databases = NULL;
	char	   *postgresql_conf = NULL,
			   *pg_hba_conf = NULL,
			   *recovery_conf = NULL;
	int			apply_delay = 0;
	bool		force_text_transfer = false;
	char	  **slot_names;
	char       *sub_connstr;
	char       *prov_connstr;
	char      **database_list = { NULL };
	int         n_databases = 1;
	int         dbnum;
	bool		use_existing_data_dir = false;
	int			pg_ctl_ret,
				logfd;
	char	   *restore_point_name = NULL;
	char	   *extra_basebackup_args = NULL;

	static struct option long_options[] = {
		{"subscriber-name", required_argument, NULL, 'n'},
		{"pgdata", required_argument, NULL, 'D'},
		{"provider-dsn", required_argument, NULL, 1},
		{"subscriber-dsn", required_argument, NULL, 2},
		{"replication-sets", required_argument, NULL, 3},
		{"postgresql-conf", required_argument, NULL, 4},
		{"hba-conf", required_argument, NULL, 5},
		{"recovery-conf", required_argument, NULL, 6},
		{"stop", no_argument, NULL, 's'},
		{"drop-slot-if-exists", no_argument, NULL, 7},
		{"apply-delay", required_argument, NULL, 8},
		{"databases", required_argument, NULL, 9},
		{"extra-basebackup-args", required_argument, NULL, 10},
		{"text-types", no_argument, NULL, 11},
		{NULL, 0, NULL, 0}
	};

	argv0 = argv[0];
	progname = get_progname(argv[0]);
	start_time = time(NULL);
	signal(SIGINT, signal_handler);

	/* check for --help */
	if (argc > 1)
	{
		for (i = 1; i < argc; i++)
		{
			if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-?") == 0)
			{
				usage();
				exit(0);
			}
		}
	}

	/* Option parsing and validation */
	while ((c = getopt_long(argc, argv, "D:n:sv", long_options, &optindex)) != -1)
	{
		switch (c)
		{
			case 'D':
				data_dir = pg_strdup(optarg);
				break;
			case 'n':
				subscriber_name = pg_strdup(optarg);
				break;
			case 1:
				base_prov_connstr = pg_strdup(optarg);
				break;
			case 2:
				base_sub_connstr = pg_strdup(optarg);
				break;
			case 3:
				replication_sets = validate_replication_set_input(pg_strdup(optarg));
				break;
			case 4:
				{
					postgresql_conf = pg_strdup(optarg);
					if (postgresql_conf != NULL && !file_exists(postgresql_conf))
						die(_("The specified postgresql.conf file does not exist."));
					break;
				}
			case 5:
				{
					pg_hba_conf = pg_strdup(optarg);
					if (pg_hba_conf != NULL && !file_exists(pg_hba_conf))
						die(_("The specified pg_hba.conf file does not exist."));
					break;
				}
			case 6:
				{
					recovery_conf = pg_strdup(optarg);
					if (recovery_conf != NULL && !file_exists(recovery_conf))
						die(_("The specified recovery configuration file does not exist."));
					break;
				}
			case 'v':
				verbosity++;
				break;
			case 's':
				stop = true;
				break;
			case 7:
				drop_slot_if_exists = true;
				break;
			case 8:
				apply_delay = atoi(optarg);
				break;
			case 9:
				databases = pg_strdup(optarg);
				break;
			case 10:
				extra_basebackup_args = pg_strdup(optarg);
				break;
			case 11:
				force_text_transfer = true;
				break;
			default:
				fprintf(stderr, _("Unknown option\n"));
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
				exit(1);
		}
	}

	/*
	 * Sanity checks
	 */

	if (data_dir == NULL)
	{
		fprintf(stderr, _("No data directory specified\n"));
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}
	else if (subscriber_name == NULL)
	{
		fprintf(stderr, _("No subscriber name specified\n"));
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}

	if (!base_prov_connstr || !strlen(base_prov_connstr))
		die(_("Provider connection string must be specified.\n"));
	if (!base_sub_connstr || !strlen(base_sub_connstr))
		die(_("Subscriber connection string must be specified.\n"));

	if (apply_delay < 0)
		die(_("Apply delay cannot be negative.\n"));

	if (apply_delay > MAX_APPLY_DELAY)
		die(_("Apply delay cannot be more than %d.\n"), MAX_APPLY_DELAY);

	if (!replication_sets || !strlen(replication_sets))
		replication_sets = "default,default_insert_only,ddl_sql";

	/* Init random numbers used for slot suffixes, etc */
	srand(time(NULL));

	/* Parse database list or connection string. */
	if (databases != NULL)
	{
		database_list = get_database_list(databases, &n_databases);
	}
	else
	{
		char *dbname = get_connstr_dbname(base_prov_connstr);

		if (!dbname)
			die(_("Either provider connection string must contain database "
				  "name or --databases option must be specified.\n"));

		n_databases = 1;
		database_list = palloc(n_databases * sizeof(char *));
		database_list[0] = dbname;
	}

	slot_names = palloc(n_databases * sizeof(char *));

	/*
	 * Check connection strings for validity before doing anything
	 * expensive.
	 */
	for (dbnum = 0; dbnum < n_databases; dbnum++)
	{
		char *db = database_list[dbnum];

		prov_connstr = get_connstr(base_prov_connstr, db);
		if (!prov_connstr || !strlen(prov_connstr))
			die(_("Provider connection string is not valid.\n"));

		sub_connstr = get_connstr(base_sub_connstr, db);
		if (!sub_connstr || !strlen(sub_connstr))
			die(_("Subscriber connection string is not valid.\n"));
	}

	/*
	 * Create log file where new postgres instance will log to while being
	 * initialized.
	 */
	logfd = open("pglogical_create_subscriber_postgres.log", O_CREAT | O_RDWR,
				 S_IRUSR | S_IWUSR);
	if (logfd == -1)
	{
		die(_("Creating pglogical_create_subscriber_postgres.log failed: %s"),
			strerror(errno));
	}
	/* Safe to close() unchecked, we didn't write */
	(void) close(logfd);

	/* Let's start the real work... */
	print_msg(VERBOSITY_NORMAL, _("%s: starting ...\n"), progname);

	for (dbnum = 0; dbnum < n_databases; dbnum++)
	{
		char *db = database_list[dbnum];

		prov_connstr = get_connstr(base_prov_connstr, db);
		if (!prov_connstr || !strlen(prov_connstr))
			die(_("Provider connection string is not valid.\n"));

		/* Read the remote server indetification. */
		print_msg(VERBOSITY_NORMAL,
				  _("Getting information for database %s ...\n"), db);
		provider_conn = connectdb(prov_connstr);
		remote_info = get_remote_info(provider_conn);

		/* only need to do this piece once */

		if (dbnum == 0)
		{
			use_existing_data_dir = check_data_dir(data_dir, remote_info);

			if (use_existing_data_dir &&
				strcmp(remote_info->sysid, read_sysid(data_dir)) != 0)
				die(_("Subscriber data directory is not basebackup of remote node.\n"));
		}

		/*
		 * Create replication slots on remote node.
		 */
		print_msg(VERBOSITY_NORMAL,
				  _("Creating replication slot in database %s ...\n"), db);
		slot_names[dbnum] = initialize_replication_slot(provider_conn,
														remote_info->dbname,
														remote_info->node_name,
														subscriber_name,
														drop_slot_if_exists);
		PQfinish(provider_conn);
		provider_conn = NULL;
	}

	/*
	 * Create basebackup or use existing one
	 */
	prov_connstr = get_connstr(base_prov_connstr, database_list[0]);
	sub_connstr = get_connstr(base_sub_connstr, database_list[0]);

	initialize_data_dir(data_dir,
						use_existing_data_dir ? NULL : prov_connstr,
						postgresql_conf, pg_hba_conf,
						extra_basebackup_args);
	snprintf(pid_file, MAXPGPATH, "%s/postmaster.pid", data_dir);

	restore_point_name = generate_restore_point_name();

	print_msg(VERBOSITY_NORMAL, _("Creating restore point \"%s\" on remote node ...\n"),
		restore_point_name);
	provider_conn = connectdb(prov_connstr);
	remote_lsn = create_restore_point(provider_conn, restore_point_name);
	PQfinish(provider_conn);
	provider_conn = NULL;

	/*
	 * Get subscriber db to consistent state (for lsn after slot creation).
	 */
	print_msg(VERBOSITY_NORMAL,
			  _("Bringing subscriber node to the restore point ...\n"));
	if (recovery_conf)
	{
#if PG_VERSION_NUM >= 120000
		CopyConfFile(recovery_conf, "postgresql.auto.conf", true);
#else
		CopyConfFile(recovery_conf, "recovery.conf", false);
#endif
	}
	else
	{
#if PG_VERSION_NUM < 120000
		appendPQExpBuffer(recoveryconfcontents, "standby_mode = 'on'\n");
#endif
		appendPQExpBuffer(recoveryconfcontents, "primary_conninfo = '%s'\n",
								escape_single_quotes_ascii(prov_connstr));
	}
	appendPQExpBuffer(recoveryconfcontents, "recovery_target_name = '%s'\n", restore_point_name);
	appendPQExpBuffer(recoveryconfcontents, "recovery_target_inclusive = true\n");
#if PG_VERSION_NUM >= 90500
	appendPQExpBuffer(recoveryconfcontents, "recovery_target_action = promote\n");
#else
	appendPQExpBuffer(recoveryconfcontents, "pause_at_recovery_target = false\n");
#endif
	WriteRecoveryConf(recoveryconfcontents);

	free(restore_point_name);
	restore_point_name = NULL;

	/*
	 * Start subscriber node with pglogical disabled, and wait until it starts
	 * accepting connections which means it has caught up to the restore point.
	 */
	pg_ctl_ret = run_pg_ctl("start -l \"pglogical_create_subscriber_postgres.log\" -o \"-c shared_preload_libraries=''\"");
	if (pg_ctl_ret != 0)
		die(_("Postgres startup for restore point catchup failed with %d. See pglogical_create_subscriber_postgres.log."), pg_ctl_ret);

	wait_primary_connection(sub_connstr);

	/*
	 * Clean any per-node data that were copied by pg_basebackup.
	 */
	print_msg(VERBOSITY_VERBOSE,
			  _("Removing old pglogical configuration ...\n"));

	for (dbnum = 0; dbnum < n_databases; dbnum++)
	{
		char *db = database_list[dbnum];

		sub_connstr = get_connstr(base_sub_connstr, db);

		if (!sub_connstr || !strlen(sub_connstr))
			die(_("Subscriber connection string is not valid.\n"));

		subscriber_conn = connectdb(sub_connstr);
		remove_unwanted_data(subscriber_conn);
		PQfinish(subscriber_conn);
		subscriber_conn = NULL;
	}

	/* Stop Postgres so we can reset system id and start it with pglogical loaded. */
	pg_ctl_ret = run_pg_ctl("stop");
	if (pg_ctl_ret != 0)
		die(_("Postgres stop after restore point catchup failed with %d. See pglogical_create_subscriber_postgres.log."), pg_ctl_ret);
	wait_postmaster_shutdown();

	/*
	 * Start the node again, now with pglogical active so that we can start the
	 * logical replication. This is final start, so don't log to to special log
	 * file anymore.
	 */
	print_msg(VERBOSITY_NORMAL,
			  _("Initializing pglogical on the subscriber node:\n"));

	pg_ctl_ret = run_pg_ctl("start");
	if (pg_ctl_ret != 0)
		die(_("Postgres restart with pglogical enabled failed with %d."), pg_ctl_ret);
	wait_postmaster_connection(base_sub_connstr);

	for (dbnum = 0; dbnum < n_databases; dbnum++)
	{
		char *db = database_list[dbnum];

		sub_connstr = get_connstr(base_sub_connstr, db);
		prov_connstr = get_connstr(base_prov_connstr, db);

		subscriber_conn = connectdb(sub_connstr);

		/* Create the extension. */
		print_msg(VERBOSITY_VERBOSE,
				  _("Creating pglogical extension for database %s...\n"), db);
		if (PQserverVersion(subscriber_conn) < 90500)
			install_extension(subscriber_conn, "pglogical_origin");
		install_extension(subscriber_conn, "pglogical");

		/*
		 * Create the identifier which is setup with the position to which we
		 * already caught up using physical replication.
		 */
		print_msg(VERBOSITY_VERBOSE,
				  _("Creating replication origin for database %s...\n"), db);
		initialize_replication_origin(subscriber_conn, slot_names[dbnum], remote_lsn);

		/*
		 * And finally add the node to the cluster.
		 */
		print_msg(VERBOSITY_NORMAL, _("Creating subscriber %s for database %s...\n"),
				  subscriber_name, db);
		print_msg(VERBOSITY_VERBOSE, _("Replication sets: %s\n"), replication_sets);

		pglogical_subscribe(subscriber_conn, subscriber_name, sub_connstr,
							prov_connstr, replication_sets, apply_delay,
							force_text_transfer);

		PQfinish(subscriber_conn);
		subscriber_conn = NULL;
	}

	/* If user does not want the node to be running at the end, stop it. */
	if (stop)
	{
		print_msg(VERBOSITY_NORMAL, _("Stopping the subscriber node ...\n"));
		pg_ctl_ret = run_pg_ctl("stop");
		if (pg_ctl_ret != 0)
			die(_("Stopping postgres after successful subscribtion failed with %d."), pg_ctl_ret);
		wait_postmaster_shutdown();
	}

	print_msg(VERBOSITY_NORMAL, _("All done\n"));

	return 0;
}


/*
 * Print help.
 */
static void
usage(void)
{
	printf(_("%s create new pglogical subscriber from basebackup of provider.\n\n"), progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]...\n"), progname);
	printf(_("\nGeneral options:\n"));
	printf(_("  -D, --pgdata=DIRECTORY      data directory to be used for new node,\n"));
	printf(_("                              can be either empty/non-existing directory,\n"));
	printf(_("                              or directory populated using\n"));
	printf(_("                              pg_basebackup -X stream command\n"));
	printf(_("  --databases                 optional list of databases to replicate\n"));
	printf(_("  -n, --subscriber-name=NAME  name of the newly created subscrber\n"));
	printf(_("  --subscriber-dsn=CONNSTR    connection string to the newly created subscriber\n"));
	printf(_("  --provider-dsn=CONNSTR      connection string to the provider\n"));
	printf(_("  --replication-sets=SETS     comma separated list of replication set names\n"));
	printf(_("  --apply-delay=DELAY         apply delay in seconds (by default 0)\n"));
	printf(_("  --drop-slot-if-exists       drop replication slot of conflicting name\n"));
	printf(_("  -s, --stop                  stop the server once the initialization is done\n"));
	printf(_("  -v                          increase logging verbosity\n"));
	printf(_("  --extra-basebackup-args     additional arguments to pass to pg_basebackup.\n"));
	printf(_("                              Safe options: -T, -c, --xlogdir/--waldir\n"));
	printf(_("\nConfiguration files override:\n"));
	printf(_("  --hba-conf              path to the new pg_hba.conf\n"));
	printf(_("  --postgresql-conf       path to the new postgresql.conf\n"));
	printf(_("  --recovery-conf         path to the template recovery configuration\n"));
}

/*
 * Print error and exit.
 */
static void
die(const char *fmt,...)
{
	va_list argptr;
	va_start(argptr, fmt);
	vfprintf(stderr, fmt, argptr);
	va_end(argptr);

	if (subscriber_conn)
		PQfinish(subscriber_conn);
	if (provider_conn)
		PQfinish(provider_conn);

	if (get_pgpid())
	{
		if (!run_pg_ctl("stop -s"))
		{
			fprintf(stderr, _("WARNING: postgres seems to be running, but could not be stopped\n"));
		}
	}

	exit(1);
}

/*
 * Print message to stdout and flush
 */
static void
print_msg(VerbosityLevelEnum level, const char *fmt,...)
{
	if (verbosity >= level)
	{
		va_list argptr;
		va_start(argptr, fmt);
		vfprintf(stdout, fmt, argptr);
		va_end(argptr);
		fflush(stdout);
	}
}


/*
 * Start pg_ctl with given argument(s) - used to start/stop postgres
 *
 * Returns the exit code reported by pg_ctl. If pg_ctl exits due to a
 * signal this call will die and not return.
 */
static int
run_pg_ctl(const char *arg)
{
	int			 ret;
	PQExpBuffer  cmd = createPQExpBuffer();
	char		*exec_path = find_other_exec_or_die(argv0, "pg_ctl");

	appendPQExpBuffer(cmd, "%s %s -D \"%s\" -s", exec_path, arg, data_dir);

	/* Run pg_ctl in silent mode unless we run in debug mode. */
	if (verbosity < VERBOSITY_DEBUG)
		appendPQExpBuffer(cmd, " -s");

	print_msg(VERBOSITY_DEBUG, _("Running pg_ctl: %s.\n"), cmd->data);
	ret = system(cmd->data);

	destroyPQExpBuffer(cmd);

	if (WIFEXITED(ret))
		return WEXITSTATUS(ret);
	else if (WIFSIGNALED(ret))
		die(_("pg_ctl exited with signal %d"), WTERMSIG(ret));
	else
		die(_("pg_ctl exited for an unknown reason (system() returned %d)"), ret);

	return -1;
}


/*
 * Run pg_basebackup to create the copy of the origin node.
 */
static void
run_basebackup(const char *provider_connstr, const char *data_dir,
	const char *extra_basebackup_args)
{
	int			 ret;
	PQExpBuffer  cmd = createPQExpBuffer();
	char		*exec_path = find_other_exec_or_die(argv0, "pg_basebackup");

	appendPQExpBuffer(cmd, "%s -D \"%s\" -d \"%s\" -X s -P", exec_path, data_dir, provider_connstr);

	/* Run pg_basebackup in verbose mode if we are running in verbose mode. */
	if (verbosity >= VERBOSITY_VERBOSE)
		appendPQExpBuffer(cmd, " -v");

	if (extra_basebackup_args != NULL)
		appendPQExpBuffer(cmd, "%s", extra_basebackup_args);

	print_msg(VERBOSITY_DEBUG, _("Running pg_basebackup: %s.\n"), cmd->data);
	ret = system(cmd->data);

	destroyPQExpBuffer(cmd);

	if (WIFEXITED(ret) && WEXITSTATUS(ret) == 0)
		return;
	if (WIFEXITED(ret))
		die(_("pg_basebackup failed with exit status %d, cannot continue.\n"), WEXITSTATUS(ret));
	else if (WIFSIGNALED(ret))
		die(_("pg_basebackup exited with signal %d, cannot continue"), WTERMSIG(ret));
	else
		die(_("pg_basebackup exited for an unknown reason (system() returned %d)"), ret);
}

/*
 * Init the datadir
 *
 * This function can either ensure provided datadir is a postgres datadir,
 * or create it using pg_basebackup.
 *
 * In any case, new postresql.conf and pg_hba.conf will be copied to the
 * datadir if they are provided.
 */
static void
initialize_data_dir(char *data_dir, char *connstr,
					char *postgresql_conf, char *pg_hba_conf,
					char *extra_basebackup_args)
{
	if (connstr)
	{
		print_msg(VERBOSITY_NORMAL,
				  _("Creating base backup of the remote node...\n"));
		run_basebackup(connstr, data_dir, extra_basebackup_args);
	}

	if (postgresql_conf)
		CopyConfFile(postgresql_conf, "postgresql.conf", false);
	if (pg_hba_conf)
		CopyConfFile(pg_hba_conf, "pg_hba.conf", false);
}

/*
 * This function checks if provided datadir is clone of the remote node
 * described by the remote info, or if it's emtpy directory that can be used
 * as new datadir.
 */
static bool
check_data_dir(char *data_dir, RemoteInfo *remoteinfo)
{
	/* Run basebackup as needed. */
	switch (pg_check_dir(data_dir))
	{
		case 0:		/* Does not exist */
		case 1:		/* Exists, empty */
				return false;
		case 2:
		case 3:		/* Exists, not empty */
		case 4:
			{
				if (!is_pg_dir(data_dir))
					die(_("Directory \"%s\" exists but is not valid postgres data directory.\n"),
						data_dir);
				return true;
			}
		case -1:	/* Access problem */
			die(_("Could not access directory \"%s\": %s.\n"),
				data_dir, strerror(errno));
	}

	/* Unreachable */
	die(_("Unexpected result from pg_check_dir() call"));
	return false;
}

/*
 * Initialize replication slots
 */
static char *
initialize_replication_slot(PGconn *conn, char *dbname,
							char *provider_node_name, char *subscription_name,
							bool drop_slot_if_exists)
{
	PQExpBufferData		query;
	char			   *slot_name;
	PGresult		   *res;

	/* Generate the slot name. */
	initPQExpBuffer(&query);
	printfPQExpBuffer(&query,
					  "SELECT pglogical.pglogical_gen_slot_name(%s, %s, %s)",
					  PQescapeLiteral(conn, dbname, strlen(dbname)),
					  PQescapeLiteral(conn, provider_node_name,
									  strlen(provider_node_name)),
					  PQescapeLiteral(conn, subscription_name,
									  strlen(subscription_name)));

	res = PQexec(conn, query.data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		die(_("Could generate slot name: %s"), PQerrorMessage(conn));

	slot_name = pstrdup(PQgetvalue(res, 0, 0));

	PQclear(res);
	resetPQExpBuffer(&query);

	/* Check if the current slot exists. */
	printfPQExpBuffer(&query,
					  "SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = %s",
					  PQescapeLiteral(conn, slot_name, strlen(slot_name)));

	res = PQexec(conn, query.data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		die(_("Could not fetch existing slot information: %s"), PQerrorMessage(conn));

	/* Drop the existing slot when asked for it or error if it already exists. */
	if (PQntuples(res) > 0)
	{
		PQclear(res);
		resetPQExpBuffer(&query);

		if (!drop_slot_if_exists)
			die(_("Slot %s already exists, drop it or use --drop-slot-if-exists to drop it automatically.\n"),
				slot_name);

		print_msg(VERBOSITY_VERBOSE,
				  _("Droping existing slot %s ...\n"), slot_name);

		printfPQExpBuffer(&query,
						  "SELECT pg_catalog.pg_drop_replication_slot(%s)",
						  PQescapeLiteral(conn, slot_name, strlen(slot_name)));

		res = PQexec(conn, query.data);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			die(_("Could not drop existing slot %s: %s"), slot_name,
				PQerrorMessage(conn));
	}

	PQclear(res);
	resetPQExpBuffer(&query);

	/* And finally, create the slot. */
	appendPQExpBuffer(&query, "SELECT pg_create_logical_replication_slot(%s, '%s');",
					  PQescapeLiteral(conn, slot_name, strlen(slot_name)),
					  "pglogical_output");

	res = PQexec(conn, query.data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		die(_("Could not create replication slot, status %s: %s\n"),
			 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
	}

	PQclear(res);
	termPQExpBuffer(&query);

	return slot_name;
}

/*
 * Read replication info about remote connection
 *
 * TODO: unify with pglogical_remote_node_info in pglogical_rpc
 */
static RemoteInfo *
get_remote_info(PGconn* conn)
{
	RemoteInfo		    *ri = (RemoteInfo *)pg_malloc0(sizeof(RemoteInfo));
	PGresult	   *res;

	if (!extension_exists(conn, "pglogical"))
		die(_("The remote node is not configured as a pglogical provider.\n"));

	res = PQexec(conn, "SELECT node_id, node_name, sysid, dbname, replication_sets FROM pglogical.pglogical_node_info()");
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		die(_("could not fetch remote node info: %s\n"), PQerrorMessage(conn));

	/* No nodes found? */
	if (PQntuples(res) == 0)
		die(_("The remote database is not configured as a pglogical node.\n"));

	if (PQntuples(res) > 1)
		die(_("The remote database has multiple nodes configured. That is not supported with current version of pglogical.\n"));

#define atooid(x)  ((Oid) strtoul((x), NULL, 10))

	ri->nodeid = atooid(PQgetvalue(res, 0, 0));
	ri->node_name = pstrdup(PQgetvalue(res, 0, 1));
	ri->sysid = pstrdup(PQgetvalue(res, 0, 2));
	ri->dbname = pstrdup(PQgetvalue(res, 0, 3));
	ri->replication_sets = pstrdup(PQgetvalue(res, 0, 4));

	PQclear(res);

	return ri;
}

/*
 * Check if extension exists.
 */
static bool
extension_exists(PGconn *conn, const char *extname)
{
	PQExpBuffer		query = createPQExpBuffer();
	PGresult	   *res;
	bool			ret;

	printfPQExpBuffer(query, "SELECT 1 FROM pg_catalog.pg_extension WHERE extname = %s;",
					  PQescapeLiteral(conn, extname, strlen(extname)));
	res = PQexec(conn, query->data);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		die(_("Could not read extension info: %s\n"), PQerrorMessage(conn));
	}

	ret = PQntuples(res) == 1;

	PQclear(res);
	destroyPQExpBuffer(query);

	return ret;
}

/*
 * Create extension.
 */
static void
install_extension(PGconn *conn, const char *extname)
{
	PQExpBuffer		query = createPQExpBuffer();
	PGresult	   *res;

	printfPQExpBuffer(query, "CREATE EXTENSION IF NOT EXISTS %s;",
					  PQescapeIdentifier(conn, extname, strlen(extname)));
	res = PQexec(conn, query->data);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		PQclear(res);
		die(_("Could not install %s extension: %s\n"), extname, PQerrorMessage(conn));
	}

	PQclear(res);
	destroyPQExpBuffer(query);
}

/*
 * Clean all the data that was copied from remote node but we don't
 * want it here (currently shared security labels and replication identifiers).
 */
static void
remove_unwanted_data(PGconn *conn)
{
	PGresult		   *res;

	/*
	 * Remove replication identifiers (9.4 will get them removed by dropping
	 * the extension later as we emulate them there).
	 */
	if (PQserverVersion(conn) >= 90500)
	{
		res = PQexec(conn, "SELECT pg_replication_origin_drop(external_id) FROM pg_replication_origin_status;");
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			PQclear(res);
			die(_("Could not remove existing replication origins: %s\n"), PQerrorMessage(conn));
		}
		PQclear(res);
	}

	res = PQexec(conn, "DROP EXTENSION pglogical CASCADE;");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		die(_("Could not clean the pglogical extension, status %s: %s\n"),
			 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
	}
	PQclear(res);
}

/*
 * Initialize new remote identifier to specific position.
 */
static void
initialize_replication_origin(PGconn *conn, char *origin_name, char *remote_lsn)
{
	PGresult   *res;
	PQExpBuffer query = createPQExpBuffer();

	if (PQserverVersion(conn) >= 90500)
	{
		printfPQExpBuffer(query, "SELECT pg_replication_origin_create(%s)",
						  PQescapeLiteral(conn, origin_name, strlen(origin_name)));

		res = PQexec(conn, query->data);

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			die(_("Could not create replication origin \"%s\": status %s: %s\n"),
				query->data,
				PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
		}
		PQclear(res);

		if (remote_lsn)
		{
			printfPQExpBuffer(query, "SELECT pg_replication_origin_advance(%s, '%s')",
							  PQescapeLiteral(conn, origin_name, strlen(origin_name)),
							  remote_lsn);

			res = PQexec(conn, query->data);

			if (PQresultStatus(res) != PGRES_TUPLES_OK)
			{
				die(_("Could not advance replication origin \"%s\": status %s: %s\n"),
					query->data,
					PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
			}
			PQclear(res);
		}
	}
	else
	{
		printfPQExpBuffer(query, "INSERT INTO pglogical_origin.replication_origin (roident, roname, roremote_lsn) SELECT COALESCE(MAX(roident::int), 0) + 1, %s, %s FROM pglogical_origin.replication_origin",
						  PQescapeLiteral(conn, origin_name, strlen(origin_name)),
						  remote_lsn ? PQescapeLiteral(conn, remote_lsn, strlen(remote_lsn)) : "0");

		res = PQexec(conn, query->data);

		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			die(_("Could not create replication origin \"%s\": status %s: %s\n"),
				query->data,
				PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
		}
		PQclear(res);
	}

	destroyPQExpBuffer(query);
}


/*
 * Create remote restore point which will be used to get into synchronized
 * state through physical replay.
 */
static char *
create_restore_point(PGconn *conn, char *restore_point_name)
{
	PQExpBuffer  query = createPQExpBuffer();
	PGresult	*res;
	char		*remote_lsn = NULL;

	printfPQExpBuffer(query, "SELECT pg_create_restore_point('%s')", restore_point_name);
	res = PQexec(conn, query->data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		die(_("Could not create restore point, status %s: %s\n"),
			 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
	}
	remote_lsn = pstrdup(PQgetvalue(res, 0, 0));

	PQclear(res);
	destroyPQExpBuffer(query);

	return remote_lsn;
}

static void
pglogical_subscribe(PGconn *conn, char *subscriber_name, char *subscriber_dsn,
					char *provider_dsn, char *replication_sets,
					int apply_delay, bool force_text_transfer)
{
	PQExpBufferData		query;
	PQExpBufferData		repsets;
	PGresult		   *res;

	initPQExpBuffer(&query);
	printfPQExpBuffer(&query,
					  "SELECT pglogical.create_node(node_name := %s, dsn := %s);",
					  PQescapeLiteral(conn, subscriber_name, strlen(subscriber_name)),
					  PQescapeLiteral(conn, subscriber_dsn, strlen(subscriber_dsn)));

	res = PQexec(conn, query.data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		die(_("Could not create local node, status %s: %s\n"),
			 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
	}
	PQclear(res);

	resetPQExpBuffer(&query);
	initPQExpBuffer(&repsets);

	printfPQExpBuffer(&repsets, "{%s}", replication_sets);
	printfPQExpBuffer(&query,
					  "SELECT pglogical.create_subscription("
					  "subscription_name := %s, provider_dsn := %s, "
					  "replication_sets := %s, "
					  "apply_delay := '%d seconds'::interval, "
					  "synchronize_structure := false, "
					  "synchronize_data := false, "
					  "force_text_transfer := '%s');",
					  PQescapeLiteral(conn, subscriber_name, strlen(subscriber_name)),
					  PQescapeLiteral(conn, provider_dsn, strlen(provider_dsn)),
					  PQescapeLiteral(conn, repsets.data, repsets.len),
					  apply_delay, (force_text_transfer ? "t" : "f"));

	res = PQexec(conn, query.data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		die(_("Could not create subscription, status %s: %s\n"),
			 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
	}
	PQclear(res);

	/* TODO */
	res = PQexec(conn, "UPDATE pglogical.local_sync_status SET sync_status = 'r'");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		die(_("Could not update subscription, status %s: %s\n"),
			 PQresStatus(PQresultStatus(res)), PQresultErrorMessage(res));
	}

	PQclear(res);

	termPQExpBuffer(&repsets);
	termPQExpBuffer(&query);
}


/*
 * Validates input of the replication sets and returns normalized data.
 */
static char *
validate_replication_set_input(char *replication_sets)
{
	char	   *name;
	PQExpBuffer	retbuf = createPQExpBuffer();
	char	   *ret;
	bool		first = true;

	if (!replication_sets)
		return NULL;

	name = strtok(replication_sets, " ,");
	while (name != NULL)
	{
		const char *cp;

		if (strlen(name) == 0)
			die(_("Replication set name \"%s\" is too short\n"), name);

		if (strlen(name) > NAMEDATALEN)
			die(_("Replication set name \"%s\" is too long\n"), name);

		for (cp = name; *cp; cp++)
		{
			if (!((*cp >= 'a' && *cp <= 'z')
				  || (*cp >= '0' && *cp <= '9')
				  || (*cp == '_')
				  || (*cp == '-')))
			{
				die(_("Replication set name \"%s\" contains invalid character\n"),
					name);
			}
		}

		if (first)
			first = false;
		else
			appendPQExpBufferStr(retbuf, ", ");
		appendPQExpBufferStr(retbuf, name);

		name = strtok(NULL, " ,");
	}

	ret = pg_strdup(retbuf->data);
	destroyPQExpBuffer(retbuf);

	return ret;
}

static char *
get_connstr_dbname(char *connstr)
{
	PQconninfoOption *conn_opts = NULL;
	PQconninfoOption *conn_opt;
	char	   *err_msg = NULL;
	char	   *ret = NULL;

	conn_opts = PQconninfoParse(connstr, &err_msg);
	if (conn_opts == NULL)
	{
		die(_("Invalid connection string: %s\n"), err_msg);
	}

	for (conn_opt = conn_opts; conn_opt->keyword != NULL; conn_opt++)
	{
		if (strcmp(conn_opt->keyword, "dbname") == 0)
		{
			ret = pstrdup(conn_opt->val);
			break;
		}
	}

	PQconninfoFree(conn_opts);

	return ret;
}


/*
 * Build connection string from individual parameter.
 *
 * dbname can be specified in connstr parameter
 */
static char *
get_connstr(char *connstr, char *dbname)
{
	char		*ret;
	int			argcount = 4;	/* dbname, host, user, port */
	int			i;
	const char **keywords;
	const char **values;
	PQconninfoOption *conn_opts = NULL;
	PQconninfoOption *conn_opt;
	char	   *err_msg = NULL;

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
		conn_opts = PQconninfoParse(connstr, &err_msg);
		if (conn_opts == NULL)
		{
			die(_("Invalid connection string: %s\n"), err_msg);
		}

		for (conn_opt = conn_opts; conn_opt->keyword != NULL; conn_opt++)
		{
			if (conn_opt->val != NULL && conn_opt->val[0] != '\0')
				argcount++;
		}

		keywords = pg_malloc0((argcount + 1) * sizeof(*keywords));
		values = pg_malloc0((argcount + 1) * sizeof(*values));

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
		keywords = pg_malloc0((argcount + 1) * sizeof(*keywords));
		values = pg_malloc0((argcount + 1) * sizeof(*values));

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

	ret = PQconninfoParamsToConnstr(keywords, values);

	/* Connection ok! */
	pg_free(values);
	pg_free(keywords);
	if (conn_opts)
		PQconninfoFree(conn_opts);

	return ret;
}


/*
 * Reads the pg_control file of the existing data dir.
 */
static char *
read_sysid(const char *data_dir)
{
	ControlFileData ControlFile;
	int			fd;
	char		ControlFilePath[MAXPGPATH];
	char	   *res = (char *) pg_malloc0(33);

	snprintf(ControlFilePath, MAXPGPATH, "%s/global/pg_control", data_dir);

	if ((fd = open(ControlFilePath, O_RDONLY | PG_BINARY, 0)) == -1)
		die(_("%s: could not open file \"%s\" for reading: %s\n"),
			progname, ControlFilePath, strerror(errno));

	if (read(fd, &ControlFile, sizeof(ControlFileData)) != sizeof(ControlFileData))
		die(_("%s: could not read file \"%s\": %s\n"),
			progname, ControlFilePath, strerror(errno));

	close(fd);

	snprintf(res, 33, UINT64_FORMAT, ControlFile.system_identifier);
	return res;
}

/*
 * Write contents of recovery.conf or postgresql.auto.conf
 */
static void
WriteRecoveryConf(PQExpBuffer contents)
{
	char		filename[MAXPGPATH];
	FILE	   *cf;

#if PG_VERSION_NUM >= 120000
	sprintf(filename, "%s/postgresql.auto.conf", data_dir);

	cf = fopen(filename, "a");
#else
	sprintf(filename, "%s/recovery.conf", data_dir);

	cf = fopen(filename, "w");
#endif
	if (cf == NULL)
	{
		die(_("%s: could not create file \"%s\": %s\n"), progname, filename, strerror(errno));
	}

	if (fwrite(contents->data, contents->len, 1, cf) != 1)
	{
		die(_("%s: could not write to file \"%s\": %s\n"),
				progname, filename, strerror(errno));
	}

	fclose(cf);

#if PG_VERSION_NUM >= 120000
	{
		sprintf(filename, "%s/standby.signal", data_dir);
		cf = fopen(filename, "w");
		if (cf == NULL)
		{
			die(_("%s: could not create file \"%s\": %s\n"), progname, filename, strerror(errno));
		}

		fclose(cf);
	}
#endif
}

/*
 * Copy file to data
 */
static void
CopyConfFile(char *fromfile, char *tofile, bool append)
{
	char		filename[MAXPGPATH];

	sprintf(filename, "%s/%s", data_dir, tofile);

	print_msg(VERBOSITY_DEBUG, _("Copying \"%s\" to \"%s\".\n"),
			  fromfile, filename);
	copy_file(fromfile, filename, append);
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

	ret = pg_strdup(retbuf->data);
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


/*
 * Find the pgport and try a connection
 */
static void
wait_postmaster_connection(const char *connstr)
{
	PGPing		res;
	long		pmpid = 0;

	print_msg(VERBOSITY_VERBOSE, "Waiting for PostgreSQL to accept connections ...");

	/* First wait for Postmaster to come up. */
	for (;;)
	{
		if ((pmpid = get_pgpid()) != 0 &&
			postmaster_is_alive((pid_t) pmpid))
			break;

		pg_usleep(1000000);		/* 1 sec */
		print_msg(VERBOSITY_VERBOSE, ".");
	}

	/* Now wait for Postmaster to either accept connections or die. */
	for (;;)
	{
		res = PQping(connstr);
		if (res == PQPING_OK)
			break;
		else if (res == PQPING_NO_ATTEMPT)
			break;

		/*
		 * Check if the process is still alive. This covers cases where the
		 * postmaster successfully created the pidfile but then crashed without
		 * removing it.
		 */
		if (!postmaster_is_alive((pid_t) pmpid))
			break;

		/* No response; wait */
		pg_usleep(1000000);		/* 1 sec */
		print_msg(VERBOSITY_VERBOSE, ".");
	}

	print_msg(VERBOSITY_VERBOSE, "\n");
}


/*
 * Wait for PostgreSQL to leave recovery/standby mode
 */
static void
wait_primary_connection(const char *connstr)
{
	bool		ispri = false;
	PGconn		*conn = NULL;
	PGresult	*res;

	wait_postmaster_connection(connstr);

	print_msg(VERBOSITY_VERBOSE, "Waiting for PostgreSQL to become primary...");

	while (!ispri)
	{
		if (!conn || PQstatus(conn) != CONNECTION_OK)
		{
			if (conn)
				PQfinish(conn);
			wait_postmaster_connection(connstr);
			conn = connectdb(connstr);
		}

		res = PQexec(conn, "SELECT pg_is_in_recovery()");
		if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1 && *PQgetvalue(res, 0, 0) == 'f')
			ispri = true;
		else
		{
			pg_usleep(1000000);		/* 1 sec */
			print_msg(VERBOSITY_VERBOSE, ".");
		}

		PQclear(res);
	}

	PQfinish(conn);
	print_msg(VERBOSITY_VERBOSE, "\n");
}

/*
 * Wait for postmaster to die
 */
static void
wait_postmaster_shutdown(void)
{
	long pid;

	print_msg(VERBOSITY_VERBOSE, "Waiting for PostgreSQL to shutdown ...");

	for (;;)
	{
		if ((pid = get_pgpid()) != 0)
		{
			pg_usleep(1000000);		/* 1 sec */
			print_msg(VERBOSITY_NORMAL, ".");
		}
		else
			break;
	}

	print_msg(VERBOSITY_VERBOSE, "\n");
}

static bool
file_exists(const char *path)
{
	struct stat statbuf;

	if (stat(path, &statbuf) != 0)
		return false;

	return true;
}

static bool
is_pg_dir(const char *path)
{
	struct stat statbuf;
	char		version_file[MAXPGPATH];

	if (stat(path, &statbuf) != 0)
		return false;

	snprintf(version_file, MAXPGPATH, "%s/PG_VERSION", data_dir);
	if (stat(version_file, &statbuf) != 0 && errno == ENOENT)
	{
		return false;
	}

	return true;
}

/*
 * copy one file
 */
static void
copy_file(char *fromfile, char *tofile, bool append)
{
	char	   *buffer;
	int			srcfd;
	int			dstfd;
	int			nbytes;
	off_t		offset;

#define COPY_BUF_SIZE (8 * BLCKSZ)

	buffer = malloc(COPY_BUF_SIZE);

	/*
	 * Open the files
	 */
	srcfd = open(fromfile, O_RDONLY | PG_BINARY, 0);
	if (srcfd < 0)
		die(_("could not open file \"%s\""), fromfile);

	dstfd = open(tofile, O_RDWR | O_CREAT | (append ? O_APPEND : O_TRUNC) | PG_BINARY,
							  S_IRUSR | S_IWUSR);
	if (dstfd < 0)
		die(_("could not create file \"%s\""), tofile);

	/*
	 * Do the data copying.
	 */
	for (offset = 0;; offset += nbytes)
	{
		nbytes = read(srcfd, buffer, COPY_BUF_SIZE);
		if (nbytes < 0)
			die(_("could not read file \"%s\""), fromfile);
		if (nbytes == 0)
			break;
		errno = 0;
		if ((int) write(dstfd, buffer, nbytes) != nbytes)
		{
			/* if write didn't set errno, assume problem is no disk space */
			if (errno == 0)
				errno = ENOSPC;
			die(_("could not write to file \"%s\""), tofile);
		}
	}

	if (close(dstfd))
		die(_("could not close file \"%s\""), tofile);

	/* we don't care about errors here */
	close(srcfd);

	free(buffer);
}


static char *
find_other_exec_or_die(const char *argv0, const char *target)
{
	int			ret;
	char	   *found_path;
	uint32		bin_version;

	found_path = pg_malloc(MAXPGPATH);

	ret = find_other_exec_version(argv0, target, &bin_version, found_path);

	if (ret < 0)
	{
		char		full_path[MAXPGPATH];

		if (find_my_exec(argv0, full_path) < 0)
			strlcpy(full_path, progname, sizeof(full_path));

		if (ret == -1)
			die(_("The program \"%s\" is needed by %s "
						   "but was not found in the\n"
						   "same directory as \"%s\".\n"
						   "Check your installation.\n"),
						 target, progname, full_path);
		else
			die(_("The program \"%s\" was found by \"%s\"\n"
						   "but was not the same version as %s.\n"
						   "Check your installation.\n"),
						 target, full_path, progname);
	}
	else
	{
		char		full_path[MAXPGPATH];

		if (find_my_exec(argv0, full_path) < 0)
			strlcpy(full_path, progname, sizeof(full_path));

		if (bin_version / 100 != PG_VERSION_NUM / 100)
			die(_("The program \"%s\" was found by \"%s\"\n"
						   "but was not the same version as %s.\n"
						   "Check your installation.\n"),
						 target, full_path, progname);

	}

	return found_path;
}

static bool
postmaster_is_alive(pid_t pid)
{
	/*
	 * Test to see if the process is still there.  Note that we do not
	 * consider an EPERM failure to mean that the process is still there;
	 * EPERM must mean that the given PID belongs to some other userid, and
	 * considering the permissions on $PGDATA, that means it's not the
	 * postmaster we are after.
	 *
	 * Don't believe that our own PID or parent shell's PID is the postmaster,
	 * either.  (Windows hasn't got getppid(), though.)
	 */
	if (pid == getpid())
		return false;
#ifndef WIN32
	if (pid == getppid())
		return false;
#endif
	if (kill(pid, 0) == 0)
		return true;
	return false;
}

static long
get_pgpid(void)
{
	FILE	   *pidf;
	long		pid;

	pidf = fopen(pid_file, "r");
	if (pidf == NULL)
	{
		return 0;
	}
	if (fscanf(pidf, "%ld", &pid) != 1)
	{
		return 0;
	}
	fclose(pidf);
	return pid;
}

static char **
get_database_list(char *databases, int *n_databases)
{
	char *c;
	char **result;
	int num = 1;
	for (c = databases; *c; c++ )
		if (*c == ',')
			num++;
	*n_databases = num;
	result = palloc(num * sizeof(char *));
	num = 0;
	/* clone the argument so we don't destroy it with strtok*/
	databases = pstrdup(databases);
	c = strtok(databases, ",");
	while (c != NULL)
	{
		result[num] = pstrdup(c);
		num++;
		c = strtok(NULL,",");
	}
	pfree(databases);
	return result;
}

static char *
generate_restore_point_name(void)
{
	char *rpn = malloc(NAMEDATALEN);
	snprintf(rpn, NAMEDATALEN-1, "pglogical_create_subscriber_%lx", random());
	return rpn;
}
