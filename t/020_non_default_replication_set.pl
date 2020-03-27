use strict;
use warnings;
use Cwd;
use Config;
use TestLib;
use Test::More tests => 1;

my $PGPORT=65432; #subscriber's port
my $PROVIDER_PORT=65431;
my $PROVIDER_DSN = "postgresql://super\@localhost:$PROVIDER_PORT/postgres";
my $SUBSCRIBER_DSN = "postgresql://super\@localhost:$PGPORT/postgres";

system_or_bail 'rm', '-rf', '/tmp/tmp_020_pdatadir';
system_or_bail 'rm', '-rf', '/tmp/tmp_020_sdatadir';

#provider's and subscriber's datadir
system_or_bail 'initdb', '-A trust', '-D', '/tmp/tmp_020_pdatadir';
system_or_bail 'initdb', '-A trust', '-D', '/tmp/tmp_020_sdatadir';

system_or_bail 'pwd';

system_or_bail 'cp', 'regress-pg_hba.conf', '/tmp/tmp_020_pdatadir/pg_hba.conf';
system_or_bail 'cp', 'regress-pg_hba.conf', '/tmp/tmp_020_sdatadir/pg_hba.conf';

my $pg_version = `pg_config --version| sed 's/[^0-9\.]//g' | awk -F . '{ print \$1\$2 }'`;

if ($pg_version >= 95) {
    `cat t/perl-95-postgresql.conf>>/tmp/tmp_020_pdatadir/postgresql.conf`;
    `cat t/perl-95-postgresql.conf>>/tmp/tmp_020_sdatadir/postgresql.conf`;
} else {
    `cat t/perl-94-postgresql.conf>>/tmp/tmp_020_pdatadir/postgresql.conf`;
    `cat t/perl-94-postgresql.conf>>/tmp/tmp_020_sdatadir/postgresql.conf`;
}

system("postgres -p $PROVIDER_PORT -D /tmp/tmp_020_pdatadir -c logging_collector=on &");
system("postgres -p $PGPORT -D /tmp/tmp_020_sdatadir -c logging_collector=on &");

#allow Postgres servers to startup
system_or_bail 'sleep', '17';

system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "CREATE USER super SUPERUSER";
system_or_bail 'psql', '-p', "$PGPORT", '-d', "postgres", '-c', "CREATE USER super SUPERUSER";

# Required for PostgreSQL 9.4 run
#system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "CREATE EXTENSION IF NOT EXISTS pglogical_origin";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "CREATE EXTENSION IF NOT EXISTS pglogical VERSION '1.0.0'";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "ALTER EXTENSION pglogical UPDATE";

# Required for PostgreSQL 9.4 run
#system_or_bail 'psql', '-p', "$PGPORT", '-d', "postgres", '-c', "CREATE EXTENSION IF NOT EXISTS pglogical_origin";
system_or_bail 'psql', '-p', "$PGPORT", '-d', "postgres", '-c', "CREATE EXTENSION IF NOT EXISTS pglogical";

system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "SELECT * FROM pglogical.create_node(node_name := 'test_provider', dsn := 'dbname=postgres user=super')";

system_or_bail 'psql', '-p', "$PGPORT", '-d', "postgres", '-c', "SELECT * FROM pglogical.create_node(node_name := 'test_subscriber', dsn := '$SUBSCRIBER_DSN')";

system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "SELECT * FROM pglogical.create_replication_set('delay')";

system_or_bail 'psql', '-p', "$PGPORT", '-d', "postgres", '-c', "SELECT * FROM pglogical.create_subscription(
    subscription_name := 'test_subscription_delay',
    provider_dsn := '$PROVIDER_DSN',
        replication_sets := '{delay}',
        forward_origins := '{}',
        synchronize_structure := false,
        synchronize_data := false
)";

system_or_bail 'psql', '-p', "$PGPORT", '-d', "postgres", '-c', "DO \$\$
BEGIN
        FOR i IN 1..100 LOOP
                IF EXISTS (SELECT 1 FROM pglogical.show_subscription_status() WHERE status = 'replicating') THEN
                        RETURN;
                END IF;
                PERFORM pg_sleep(0.1);
        END LOOP;
END;
\$\$";

system_or_bail 'psql', '-p', "$PGPORT", '-d', "postgres", '-c', "SELECT subscription_name, status, provider_node, replication_sets, forward_origins FROM pglogical.show_subscription_status()";

system_or_bail 'psql', '-p', "$PGPORT", '-d', "postgres", '-c', "DO \$\$
BEGIN
    FOR i IN 1..300 LOOP
        IF EXISTS (SELECT 1 FROM pglogical.local_sync_status WHERE sync_status = 'r') THEN
            EXIT;
        END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
END;\$\$";

system_or_bail 'psql', '-p', "$PGPORT", '-d', "postgres", '-c', "SELECT sync_kind, sync_subid, sync_nspname, sync_relname, sync_status FROM pglogical.local_sync_status ORDER BY 2,3,4";

system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "CREATE OR REPLACE FUNCTION public.pg_xlog_wait_remote_apply(i_pos pg_lsn, i_pid integer) RETURNS VOID
AS \$FUNC\$
BEGIN
    WHILE EXISTS(SELECT true FROM pg_stat_get_wal_senders() s WHERE s.replay_location < i_pos AND (i_pid = 0 OR s.pid = i_pid)) LOOP
                PERFORM pg_sleep(0.01);
        END LOOP;
END;\$FUNC\$ LANGUAGE plpgsql";

system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "SELECT pglogical.replicate_ddl_command(\$\$
    CREATE TABLE public.basic_dml1 (
        id serial primary key,
        other integer,
        data text,
        something interval
    );
\$\$)";
system_or_bail 'psql', '-p', "$PGPORT", '-d', "postgres", '-c', "select * from pglogical.show_subscription_status('test_subscription_delay');";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "SELECT * FROM pglogical.replication_set_add_table('delay', 'basic_dml1', true) ";

#At this point subscriber sync starts crashing (`sync worker`) and recovering
# check logs at this point at:
# /tmp/tmp_020_pdatadir and /tmp/tmp_020_sdatadir
# As per Petr this is expected behavior.
# But since the table does not exist on subscriber, the sync worker dies when trying
# to accessing it. It even logs why it dies on the line above.
system_or_bail 'sleep', '10';
command_fails([ 'psql', '-p', "$PGPORT", '-d', "postgres", '-c', "SELECT * FROM basic_dml1" ], 'replication check');
#cleanup
system("pg_ctl stop -D /tmp/tmp_020_sdatadir -m immediate &");
system("pg_ctl stop -D /tmp/tmp_020_pdatadir -m immediate &");

