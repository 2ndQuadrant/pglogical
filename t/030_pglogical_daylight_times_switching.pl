use strict;
use warnings;
use Cwd;
use Config;
use TestLib;
use Test::More;
#use Test::More tests => 6;
# This is a special test, that tries to modify system time 
# Not required to run in usual suite tests
TODO: {
todo_skip 'Whole test need rewriting using the new framework, with no sudo etc', 1;

my $PGPORT=65432; #subscriber's port
my $PROVIDER_PORT=65431;
my $PROVIDER_DSN = "postgresql://super\@localhost:$PROVIDER_PORT/postgres";
my $SUBSCRIBER_DSN = "postgresql://super\@localhost:$PGPORT/postgres";

#This test requires user to be a part of sudo group
#for time and timezone changes
#It is interactive now as it needs password to do sudo
system_or_bail 'rm', '-rf', '/tmp/tmp_030_pdatadir';
system_or_bail 'rm', '-rf', '/tmp/tmp_030_sdatadir';

#bail out if ntp not installed - as we need it to set time back.
system_or_bail 'sudo', 'sntp', '-s', '24.56.178.140';
my $timezone = `timedatectl |grep \'Timezone\'|cut -d ':' -f 2|cut -d ' ' -f 2|sed 's/ //g'`;

#change timezone to before daylight savings border.
command_ok([ 'timedatectl', 'set-timezone', 'America/Los_Angeles' ], 'pre-daylight savings time-zone check ');
command_ok([ 'timedatectl', 'set-time', "2016-11-05 06:40:00" ], 'pre-daylight savings time check');

#sleep for a short while after this date change
system_or_bail 'sleep', '10';

#provider's and subscriber's datadir
system_or_bail 'initdb', '-A trust', '-D', '/tmp/tmp_030_pdatadir';
system_or_bail 'initdb', '-A trust', '-D', '/tmp/tmp_030_sdatadir';

system_or_bail 'pwd';

system_or_bail 'cp', 'regress-pg_hba.conf', '/tmp/tmp_030_pdatadir/pg_hba.conf';
system_or_bail 'cp', 'regress-pg_hba.conf', '/tmp/tmp_030_sdatadir/pg_hba.conf';

my $pg_version = `pg_config --version| sed 's/[^0-9\.]//g' | awk -F . '{ print \$1\$2 }'`;

if ($pg_version >= 95) {
    `cat t/perl-95-postgresql.conf>>/tmp/tmp_030_pdatadir/postgresql.conf`;
    `cat t/perl-95-postgresql.conf>>/tmp/tmp_030_sdatadir/postgresql.conf`;
} else {
    `cat t/perl-94-postgresql.conf>>/tmp/tmp_030_pdatadir/postgresql.conf`;
    `cat t/perl-94-postgresql.conf>>/tmp/tmp_030_sdatadir/postgresql.conf`;
}

system("postgres -p $PROVIDER_PORT -D /tmp/tmp_030_pdatadir -c logging_collector=on &");
system("postgres -p $PGPORT -D /tmp/tmp_030_sdatadir -c logging_collector=on &");

#allow Postgres servers to startup
system_or_bail 'sleep', '17';

system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-c', "CREATE USER super SUPERUSER";
system_or_bail 'psql', '-p', "$PGPORT", '-c', "CREATE USER super SUPERUSER";

system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-c', "CREATE EXTENSION IF NOT EXISTS pglogical VERSION '1.0.0'";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-c', "ALTER EXTENSION pglogical UPDATE";
system_or_bail 'psql', '-p', "$PGPORT", '-c', "CREATE EXTENSION IF NOT EXISTS pglogical";

system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-c', "SELECT * FROM pglogical.create_node(node_name := 'test_provider', dsn := 'dbname=postgres user=super')";

system_or_bail 'psql', '-p', "$PGPORT", '-c', "SELECT * FROM pglogical.create_node(node_name := 'test_subscriber', dsn := '$SUBSCRIBER_DSN')";

system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-c', "SELECT * FROM pglogical.create_replication_set('delay')";

system_or_bail 'psql', '-p', "$PGPORT", '-c', "CREATE or REPLACE function int2interval (x integer) returns interval as
\$\$ select \$1*'1 sec'::interval \$\$
language sql";

# create default subscription
system_or_bail 'psql', '-p', "$PGPORT", '-c', "SELECT * FROM pglogical.create_subscription(
    subscription_name := 'test_subscription',
    provider_dsn := '$PROVIDER_DSN',
    forward_origins := '{}',
    synchronize_structure := false,
    synchronize_data := false
)";

# create delayed subscription too.
system_or_bail 'psql', '-p', "$PGPORT", '-c', "SELECT * FROM pglogical.create_subscription(
    subscription_name := 'test_subscription_delay',
    provider_dsn := '$PROVIDER_DSN',
    replication_sets := '{delay}',
    forward_origins := '{}',
    synchronize_structure := false,
    synchronize_data := false,
    apply_delay := int2interval(1) -- 1 seconds
)";

system_or_bail 'psql', '-p', "$PGPORT", '-c', "DO \$\$
BEGIN
        FOR i IN 1..100 LOOP
                IF EXISTS (SELECT 1 FROM pglogical.show_subscription_status() WHERE status = 'replicating' AND subscription_name = 'test_subscription_delay') THEN
                        RETURN;
                END IF;
                PERFORM pg_sleep(0.1);
        END LOOP;
END;
\$\$";

system_or_bail 'psql', '-p', "$PGPORT", '-c', "SELECT subscription_name, status, provider_node, replication_sets, forward_origins FROM pglogical.show_subscription_status()";

system_or_bail 'psql', '-p', "$PGPORT", '-c', "DO \$\$
BEGIN
    FOR i IN 1..300 LOOP
        IF EXISTS (SELECT 1 FROM pglogical.local_sync_status WHERE sync_status != 'r') THEN
            PERFORM pg_sleep(0.1);
        ELSE
            EXIT;
        END IF;
    END LOOP;
END;\$\$";

system_or_bail 'psql', '-p', "$PGPORT", '-c', "SELECT sync_kind, sync_subid, sync_nspname, sync_relname, sync_status FROM pglogical.local_sync_status ORDER BY 2,3,4";
#change timezone to after daylight savings border.
command_ok([ 'timedatectl', 'set-time', "2016-11-06 06:40:00" ], 'switching daylight savings time check');

# sleep for ~5 mins to allow both servers to recover
system_or_bail 'sleep', '300';
system_or_bail 'psql', '-p', "$PGPORT", '-c', "DO \$\$
BEGIN
        FOR i IN 1..100 LOOP
                IF EXISTS (SELECT 1 FROM pglogical.show_subscription_status() WHERE status = 'replicating' AND subscription_name = 'test_subscription_delay') THEN
                        RETURN;
                END IF;
                PERFORM pg_sleep(0.1);
        END LOOP;
END;
\$\$";

system_or_bail 'psql', '-p', "$PGPORT", '-c', "SELECT subscription_name, status, provider_node, replication_sets, forward_origins FROM pglogical.show_subscription_status()";

system_or_bail 'psql', '-p', "$PGPORT", '-c', "DO \$\$
BEGIN
    FOR i IN 1..300 LOOP
        IF EXISTS (SELECT 1 FROM pglogical.local_sync_status WHERE sync_status != 'r') THEN
            PERFORM pg_sleep(0.1);
        ELSE
            EXIT;
        END IF;
    END LOOP;
END;\$\$";

system_or_bail 'psql', '-p', "$PGPORT", '-c', "SELECT sync_kind, sync_subid, sync_nspname, sync_relname, sync_status FROM pglogical.local_sync_status ORDER BY 2,3,4";

system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-c', "CREATE OR REPLACE FUNCTION public.pg_xlog_wait_remote_apply(i_pos pg_lsn, i_pid integer) RETURNS VOID
AS \$FUNC\$
BEGIN
    WHILE EXISTS(SELECT true FROM pg_stat_get_wal_senders() s WHERE s.replay_location < i_pos AND (i_pid = 0 OR s.pid = i_pid)) LOOP
                PERFORM pg_sleep(0.01);
        END LOOP;
END;\$FUNC\$ LANGUAGE plpgsql";

system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-c', "CREATE TABLE public.timestamps (
        id text primary key,
        ts timestamptz
)";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-c', "SELECT pglogical.replicate_ddl_command(\$\$
    CREATE TABLE public.basic_dml1 (
        id serial primary key,
        other integer,
        data text,
        something interval
    );
\$\$)";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-c', "SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0)";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-c', "INSERT INTO timestamps VALUES ('ts1', CURRENT_TIMESTAMP)";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-c', "SELECT * FROM pglogical.replication_set_add_table('delay', 'basic_dml1', true) ";

system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-c', "SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0)";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-c', "INSERT INTO timestamps VALUES ('ts2', CURRENT_TIMESTAMP)";

system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-c', "INSERT INTO basic_dml1(other, data, something)
VALUES (5, 'foo', '1 minute'::interval),
       (4, 'bar', '12 weeks'::interval),
       (3, 'baz', '2 years 1 hour'::interval),
       (2, 'qux', '8 months 2 days'::interval),
       (1, NULL, NULL)";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-c', "SELECT pg_xlog_wait_remote_apply(pg_current_xlog_location(), 0)";

system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-c', "INSERT INTO timestamps VALUES ('ts3', CURRENT_TIMESTAMP)";

system_or_bail 'psql', '-p', "$PGPORT", '-c', "select * from pglogical.show_subscription_status('test_subscription_delay');";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-c', "SELECT round (EXTRACT(EPOCH FROM (SELECT ts from timestamps where id = 'ts2')) - EXTRACT(EPOCH FROM (SELECT ts from timestamps where id = 'ts1'))) :: integer as ddl_replicate_time";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-c', "SELECT round (EXTRACT(EPOCH FROM (SELECT ts from timestamps where id = 'ts3')) - EXTRACT(EPOCH FROM (SELECT ts from timestamps where id = 'ts2'))) :: integer as inserts_replicate_time";
command_ok([ 'psql', '-p', "$PROVIDER_PORT", '-c', "SELECT * FROM basic_dml1" ], 'provider data check');

system_or_bail 'psql', '-p', "$PGPORT", '-c', "select * from pglogical.show_subscription_status('test_subscription_delay');";
system_or_bail 'psql', '-p', "$PGPORT", '-c', "SELECT * FROM pglogical.show_subscription_table('test_subscription_delay', 'basic_dml1')";
#check the data of table at subscriber
command_ok([ 'psql', '-p', "$PGPORT", '-c', "SELECT * FROM basic_dml1" ], 'replication check');
system_or_bail 'psql', '-p', "$PGPORT", '-c', "SELECT pglogical.drop_subscription('test_subscription_delay')";

#cleanup
system("pg_ctl stop -D /tmp/tmp_030_sdatadir -m immediate &");
system("pg_ctl stop -D /tmp/tmp_030_pdatadir -m immediate &");
# change time and timezone back to normal:
command_ok([ 'sudo', 'sntp', '-s', '24.56.178.140' ], 'sync time with ntp server check');
system("timedatectl set-timezone $timezone");

}

done_testing();
