use strict;
use warnings;
use Cwd;
use Config;
use TestLib;
use Test::More tests => 11;

my $PGPORT=$ENV{'PGPORT'};
my $PROVIDER_PORT=5431;
my $PROVIDER_DSN = "postgresql://super\@localhost:$PROVIDER_PORT/postgres";
my $SUBSCRIBER_DSN = "postgresql://super\@localhost:$PGPORT/postgres";

program_help_ok('pglogical_create_subscriber');

program_options_handling_ok('pglogical_create_subscriber');

system_or_bail 'rm', '-rf', '/tmp/tmp_datadir';
system_or_bail 'rm', '-rf', '/tmp/tmp_backupdir';

system_or_bail 'initdb', '-A trust', '-D', '/tmp/tmp_datadir';

system_or_bail 'pwd';

system_or_bail 'cp', 'regress-pg_hba.conf', '/tmp/tmp_datadir/pg_hba.conf';

my $pg_version = `pg_config --version| sed 's/[^0-9\.]//g' | awk -F . '{ print \$1\$2 }'`;

if ($pg_version >= 95) {
    `cat t/perl-95-postgresql.conf>>/tmp/tmp_datadir/postgresql.conf`;
} else {
    `cat t/perl-94-postgresql.conf>>/tmp/tmp_datadir/postgresql.conf`;
}

system("postgres -p $PROVIDER_PORT -D /tmp/tmp_datadir -c logging_collector=on &");

#allow Postgres server to startup
system_or_bail 'sleep', '17';

system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "CREATE USER super SUPERUSER";

# insert some pre-seed data
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "CREATE SEQUENCE some_local_seq";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "CREATE TABLE some_local_tbl(id serial primary key, key text unique not null, data text)";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "INSERT INTO some_local_tbl(key, data) VALUES('key1', 'data1')";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "INSERT INTO some_local_tbl(key, data) VALUES('key2', NULL)";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "INSERT INTO some_local_tbl(key, data) VALUES('key3', 'data3')";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "CREATE TABLE some_local_tbl1(id serial, key text unique not null, data text)";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "INSERT INTO some_local_tbl1(key, data) VALUES('key1', 'data1')";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "INSERT INTO some_local_tbl1(key, data) VALUES('key2', NULL)";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "INSERT INTO some_local_tbl1(key, data) VALUES('key3', 'data3')";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "CREATE TABLE some_local_tbl2(id serial, key text, data text)";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "INSERT INTO some_local_tbl2(key, data) VALUES('key1', 'data1')";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "INSERT INTO some_local_tbl2(key, data) VALUES('key2', NULL)";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "INSERT INTO some_local_tbl2(key, data) VALUES('key3', 'data3')";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "CREATE TABLE some_local_tbl3(id integer, key text, data text)";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "INSERT INTO some_local_tbl3(key, data) VALUES('key1', 'data1')";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "INSERT INTO some_local_tbl3(key, data) VALUES('key2', NULL)";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "INSERT INTO some_local_tbl3(key, data) VALUES('key3', 'data3')";
# Required for PostgreSQL 9.4 run
#system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "CREATE EXTENSION IF NOT EXISTS pglogical_origin";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "CREATE EXTENSION IF NOT EXISTS pglogical VERSION '1.0.0'";
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "ALTER EXTENSION pglogical UPDATE";

system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-c', "SELECT * FROM pglogical.create_node(node_name := 'test_provider', dsn := 'dbname=postgres user=super')";

command_ok([ 'pglogical_create_subscriber', '-D', '/tmp/tmp_backupdir', "--subscriber-name=test_subscriber", "--subscriber-dsn=$SUBSCRIBER_DSN", "--provider-dsn=$PROVIDER_DSN", '--drop-slot-if-exists', '-v', '--hba-conf=regress-pg_hba.conf', '--postgresql-conf=/tmp/tmp_datadir/postgresql.conf'], 'pglogical_create_subscriber check');

#test whether preseed data is there

command_ok([ 'psql', '-p', "$PGPORT", '-d', "postgres", '-c', "\\d" ], 'preseed check 1 ');
command_ok([ 'psql', '-p', "$PGPORT", '-d', "postgres", '-c', "SELECT * from some_local_tbl" ], 'preseed check 2 ');

#insert some new data
system_or_bail 'psql', '-p', "$PROVIDER_PORT", '-d', "postgres", '-f', 't/basic.sql';

#allow script to complete executing
system_or_bail 'sleep', '11';

#check whether it is replicated
command_ok([ 'psql', '-p', "$PGPORT", '-d', "postgres", '-c', "\\d" ], 'replication check 1 ');
command_ok([ 'psql', '-p', "$PGPORT", '-d', "postgres", '-c', "SELECT * from basic_dml" ], 'replication check 2 ');
command_ok([ 'psql', '-p', "$PGPORT", '-d', "postgres", '-c', "SELECT * from public.basic_dml" ], 'replication check 3 ');

#cleanup
system("pg_ctl stop -D /tmp/tmp_backupdir -m immediate &");
system("pg_ctl stop -D /tmp/tmp_datadir -m immediate &");

