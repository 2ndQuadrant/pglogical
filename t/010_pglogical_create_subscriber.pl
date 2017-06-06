use strict;
use warnings;
use 5.10.0;
use Cwd;
use Config;
use TestLib;
use Test::More;
use Time::HiRes qw(gettimeofday tv_interval);
# Local
use PostgresPGLNode;
use PGLDB;

my $PGPORT=$ENV{'PGPORT'};
my $PROVIDER_PORT=5431;
my $PROVIDER_DSN = "postgresql://super\@localhost:$PROVIDER_PORT/postgres";
my $SUBSCRIBER_DSN = "postgresql://super\@localhost:$PGPORT/postgres";

program_help_ok('pglogical_create_subscriber');

program_options_handling_ok('pglogical_create_subscriber');

my $ts;

my $pgldb = 'pgltest';
my $providername = 'test_provider';
my $subscribername = 'test_subscriber';
my $subscriptionname = 'test_physical';

my $node_pub = get_new_pgl_node('node_pub');
$node_pub->init;
$node_pub->start;
$node_pub->safe_psql('postgres', "CREATE DATABASE $pgldb");

# insert some pre-seed data
$node_pub->safe_psql($pgldb, q[
CREATE SEQUENCE some_local_seq;
CREATE TABLE some_local_tbl(id serial primary key, key text unique not null, data text);
INSERT INTO some_local_tbl(key, data) VALUES('key1', 'data1');
INSERT INTO some_local_tbl(key, data) VALUES('key2', NULL);
INSERT INTO some_local_tbl(key, data) VALUES('key3', 'data3');
CREATE TABLE some_local_tbl1(id serial, key text unique not null, data text);
INSERT INTO some_local_tbl1(key, data) VALUES('key1', 'data1');
INSERT INTO some_local_tbl1(key, data) VALUES('key2', NULL);
INSERT INTO some_local_tbl1(key, data) VALUES('key3', 'data3');
CREATE TABLE some_local_tbl2(id serial, key text, data text);
INSERT INTO some_local_tbl2(key, data) VALUES('key1', 'data1');
INSERT INTO some_local_tbl2(key, data) VALUES('key2', NULL);
INSERT INTO some_local_tbl2(key, data) VALUES('key3', 'data3');
CREATE TABLE some_local_tbl3(id integer, key text, data text);
INSERT INTO some_local_tbl3(key, data) VALUES('key1', 'data1');
INSERT INTO some_local_tbl3(key, data) VALUES('key2', NULL);
INSERT INTO some_local_tbl3(key, data) VALUES('key3', 'data3');
]);

my $pub = PGLDB->new($node_pub, $pgldb, $providername);
$pub->init;

my $node_sub = get_new_pgl_node('node_sub');
my $subscriptions = $node_sub->init_physical_clone(
	publisher => $pub,
	subscriber_name => $subscribername
);

is(scalar(@$subscriptions), 1, '1 subscription created');
my $sub = $$subscriptions[0];

is($sub->name, $subscribername, 'subscriber name matches');
is($sub->dbname, $pgldb, 'dbname matches');
is($sub->safe_psql(q[
	SELECT origin.node_name, target.node_name, sub_name
	FROM pglogical.subscription
		INNER JOIN pglogical.node origin ON (origin.node_id = sub_origin)
		INNER JOIN pglogical.node target ON (target.node_id = sub_target)
	]),
   "$providername|$subscribername|$subscribername",
   'node and subscription names match');

is($sub->safe_psql(q[
	select table_name, table_type from information_schema.tables where table_schema = 'public' order by table_name;
	]),
	q[some_local_tbl|BASE TABLE
some_local_tbl1|BASE TABLE
some_local_tbl2|BASE TABLE
some_local_tbl3|BASE TABLE],
	'expected tables exist on subscriber');

is($sub->safe_psql(q[
	SELECT * from some_local_tbl order by key;
	]),
	q[1|key1|data1
2|key2|
3|key3|data3],
	'expected data in table some_local_tbl');

print "Creating public.basic_dml...";
$pub->replicate_ddl(q[
	CREATE TABLE public.basic_dml (
		id serial primary key,
		other integer
	)]
);
say " created";

print "Waiting to see public.basic_dml on subscriber...";
print "(my dsn is " . $sub->connstr . ")";
$ts = [gettimeofday()];
$sub->poll_query_until(
	q[SELECT EXISTS (SELECT 1 FROM information_schema.tables where table_schema = 'public' and table_name = 'basic_dml')])
	or BAIL_OUT("basic_dml didn't replicate");
say " seen; took " . tv_interval ( $ts, [gettimeofday()] ) . " seconds";

is($sub->safe_psql(q[SELECT 1 FROM information_schema.tables where table_schema = 'public' and table_name = 'basic_dml']),
	'1', 'table schema basic_dml got replicated');

$pub->safe_psql(q[INSERT INTO basic_dml(other) VALUES (666)]);
$pub->replication_set_add_table('default', 'basic_dml', 1);
$pub->safe_psql(q[INSERT INTO basic_dml(other) VALUES (42)]);

print "Waiting to see row other=42 in public.basic_dml on subscriber...";
$ts = [gettimeofday()];
$sub->node->poll_query_until($sub->dbname, q[SELECT exists(SELECT 1 FROM basic_dml WHERE other = 42)])
	or BAIL_OUT("value in basic_dml didn't replicate");
say " seen; took " . tv_interval ( $ts, [gettimeofday()] ) . " seconds";

is($sub->safe_psql(q[
	SELECT id, other from basic_dml order by id;
	]),
	q[1|666
2|42],
	'expected data in table basic_dml');

done_testing();
