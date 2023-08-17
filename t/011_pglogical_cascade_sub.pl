# test truncate on cascade nodes with diferent replication sets. RT87453
use strict;
use warnings;
use PostgresNode;
use Cwd;
use Config;
use TestLib;
use Test::More tests => 7;

my $dbname = 'pgltest';
my $super_user="super";

# create the node_a
my $node_a = get_new_node('node_a');
$node_a->init();
$node_a->append_conf('postgresql.conf', qq[
wal_level = 'logical'
max_replication_slots = 12
max_wal_senders = 12
max_connections = 200
shared_preload_libraries = 'pglogical'
track_commit_timestamp = on
pglogical.synchronous_commit = true
]);
$node_a->dump_info;
$node_a->start;
$node_a->safe_psql('postgres', qq[CREATE DATABASE $dbname]);

# create the node_b
my $node_b = get_new_node('node_b');
$node_b->init();
$node_b->append_conf('postgresql.conf', qq[
shared_preload_libraries = 'pglogical'
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
track_commit_timestamp = on
fsync=off
pglogical.synchronous_commit = true
]);
$node_b->dump_info;
$node_b->start;
$node_b->safe_psql('postgres', qq[CREATE DATABASE $dbname]);

# create the node_c
my $node_c = get_new_node('node_c');
$node_c->init();
$node_c->append_conf('postgresql.conf', qq[
shared_preload_libraries = 'pglogical'
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
track_commit_timestamp = on
fsync=off
pglogical.synchronous_commit = true
log_line_prefix = '%t %p '
]);
$node_c->dump_info;
$node_c->start;
$node_c->safe_psql('postgres', qq[CREATE DATABASE $dbname]);



# Create pglogical node on all nodes

for my $n ($node_a, $node_b, $node_c){
    my $node_connstr = $n->connstr;
    my $node_name = $n->name;

    $n->safe_psql($dbname,
        qq[CREATE USER $super_user SUPERUSER;]);
    $n->safe_psql($dbname,
        q[CREATE EXTENSION IF NOT EXISTS pglogical;]);
    $n->safe_psql($dbname,
        qq[SELECT * FROM pglogical.create_node(node_name := '$node_name',
            dsn := '$node_connstr dbname=$dbname user=$super_user');]);
}

# create the different replication sets
$node_a->safe_psql($dbname,
        q[SELECT * FROM pglogical.create_replication_set('set_a');]);
$node_b->safe_psql($dbname,
        q[SELECT * FROM pglogical.create_replication_set('set_b');]);

# Create subscriptions and wait for them to be replicating
my $node_a_connstr = $node_a->connstr;
$node_b->safe_psql($dbname,
        qq[SELECT pglogical.create_subscription(
    subscription_name := 'sub_a_b',
    replication_sets := '{set_a}',
    provider_dsn := '$node_a_connstr dbname=$dbname user=$super_user'
);]);
$node_b->poll_query_until($dbname,
	q[SELECT EXISTS (
        SELECT 1 FROM pglogical.show_subscription_status()
            WHERE subscription_name = 'sub_a_b' AND status = 'replicating');])
	or BAIL_OUT('subscription failed to reach "replicating" state');
my $node_b_connstr = $node_b->connstr;
$node_c->safe_psql($dbname,
        qq[SELECT pglogical.create_subscription(
    subscription_name := 'sub_b_c',
    replication_sets := '{set_b}',
    provider_dsn := '$node_b_connstr dbname=$dbname user=$super_user'
);]);
$node_c->poll_query_until($dbname,
	q[SELECT EXISTS (
        SELECT 1 FROM pglogical.show_subscription_status()
            WHERE subscription_name = 'sub_b_c' AND status = 'replicating');])
	or BAIL_OUT('subscription failed to reach "replicating" state');

# Create the tables and add them to the correct repset
$node_a->safe_psql($dbname, q[create table a2b( x int primary key);]);
$node_a->safe_psql($dbname,
			"SELECT * FROM pglogical.replication_set_add_table('set_a', 'a2b', false);");

$node_b->safe_psql($dbname, q[create table a2b( x int primary key)]);
$node_b->safe_psql($dbname, q[create table b2c( x int primary key)]);
$node_b->safe_psql($dbname,
			"SELECT * FROM pglogical.replication_set_add_table('set_b', 'b2c', false);");

$node_c->safe_psql($dbname, q[create table a2b( x int primary key)]);
$node_c->safe_psql($dbname, q[create table b2c( x int)]);
# Add an INVALID index, then add the real PRIMARY KEY.
$node_c->safe_psql($dbname, q[insert into b2c values (1),(1)]);
$node_c->psql($dbname, q[create unique index concurrently on b2c(x)]);
$node_c->safe_psql($dbname, q[delete from b2c]);
$node_c->safe_psql($dbname, q[alter table b2c add primary key (x)]);

#insert some rows in the a2b table to check that it actually worked
$node_a->safe_psql($dbname, q[INSERT INTO a2b VALUES (1)]);
$node_b->safe_psql($dbname, q[INSERT INTO b2c VALUES (1)]);
$node_c->safe_psql($dbname, q[INSERT INTO a2b VALUES (1)]);
sleep(2);
is($node_a->safe_psql($dbname, q[TABLE a2b;]), "1", 'row present on node_a');
is($node_b->safe_psql($dbname, q[TABLE a2b;]), "1", 'row present on node_b');
is($node_c->safe_psql($dbname, q[TABLE a2b;]), "1", 'row present on node_c');

$node_a->safe_psql($dbname, q[TRUNCATE TABLE a2b]);
sleep(2);

# check on  node_a and node_b that the truncate actually deleted the rows in a2b
# but not on node_c or any other table.
is($node_a->safe_psql($dbname, q[TABLE a2b;]), "", 'no row present on node_a');
is($node_b->safe_psql($dbname, q[TABLE a2b;]), "", 'no row present on node_b');
is($node_c->safe_psql($dbname, q[TABLE a2b;]), "1", 'row present on node_c');
is($node_c->safe_psql($dbname, q[TABLE b2c;]), "1", 'row present on node_c');

# check the subscriptions again
$node_b->poll_query_until($dbname,
	q[SELECT EXISTS (
        SELECT 1 FROM pglogical.show_subscription_status()
            WHERE subscription_name = 'sub_a_b' AND status = 'replicating');])
	or BAIL_OUT('subscription failed to reach "replicating" state');
$node_c->poll_query_until($dbname,
	q[SELECT EXISTS (
        SELECT 1 FROM pglogical.show_subscription_status()
            WHERE subscription_name = 'sub_b_c' AND status = 'replicating');])
	or BAIL_OUT('subscription failed to reach "replicating" state');

$node_a->teardown_node;
$node_b->teardown_node;
$node_c->teardown_node;


done_testing();