use strict;
use warnings;
use v5.10.0;
use Cwd;
use Config;
use TestLib;
use Test::More;
use Data::Dumper;
# From Pg
use TestLib;
# Local
use PostgresPGLNode;
use PGLDB;
use PGLSubscription;

my $pgldb = 'pgltest';
my $providername = 'test_provider';
my $subscribername = 'test_subscriber';
my $subscriptionname = 'test_nondefault_repsets';

my $node_pub = get_new_pgl_node('node_pub');
$node_pub->init;
$node_pub->start;

my $node_sub = get_new_pgl_node('node_sub');
$node_sub->init;
$node_sub->start;

$node_pub->safe_psql('postgres', "CREATE DATABASE $pgldb");
$node_sub->safe_psql('postgres', "CREATE DATABASE $pgldb");

my $pgl_pub = PGLDB->new(
	node => $node_pub,
	dbname => $pgldb,
	name => $providername);
$pgl_pub->create;
$pgl_pub->create_replication_set('set_include');
$pgl_pub->create_replication_set('set_exclude');

my $pgl_sub = PGLDB->new(
	node => $node_sub,
	dbname => $pgldb,
	name => $subscribername);
$pgl_sub->create;

my $subscription = PGLSubscription->new(
	from => $pgl_sub,
	name => $subscriptionname);
$subscription->create(
	$pgl_pub,
	replication_sets => ['set_include', 'ddl_sql'],
	forward_origins => [],
    synchronize_structure => 'false',
    synchronize_data => 'false' 
);

ok($subscription->wait_for_replicating(), 'replication started');

say "Subscription state: " . Dumper($subscription->subscription_status());

$pgl_pub->replicate_ddl(q[
    CREATE TABLE public.tbl_included (
        id serial primary key,
        other integer
    );
]);

$pgl_pub->replicate_ddl(q[
    CREATE TABLE public.tbl_excluded (
        id serial primary key,
        other integer
    );
]);

say "Subscription state: " . Dumper($subscription->subscription_status());

$pgl_pub->replication_set_add_table('set_include', 'tbl_included', 1);
$pgl_pub->replication_set_add_table('set_exclude', 'tbl_excluded', 1);

ok($subscription->wait_for_catchup($pgl_pub));
ok($subscription->wait_for_table_sync(['public', 'tbl_included']), 'tables synced after add');

say "sync status: " . Dumper($subscription->table_sync_status(['public', 'tbl_included']));

is($pgl_sub->safe_psql("SELECT sync_kind, sync_status FROM pglogical.local_sync_status WHERE sync_relname = 'tbl_included' AND sync_nspname = 'public'"),
   'd|r',
   'table sync for tbl_included ok');

is($pgl_sub->safe_psql("SELECT sync_kind, sync_status FROM pglogical.local_sync_status WHERE sync_relname = 'tbl_excluded' AND sync_nspname = 'public'"),
   '',
   'table not part of repset not visible in status');

$pgl_pub->safe_psql('INSERT INTO tbl_excluded (other) VALUES (4)');
$pgl_pub->safe_psql('INSERT INTO tbl_included (other) VALUES (4)');

$node_sub->poll_query_until($pgl_sub->dbname, 'SELECT EXISTS (SELECT 1 FROM tbl_included WHERE other = 4)');

is($pgl_sub->safe_psql("SELECT EXISTS (SELECT 1 FROM tbl_included WHERE other = 4)"),
   't',
   'row replicated in included set');

is($pgl_sub->safe_psql("SELECT EXISTS (SELECT 1 FROM tbl_excluded WHERE other = 4)"),
   'f',
   'Row not replicated for table in non-replicated set');

$pgl_pub->replication_set_remove_table('set_include', 'tbl_included');
$pgl_pub->replication_set_remove_table('set_exclude', 'tbl_excluded');
$pgl_pub->replication_set_add_table('set_include', 'tbl_excluded', 1);
$pgl_pub->replication_set_add_table('set_exclude', 'tbl_included', 1);

$pgl_pub->safe_psql('INSERT INTO tbl_excluded (other) VALUES (5)');
$pgl_pub->safe_psql('INSERT INTO tbl_included (other) VALUES (5)');

# since we're replicating it now
$node_sub->poll_query_until($pgl_sub->dbname, 'SELECT EXISTS (SELECT 1 FROM tbl_excluded WHERE other = 5)');

is($pgl_sub->safe_psql("SELECT sync_kind, sync_status FROM pglogical.local_sync_status WHERE sync_relname = 'tbl_excluded' AND sync_nspname = 'public'"),
   'd|r',
   'table added to repset visible in status');

# We don't remove entries from sync status when they're removed
# from a repset; we just show the old status.
is($pgl_sub->safe_psql("SELECT sync_kind, sync_status FROM pglogical.local_sync_status WHERE sync_relname = 'tbl_included' AND sync_nspname = 'public'"),
   'd|r',
   'table removed from repset still visible in status');

is($pgl_sub->safe_psql("SELECT EXISTS (SELECT 1 FROM tbl_included WHERE other = 4)"),
   't',
   'old row still in included table once removed from set');

is($pgl_sub->safe_psql("SELECT EXISTS (SELECT 1 FROM tbl_included WHERE other = 5)"),
   'f',
   'new row not in included table after removed from set');

is($pgl_sub->safe_psql("SELECT EXISTS (SELECT 1 FROM tbl_excluded WHERE other = 4)"),
   't',
   'now-included table old row now visible due to table sync');

is($pgl_sub->safe_psql("SELECT EXISTS (SELECT 1 FROM tbl_excluded WHERE other = 5)"),
   't',
   'new row in now-included table visible');

done_testing();
