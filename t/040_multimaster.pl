#
# This test exercises pglogical's multimaster capabilities.
# 
# Note that pglogical by its self doesn't provide a complete
# MM system. You can break replication easily, as these tests
# show. But we can test the building blocks here.
#

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

#
# We'll start with two nodes mutually replicating, but
# we might as well create all three here so we don't have
# to repeat the work later...
#

my @nodes = ();
my @pubs = ();
foreach my $nodename ('node1', 'node2', 'node3') {
    my $node = get_new_pgl_node($nodename);
    $node->init;
    $node->start;
    $node->safe_psql('postgres', "CREATE DATABASE $pgldb");
    push @nodes, $node;

    my $pub = PGLDB->new(
        node => $node,
        dbname => $pgldb,
        name => $nodename . "_pub");
    $pub->create;
    $pub->create_replication_set('set_include');
    $pub->create_replication_set('set_exclude');
    push @pubs, $pub;
}

# With node[0] as the "root", create some tables
# TODO more seed data
$pubs[0]->safe_psql(q[
    CREATE TABLE test_table(
        id serial primary key,
        dummy text not null
    );
]);

$pubs[0]->safe_psql(q[
    INSERT INTO test_table(dummy) VALUES ('seed')
]);

#
# OK, 2-node mutual rep subscription
#

my %common_subscribe_params = (
    replication_sets => ['default', 'set_include', 'ddl_sql'],
    forward_origins => [],
    synchronize_structure => 'true',
    synchronize_data => 'true'
);

# subs array indexed by [from][to]
my @subs = ([],[],[]);

# node[1] gets a sync copy of node[0]'s data
my $sub = PGLSubscription->new(
    from => $pubs[1],
    name => $pubs[1]->name . "_" . $pubs[0]->name
    );
$sub->create( $pubs[0], %common_subscribe_params);
$subs[1][0] = $sub;
    
# node[0] doesn't try to copy node[1]'s
$sub= PGLSubscription->new(
    from => $pubs[0],
    name => $pubs[0]->name . "_" . $pubs[1]->name
    );
$sub->create(
    $pubs[1], %common_subscribe_params,
    synchronize_structure => 'true',
    synchronize_data => 'true' 
    );
$subs[0][1] = $sub;

my %nodepairs = ( 1 => 0, 0 => 1);
while (my ($a, $b) = each %nodepairs) {
    $sub = $subs[$a][$b];
    ok($sub->wait_for_replicating(), "subscription replicating $a=>$b ")
        or diag explain $sub->subscription_status;
    ok($sub->wait_for_sync(), "tables synced from $a=>$b")
        or diag explain $sub->sync_status();
}


# Some tables and contents
$pubs[0]->replicate_ddl(q[
    CREATE TABLE public.tbl_included (
        id integer primary key,
        other integer,
        blah text
    );
]);

$pubs[0]->replicate_ddl(q[
    CREATE TABLE public.tbl_excluded (
        id integer primary key,
        other integer,
        blah text
    );
]);

$pubs[0]->replication_set_add_table('set_include', 'tbl_included', 1);
$pubs[0]->replication_set_add_table('set_exclude', 'tbl_excluded', 1);
ok($subs[1][0]->wait_for_sync(), 'tables synced on 1 after add to 0');
# TODO
# Adding table membership must be done on each node??
$pubs[1]->replication_set_add_table('set_include', 'tbl_included', 1);
$pubs[1]->replication_set_add_table('set_exclude', 'tbl_excluded', 1);

foreach my $a (0 .. 1) {
    $pubs[$a]->poll_query_until(q[SELECT 't' FROM pglogical.local_sync_status WHERE sync_relname = 'tbl_included' AND sync_nspname = 'public' AND sync_status = 'd']);
    
    # TODO qualify by filtering for sub, or query sync status result from sub
    is($pubs[$a]->safe_psql("SELECT sync_kind, sync_status FROM pglogical.local_sync_status WHERE sync_relname = 'tbl_included' AND sync_nspname = 'public'"),
       'd|r',
       "table sync for tbl_included ok on $a");

    is($pubs[$a]->safe_psql("SELECT sync_kind, sync_status FROM pglogical.local_sync_status WHERE sync_relname = 'tbl_excluded' AND sync_nspname = 'public'"),
       '',
       "table not part of repset not visible in status on $a");
}

$pubs[0]->safe_psql(q[INSERT INTO tbl_included (id, blah) VALUES (0, 'from_node0')]);
$pubs[1]->safe_psql(q[INSERT INTO tbl_included (id, blah) VALUES (1, 'from_node1')]);
$pubs[0]->safe_psql(q[INSERT INTO tbl_excluded (id, blah) VALUES (0, 'from_node0')]);
$pubs[1]->safe_psql(q[INSERT INTO tbl_excluded (id, blah) VALUES (1, 'from_node1')]);

$pubs[0]->poll_query_until('SELECT EXISTS (SELECT 1 FROM tbl_included WHERE other = 5)');
$pubs[1]->poll_query_until('SELECT EXISTS (SELECT 1 FROM tbl_included WHERE other = 4)');

my @expect = (
    [ $pubs[0], qq[0|from_node0\n1|from_node1], qq[0|from_node0] ],
    [ $pubs[1],  qq[0|from_node0\n1|from_node1], qq[1|from_node1] ],
);
foreach my $x (@expect) {
    my ($pub, @expected) = @$x;
    is($pub->safe_psql("SELECT id, blah FROM tbl_included ORDER BY id"),
        $expected[0],
       "row replicated in included set on " . $pub->name);

    is($pub->safe_psql("SELECT id, blah FROM tbl_excluded ORDER BY id"),
       $expected[1],
       "Row not replicated for table in non-replicated set on " . $pub->name);
}

done_testing();
