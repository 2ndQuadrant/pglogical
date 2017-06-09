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
use Time::HiRes qw(gettimeofday tv_interval);
# From Pg
use TestLib;
# Local
use PostgresPGLNode;
use PGLDB;
use PGLSubscription;
use PGValues qw(quote_ident);

my $pgldb = 'pgltest';
my $ts;

#
# We'll start with two nodes mutually replicating, but
# we might as well create all three here so we don't have
# to repeat the work later...
#

my @nodes = ();
my @pubs = ();
foreach my $nodename ('node0', 'node1', 'node2') {
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

# create a repset only on the initial node to test
# dump behaviour...
$pubs[0]->create_replication_set('set_only_on_node0');

# With node[0] as the "root", create some tables
# TODO more seed data
$pubs[0]->safe_psql(q[
    -- not added to any set (won't replicate)
    CREATE TABLE seed_table_noset(
        id serial primary key,
        blah text not null
    );

    -- added to a set that is subscribed to
    CREATE TABLE seed_table_include(
        id serial primary key,
        blah text not null
    );

    -- added to a set that doesn't get subscribed to
    CREATE TABLE seed_table_exclude(
        id serial primary key,
        blah text not null
    );
]);

$pubs[0]->safe_psql(q[ INSERT INTO seed_table_noset(blah) VALUES ('seed') ]);
$pubs[0]->safe_psql(q[ INSERT INTO seed_table_include(blah) VALUES ('seed') ]);
$pubs[0]->safe_psql(q[ INSERT INTO seed_table_exclude(blah) VALUES ('seed') ]);

$pubs[0]->replication_set_add_table('set_include', 'seed_table_include', 1);
$pubs[0]->replication_set_add_sequence('set_include', 'seed_table_include_id_seq', 1);
$pubs[0]->replication_set_add_table('set_exclude', 'seed_table_exclude', 1);
$pubs[0]->replication_set_add_sequence('set_exclude', 'seed_table_exclude_id_seq', 1);


# Initial replication set state. Note that there's no true 'default' membership
# of tables.
is($pubs[0]->safe_psql(q[ SELECT nspname, relname, set_name FROM pglogical.TABLES ORDER BY 1, 2, 3]),
	q[public|seed_table_exclude|set_exclude
public|seed_table_include|set_include
public|seed_table_noset|],
	'repset memberships configured on node0');

is($pubs[0]->safe_psql(q[ SELECT s.set_name, n.node_name FROM pglogical.replication_set s LEFT JOIN pglogical.node n ON s.set_nodeid = n.node_id ORDER BY 1, 2]),
	q[ddl_sql|node0_pub
default|node0_pub
default_insert_only|node0_pub
set_exclude|node0_pub
set_include|node0_pub
set_only_on_node0|node0_pub],
	'repset state on node0');

#
# OK, 2-node mutual rep subscription
#

my %common_subscribe_params = (
    replication_sets => ['default', 'set_include', 'ddl_sql'],
    forward_origins => [],
    synchronize_structure => 'true',
    synchronize_data => 'true',
    apply_delay => '500ms',
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

# Let the subscribers catch up
my %nodepairs = ( 1 => 0, 0 => 1);
while (my ($a, $b) = each %nodepairs) {
    $sub = $subs[$a][$b];
    ok($sub->wait_for_replicating(), "subscription replicating $a=>$b")
        or diag explain $sub->subscription_status;
    ok($sub->wait_for_catchup($pubs[$b]), "caught up $a=>$b");
}

# Initial repset state on node1 is empty, as we don't copy repsets.
# (we know pubs[0] is unchanged so no need to look again)
#
is($pubs[1]->safe_psql(q[ SELECT nspname, relname, set_name FROM pglogical.TABLES ORDER BY 1, 2, 3]),
	q[public|seed_table_exclude|
public|seed_table_include|
public|seed_table_noset|], 'repsets empty on node1 after subscribe');

# we shouldn't see repset 'set_only_on_node0' even when not filtering for the
# local node, since repsets don't get copied from upstream. These are the
# sets we created during setup and the default sets.
is($pubs[1]->safe_psql(q[ SELECT s.set_name, n.node_name FROM pglogical.replication_set s LEFT JOIN pglogical.node n ON s.set_nodeid = n.node_id ORDER BY 1, 2]),
	q[ddl_sql|node1_pub
default|node1_pub
default_insert_only|node1_pub
set_exclude|node1_pub
set_include|node1_pub], 'node0 replication sets did not get copied');

# The initial tables are synced by the time status=replicating on the subscription
# but due to 2ndQuadrant/pglogical_internal#134 they'll show as 'unknown' when we
# examine their state on node1 (which cloned them) and on node0 (where they were
# created before being added to a repset).

while (my ($a, $b) = each %nodepairs) {
    TODO: {
        local $TODO = 'getting unknown sync status seems wrong, see 2ndQuadrant/pglogical_internal#134';
        my @tables = (['public','seed_table_noset'], ['public','seed_table_include']);
        $sub = $subs[$a][$b];
        say "XXX" . (map { Dumper($_) } @{$sub->table_sync_status(@tables)});
        is((join "\n", (map { ${$_}{'nspname'} . "," . ${$_}{'relname'} . "," . ${$_}{'status'} } @{$sub->table_sync_status(@tables)})),
            qq[public,seed_table_noset,synchronized\npublic,seed_table_include,synchronized],
            'initial tables show sync status synchronized')
            or diag explain $sub->table_sync_status(@tables);
    }
}

# Create some new tables and contents
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

# "replicate" using a repset we don't subscribe to
$pubs[0]->replicate_ddl(q[
    CREATE TABLE public.donotreplicateme (
        id integer primary key,
        other integer,
        blah text
    );
], ['set_exclude']);

# created by direct DDL, and thus not replicated
$pubs[0]->safe_psql(q[
    CREATE TABLE public.direct_ddl (
        id integer primary key,
        other integer,
        blah text
    );
]);

# this one won't be added to any repset
$pubs[0]->replicate_ddl(q[
    CREATE TABLE public.tbl_noset (
        id integer primary key,
        other integer,
        blah text
    );
]);

$pubs[0]->replication_set_add_table('set_include', 'tbl_included', 1);
$pubs[0]->replication_set_add_table('set_exclude', 'tbl_excluded', 1);

$pubs[0]->replicate_ddl(q[
    CREATE TABLE public.tbl_included_selfadd (
        id integer primary key,
        other integer,
        blah text
    );

	-- Add ourselves within the same DDL. Users will do it...
	SELECT pglogical.replication_set_add_table('set_include', 'public.tbl_included_selfadd', true);
]);

# Check repset state is as expected
is($pubs[0]->safe_psql(q[ SELECT nspname, relname, set_name FROM pglogical.TABLES ORDER BY 1, 2, 3]),
    q[public|direct_ddl|
public|donotreplicateme|
public|seed_table_exclude|set_exclude
public|seed_table_include|set_include
public|seed_table_noset|
public|tbl_excluded|set_exclude
public|tbl_included|set_include
public|tbl_included_selfadd|set_include
public|tbl_noset|],
   'TABLES state on node0 after replicated table creation');

# Wait for added tables to sync
$ts = [gettimeofday()];
$subs[1][0]->wait_for_catchup($pubs[0]);
ok($subs[1][0]->wait_for_table_sync(['public','tbl_included'], ['public','tbl_included_selfadd']), "tables synced from 0=>1")
    or diag explain $sub->table_sync_status();
print "Sync took " . tv_interval ( $ts, [gettimeofday()] ) . " seconds\n";

is($pubs[1]->safe_psql(q[SELECT 1 FROM information_schema.tables where table_schema = 'public' and table_name = 'tbl_included']),
    '1', 'table tbl_included got created');
# The repset used for ddl was ddl_sql so the definition got replicated, even
# though the repset we assigned for the content means the contents won't
# replicate
is($pubs[1]->safe_psql(q[SELECT 1 FROM information_schema.tables where table_schema = 'public' and table_name = 'tbl_excluded']),
    '1', 'table tbl_excluded got created');
# wheras in this one the ddl its self didn't get replicated since we did
# it in a nondefault repset that isn't subscribed
is($pubs[1]->safe_psql(q[SELECT 1 FROM information_schema.tables where table_schema = 'public' and table_name = 'donotreplicateme']),
    '', 'table donotreplicateme did NOT get created');
# tbl_noset will get created but later we'll see that it's empty since we
# didn't add it to any repset and pglogical doesn't do so automatically
is($pubs[1]->safe_psql(q[SELECT 1 FROM information_schema.tables where table_schema = 'public' and table_name = 'tbl_noset']),
    '1', 'table tbl_noset got created');
# The one that added its self to the repset did get created
is($pubs[1]->safe_psql(q[SELECT 1 FROM information_schema.tables where table_schema = 'public' and table_name = 'tbl_excluded']),
    '1', 'table tbl_excluded got created');
# and is automatically a member of set_include
is($pubs[1]->safe_psql(q[SELECT 1 FROM pglogical.TABLES where nspname = 'public' AND relname = 'tbl_included_selfadd' AND set_name = 'set_include']),
	'1', 'table tbl_included_selfadd got created and added to repset');

# Once we've caught up, pglogical.TABLES should show the tables that got synced
# but no replication set memberships except for the one that added its self
# via direct DDL. Repset memberships aren't synced with tables.
is($pubs[1]->safe_psql(q[ SELECT nspname, relname, set_name FROM pglogical.TABLES ORDER BY 1, 2, 3]),
   q[public|seed_table_exclude|
public|seed_table_include|
public|seed_table_noset|
public|tbl_excluded|
public|tbl_included|
public|tbl_included_selfadd|set_include
public|tbl_noset|],
   'TABLES state on node1 after initial catchup');

# We must add the same memberships here. We could cheat by copying
# pglogical.TABLES state on the peer, but cleaner not to.
#
# We must NOT sync the contents or we'll clobber what's on node0.
$pubs[1]->replication_set_add_table('set_include', 'tbl_included', 0);
$pubs[1]->replication_set_add_table('set_include', 'seed_table_include', 0);
$pubs[1]->replication_set_add_table('set_exclude', 'tbl_excluded', 0);
$pubs[1]->replication_set_add_table('set_exclude', 'seed_table_exclude', 0);
   
# pglogical.TABLES now looks the same as on pubs[0] except
# that tables donotreplicateme and donotreplicateme_directddl
# are absent.
is($pubs[1]->safe_psql(q[ SELECT nspname, relname, set_name FROM pglogical.TABLES ORDER BY 1, 2, 3]),
   'public|seed_table_exclude|set_exclude
public|seed_table_include|set_include
public|seed_table_noset|
public|tbl_excluded|set_exclude
public|tbl_included|set_include
public|tbl_included_selfadd|set_include
public|tbl_noset|',
   'TABLES state on node0 after adding to repsets');

# Add some data in each node to test simple MM behaviour
foreach my $node (0, 1) {
	foreach my $tbl ('tbl_included', 'tbl_excluded', 'tbl_noset', 'tbl_included_selfadd') {
        $pubs[$node]->safe_psql(q[INSERT INTO ] . quote_ident($tbl) . qq[(id, blah) VALUES ($node, 'from_node$node')]);
	}
	# These use SERIAL so a bit different. We need to offset the sequences or we'll collide.
	foreach my $tbl ('seed_table_noset', 'seed_table_include', 'seed_table_exclude') {
		my $start = $node + 100;
		$pubs[$node]->safe_psql('ALTER SEQUENCE ' . quote_ident($tbl . '_id_seq') . " START WITH $start INCREMENT BY 100 RESTART");
		$pubs[$node]->safe_psql(q[INSERT INTO ] . quote_ident($tbl) . qq[(blah) VALUES ('from_node$node')]);
	}
}
# can't do this on node1, table doesn't exist. We're doing it to make sure we
# don't try to replicate the row.
$pubs[0]->safe_psql(q[INSERT INTO donotreplicateme (id, blah) VALUES (0, 'from_node0')]);
$pubs[0]->safe_psql(q[INSERT INTO direct_ddl (id, blah) VALUES (0, 'from_node0')]);

$subs[1][0]->wait_for_catchup($pubs[0]);

my @expect = (
    { node => $pubs[0],
      tbl_included => qq[0|from_node0\n1|from_node1],
      tbl_excluded => qq[0|from_node0],
      tbl_noset => qq[0|from_node0],
      seed_table_include => qq[1|seed\n100|from_node0\n101|from_node1],
      seed_table_exclude => qq[1|seed\n100|from_node0],
      seed_table_noset => qq[1|seed\n100|from_node0],
      tbl_included_selfadd => qq[0|from_node0\n1|from_node1],
      donotreplicateme => qq[0|from_node0],
      direct_ddl => qq[0|from_node0],
    },
    { node => $pubs[1],
      tbl_included => qq[0|from_node0\n1|from_node1],
      tbl_excluded => qq[1|from_node1],
      tbl_noset => qq[1|from_node1],
      seed_table_include => qq[1|seed\n100|from_node0\n101|from_node1],
      seed_table_exclude => qq[101|from_node1],
      seed_table_noset => qq[101|from_node1],
      tbl_included_selfadd => qq[0|from_node0\n1|from_node1],
    }
);
foreach my $x (@expect) {
    my (%expected) = %$x;
    my $pub = delete $expected{'node'};
    while (my ($tblname, $expected_content) = each %expected) {
        is($pub->safe_psql("SELECT id, blah FROM " . quote_ident($tblname) . " ORDER BY id"),
            $expected_content,
           "rows matched on " . $tblname . " in ". $pub->name);
    }
}

done_testing();
