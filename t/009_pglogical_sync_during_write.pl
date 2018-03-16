#
# Test for RT#60889, trying to reproduce an issue where sync fails during
# catchup with a walsender timeout for as-yet-unknown reasons.
#
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Data::Dumper;
use Test::More;
use Time::HiRes;

my $PGBENCH_SCALE = 1;
my $PGBENCH_CLIENTS = 10;
my $PGBENCH_JOBS = 1;
my $PGBENCH_TIME = 60;
my $WALSENDER_TIMEOUT = '5s';


my $node_provider = get_new_node('provider');
$node_provider->init();
$node_provider->append_conf('postgresql.conf', qq[
wal_level = 'logical'
max_replication_slots = 12
max_wal_senders = 12
wal_sender_timeout = '$WALSENDER_TIMEOUT'
max_connections = 200
log_line_prefix = '%t %p '
shared_preload_libraries = 'pglogical'
track_commit_timestamp = on
pglogical.synchronous_commit = true
]);
$node_provider->dump_info;
$node_provider->start;

my $node_subscriber = get_new_node('subscriber');
$node_subscriber->init();
$node_subscriber->append_conf('postgresql.conf', qq[
shared_preload_libraries = 'pglogical'
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
track_commit_timestamp = on
fsync=off
pglogical.synchronous_commit = true
log_line_prefix = '%t %p '
]);
$node_subscriber->dump_info;
$node_subscriber->start;

# Create provider node on master:
$node_provider->safe_psql('postgres',
        "CREATE USER super SUPERUSER;");
$node_provider->safe_psql('postgres',
        "CREATE EXTENSION IF NOT EXISTS pglogical VERSION '1.0.0';");
$node_provider->safe_psql('postgres',
        "ALTER EXTENSION pglogical UPDATE;");

my $provider_connstr = $node_provider->connstr;
print "node_provider - connstr : $provider_connstr\n";

$node_provider->safe_psql('postgres',
        "SELECT * FROM pglogical.create_node(node_name := 'test_provider', dsn := '$provider_connstr dbname=postgres user=super');");

# Create subscriber node on subscriber:
$node_subscriber->safe_psql('postgres',
        "CREATE USER super SUPERUSER;");
$node_subscriber->safe_psql('postgres',
        "CREATE EXTENSION IF NOT EXISTS pglogical VERSION '1.0.0';");
$node_subscriber->safe_psql('postgres',
        "ALTER EXTENSION pglogical UPDATE;");
my $subscriber_connstr = $node_subscriber->connstr;
print "node_subscriber - connstr : $subscriber_connstr\n";

$node_subscriber->safe_psql('postgres',
        "SELECT * FROM pglogical.create_node(node_name := 'test_subscriber', dsn := '$subscriber_connstr dbname=postgres user=super');");

# Initialise pgbench on provider and print initial data count in tables
$node_provider->command_ok([ 'pgbench', '-i', '-s', $PGBENCH_SCALE],
        'initialize pgbench');

my @pgbench_tables = ('pgbench_accounts', 'pgbench_tellers', 'pgbench_history');

# Add pgbench tables to repset
for my $tbl (@pgbench_tables)
{
	my $setname = 'default';
	$setname = 'default_insert_only' if ($tbl eq 'pgbench_history');
	$node_provider->safe_psql('postgres',
			"SELECT * FROM pglogical.replication_set_add_table('$setname', '$tbl', false);");
}

$node_subscriber->safe_psql('postgres',
        "SELECT pglogical.create_subscription(
    subscription_name := 'test_subscription',
    synchronize_structure := true,
    synchronize_data := true,
    provider_dsn := '$provider_connstr dbname=postgres user=super'
);");

$node_subscriber->poll_query_until('postgres',
	q[SELECT EXISTS (SELECT 1 FROM pglogical.show_subscription_status() where subscription_name = 'test_subscription' AND status = 'replicating')])
	or BAIL_OUT('subscription failed to reach "replicating" state');

# Make write-load active on the tables  pgbench_history
# with this TPC-B-ish run. Run it in the background.

diag "provider is" . $node_provider->name;
diag "max_connections is " . $node_provider->safe_psql('postgres', 'SHOW max_connections;');

my $pgbench_handle = IPC::Run::start(
	[ 'pgbench', '-T', $PGBENCH_TIME, '-j', $PGBENCH_JOBS, '-s', $PGBENCH_SCALE, '-c', $PGBENCH_CLIENTS, $node_provider->connstr('postgres')]);

# Wait for pgbench to connect
$node_provider->poll_query_until('postgres',
	q[SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name = 'pgbench')])
	or BAIL_OUT('subscription failed to reach "replicating" state');

# Let it warm up for a while
sleep($PGBENCH_TIME/10);

my $i = 1;
do {
	# Resync all the tables in turn
	EACH_TABLE: for my $tbl (@pgbench_tables)
	{
		my $resync_start = [Time::HiRes::gettimeofday()];

		eval {
			$node_subscriber->safe_psql('postgres',
					"SELECT * FROM pglogical.alter_subscription_resynchronize_table('test_subscription', '$tbl');");
		};
		if ($@)
		{
			diag "attempt to resync $tbl failed with $@; sync_status is currently " .
				$node_subscriber->safe_psql('postgres', "SELECT sync_status FROM pglogical.local_sync_status WHERE sync_relname = '$tbl'");
			fail("$tbl didn't sync: resync request failed");
			next EACH_TABLE;
		}

		while (1)
		{
			sleep(1);

			my $running = $node_subscriber->safe_psql('postgres',
				qq[SELECT pid, application_name FROM pg_stat_activity WHERE application_name LIKE '%sync%']);

			my $status = $node_subscriber->safe_psql('postgres',
				qq[SELECT sync_status FROM pglogical.local_sync_status WHERE sync_relname = '$tbl']);

			if ($status eq 'r')
			{
				pass("$tbl synced on iteration $i (elapsed " . Time::HiRes::tv_interval($resync_start) . ")" );
				last;
			}
			elsif ($status eq 'y')
			{
				# keep looping until master notices and switches to 'r'
			}
			elsif (!$running)
			{
				fail("$tbl didn't sync on iteration $i, sync worker exited (running=$running) while sync state was '$status' (elapsed " . Time::HiRes::tv_interval($resync_start) . ")" );
			}
		}
	}

	$i ++;

	# and repeat until pgbench exits
} while ($node_provider->safe_psql('postgres', q[SELECT 1 FROM pg_stat_activity WHERE application_name = 'pgbench']));

# Wait for catchup
$node_provider->safe_psql('postgres', 'SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);');

# Compare final table entries on provider and subscriber.
for my $tbl (@pgbench_tables)
{
	my $rowcount_provider = $node_provider->safe_psql('postgres',
			"SELECT count(*) FROM $tbl;");

	my $rowcount_subscriber = $node_subscriber->safe_psql('postgres',
			"SELECT count(*) FROM $tbl;");

	is($rowcount_provider, $rowcount_subscriber,
		"final $tbl row counts match after sync")
		or diag "final provider rowcount for $tbl is $rowcount_provider, but subscriber has $rowcount_subscriber";

}

$node_subscriber->teardown_node;
$node_provider->teardown_node;

done_testing();
