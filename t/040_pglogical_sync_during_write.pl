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
use Carp;

my $PGBENCH_SCALE = $ENV{PGBENCH_SCALE} // 1;
my $PGBENCH_CLIENTS = $ENV{PGBENCH_CLIENTS} // 10;
my $PGBENCH_JOBS = $ENV{PGBENCH_JOBS} // 1;
my $PGBENCH_TIME = $ENV{PGBENCH_TIME} // 120;
my $WALSENDER_TIMEOUT = $ENV{PGBENCH_TIMEOUT} // '5s';

$SIG{__DIE__} = sub { Carp::confess @_ };
$SIG{INT}  = sub { die("interupted by SIGINT"); };

my $dbname="pgltest";
my $super_user="super";
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
$node_provider->safe_psql('postgres', "CREATE DATABASE $dbname");

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
$node_subscriber->safe_psql('postgres', "CREATE DATABASE $dbname");

# Create provider node on master:
$node_provider->safe_psql($dbname,
        "CREATE USER $super_user SUPERUSER;");
$node_provider->safe_psql($dbname,
        "CREATE EXTENSION IF NOT EXISTS pglogical VERSION '1.0.0';");
$node_provider->safe_psql($dbname,
        "ALTER EXTENSION pglogical UPDATE;");

my $provider_connstr = $node_provider->connstr;
print "node_provider - connstr : $provider_connstr\n";

$node_provider->safe_psql($dbname,
        "SELECT * FROM pglogical.create_node(node_name := 'test_provider', dsn := '$provider_connstr dbname=$dbname user=$super_user');");

# Create subscriber node on subscriber:
$node_subscriber->safe_psql($dbname,
        "CREATE USER $super_user SUPERUSER;");
$node_subscriber->safe_psql($dbname,
        "CREATE EXTENSION IF NOT EXISTS pglogical VERSION '1.0.0';");
$node_subscriber->safe_psql($dbname,
        "ALTER EXTENSION pglogical UPDATE;");
my $subscriber_connstr = $node_subscriber->connstr;
print "node_subscriber - connstr : $subscriber_connstr\n";

$node_subscriber->safe_psql($dbname,
        "SELECT * FROM pglogical.create_node(node_name := 'test_subscriber', dsn := '$subscriber_connstr dbname=$dbname user=$super_user');");

# Initialise pgbench on provider and print initial data count in tables
$node_provider->command_ok([ 'pgbench', '-i', '-s', $PGBENCH_SCALE, $dbname],
        'initialize pgbench');

my @pgbench_tables = ('pgbench_accounts', 'pgbench_tellers', 'pgbench_history');

# Add pgbench tables to repset
for my $tbl (@pgbench_tables)
{
	my $setname = 'default';
	$setname = 'default_insert_only' if ($tbl eq 'pgbench_history');
	$node_provider->safe_psql($dbname,
			"SELECT * FROM pglogical.replication_set_add_table('$setname', '$tbl', false);");
}

$node_subscriber->safe_psql($dbname,
        "SELECT pglogical.create_subscription(
    subscription_name := 'test_subscription',
    synchronize_structure := true,
    synchronize_data := true,
    provider_dsn := '$provider_connstr dbname=$dbname user=$super_user'
);");

$node_subscriber->poll_query_until($dbname,
	q[SELECT EXISTS (SELECT 1 FROM pglogical.show_subscription_status() where subscription_name = 'test_subscription' AND status = 'replicating')])
	or BAIL_OUT('subscription failed to reach "replicating" state');

# Make write-load active on the tables  pgbench_history
# with this TPC-B-ish run. Run it in the background.

diag "provider is" . $node_provider->name;
diag "max_connections is " . $node_provider->safe_psql($dbname, 'SHOW max_connections;');

my $pgbench_stdout='';
my $pgbench_stderr='';
my $pgbench_handle = IPC::Run::start(
	[ 'pgbench', '-T', $PGBENCH_TIME, '-j', $PGBENCH_JOBS, '-s', $PGBENCH_SCALE, '-c', $PGBENCH_CLIENTS, $node_provider->connstr($dbname)],
        '>', \$pgbench_stdout, '2>', \$pgbench_stderr);
$pgbench_handle->pump();


# Wait for pgbench to connect
$node_provider->poll_query_until($dbname,
	q[SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE query like 'UPDATE pgbench%')])
	or BAIL_OUT('pgbench process is not running currently');

$node_provider->safe_psql($dbname, q[ALTER SYSTEM SET log_statement = 'ddl']);
$node_provider->safe_psql($dbname, q[SELECT pg_reload_conf();]);

# Let it warm up for a while
note "warming up pgbench for " . ($PGBENCH_TIME/10) . "s";
sleep($PGBENCH_TIME/10);
note "done warmup";

open(my $publog, "<", $node_provider->logfile)
	or die "can't open log file for provider at " . $node_provider->logfile . ": $!";
open(my $sublog, "<", $node_subscriber->logfile)
	or die "can't open log file for subscriber at " . $node_subscriber->logfile . ": $!";

my $walsender_pid = int($node_provider->safe_psql($dbname, q[SELECT pid FROM pg_stat_activity WHERE application_name = 'test_subscription']));
my $apply_pid = int($node_subscriber->safe_psql($dbname, q[SELECT pid FROM pg_stat_activity WHERE application_name LIKE '%apply%']));
note "wal sender pid is $walsender_pid; apply worker pid is $apply_pid";

# Seek to log EOF
seek($publog, 2, 0);
seek($sublog, 2, 0);

my $i = 1;
do {
	# Resync all the tables in turn
	EACH_TABLE: for my $tbl (@pgbench_tables)
	{
		my $publogpos = tell($publog);
		my $sublogpos = tell($sublog);

		my $resync_start = [Time::HiRes::gettimeofday()];

		eval {
			$node_subscriber->safe_psql($dbname,
					"SELECT * FROM pglogical.alter_subscription_resynchronize_table('test_subscription', '$tbl');");
		};
		if ($@)
		{
			diag "attempt to resync $tbl failed with $@; sync_status is currently " .
				$node_subscriber->safe_psql($dbname, "SELECT sync_status FROM pglogical.local_sync_status WHERE sync_relname = '$tbl'");
			fail("$tbl didn't sync: resync request failed");
			next EACH_TABLE;
		}

		while (1)
		{
			Time::HiRes::usleep(100);

			my $running = $node_subscriber->safe_psql($dbname,
				qq[SELECT pid FROM pg_stat_activity WHERE application_name LIKE '%sync%']);

			my $status = $node_subscriber->safe_psql($dbname,
				qq[SELECT sync_status FROM pglogical.local_sync_status WHERE sync_relname = '$tbl']);

			if ($status eq 'r')
			{
				pass("$tbl synced on iteration $i (elapsed " . Time::HiRes::tv_interval($resync_start) . ")" );
				last;
			}
			elsif ($status eq 'i')
			{
				# worker still starting
			}
			elsif ($status eq 'y')
			{
				# worker done but master hasn't noticed yet
				# keep looping until master notices and switches to 'r'
			}
			elsif (!$running)
			{
				fail("$tbl didn't sync on iteration $i, sync worker exited (running=$running) while sync state was '$status' (elapsed " . Time::HiRes::tv_interval($resync_start) . ")" );
			}
		}

		# look for walsender timeouts in logs since last test
		# We must seek to reset any prior eof marker
		seek($publog, 0, $publogpos);
		seek($sublog, 0, $sublogpos);
		# then look for log lines of interest
		my $timeout_line;
		my $finished_sync_line;
		while (my $line = <$publog>)
		{
			if ($line =~ qr/replication timeout/ && !$timeout_line)
			{
				$timeout_line = $line;

				diag "status line after failed sync is "
					. $node_subscriber->safe_psql($dbname,
					 	qq[SELECT * FROM pglogical.local_sync_status WHERE sync_relname = '$tbl']);

				if ($line =~ qr/\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [A-Z]+ (\d+) LOG:/)
				{
					if (int($1) == $walsender_pid)
					{
						diag "terminated walsender was the main walsender for the apply worker, trying to make apply core of pid $apply_pid";
						system("gcore -o tmp_check/core.$apply_pid $apply_pid")
							and diag "core saved as tmp_check/core.$apply_pid"
							or diag "couldn't save core, see logs";
					}
					else
					{
						diag "terminated walsender was not for apply worker, looking for a sync worker";
						my $sync_pid = int($node_subscriber->safe_psql($dbname, q[SELECT pid FROM pg_stat_activity WHERE application_name LIKE '%sync%']));
						if ($sync_pid)
						{
							diag "found running sync worker $sync_pid, trying to make core";
							system("gcore $sync_pid");
						}
						else
						{
							diag "no sync worker found running";
						}
					}
				}
				else
				{
					carp "couldn't match line format for $line";
				}
			}
		}

		while (my $line = <$sublog>)
		{
			if ($line =~ qr/finished sync of table/ && !$finished_sync_line)
			{
				$finished_sync_line = $line;
			}
		}

		# This test is racey, because we don't emit this message until
		# after the sync commits so we quite possibly won't see it here
		# after we finish waiting for synced state.
		#
		#isnt($finished_sync_line, undef, "found finished sync line in last test logs");

		is($timeout_line, undef, "no walsender timeout since last test");
	}

	$i ++;

	# and repeat until pgbench exits
} while ($node_provider->safe_psql($dbname, q[SELECT 1 FROM pg_stat_activity WHERE application_name = 'pgbench']));

$pgbench_handle->finish;
note "##### output of pgbench #####";
note $pgbench_stdout;
note "##### end of output #####";

is($pgbench_handle->full_result(0), 0, "pgbench run successfull ");

note " waiting for catchup";

# Wait for catchup
$node_provider->safe_psql($dbname, 'SELECT pglogical.wait_slot_confirm_lsn(NULL, NULL);');

note "comparing tables";

# Compare final table entries on provider and subscriber.
for my $tbl (@pgbench_tables)
{
	my $rowcount_provider = $node_provider->safe_psql($dbname,
			"SELECT count(*) FROM $tbl;");

	my $rowcount_subscriber = $node_subscriber->safe_psql($dbname,
			"SELECT count(*) FROM $tbl;");

	my $matched = is($rowcount_subscriber, $rowcount_provider,
		"final $tbl row counts match after sync");
	if (!$matched)
	{
		diag "final provider rowcount for $tbl is $rowcount_provider, but subscriber has $rowcount_subscriber";

		my $sortkey;
		if ($tbl == "pgbench_history") {
			$sortkey = "1, 2, 3, 4";
		} elsif ($tbl == "pgbench_tellers" || $tbl == "pgbench_accounts") {
			$sortkey = "1, 2";
		} elsif ($tbl == "pgbench_branches") {
			$sortkey = "1";
		}

		# Compare the tables
		$node_provider->safe_psql($dbname, qq[\\copy (SELECT * FROM $tbl ORDER BY $sortkey) to tmp_check/$tbl-provider]);
		$node_subscriber->safe_psql($dbname, qq[\\copy (SELECT * FROM $tbl ORDER BY $sortkey) to tmp_check/$tbl-subscriber]);
		$node_subscriber->safe_psql($dbname, qq[\\copy (SELECT * FROM $tbl, pglogical.xact_commit_timestamp_origin($tbl.xmin) ORDER BY $sortkey) to tmp_check/$tbl-subscriber-detail]);
		IPC::Run::run(['diff', '-u', "tmp_check/$tbl-provider", "tmp_check/$tbl-subscriber"], '>', "tmp_check/$tbl-diff");
		diag "differences between $tbl on provider and subscriber recorded in tmp_check/";
	}

}

$node_subscriber->teardown_node;
$node_provider->teardown_node;

done_testing();
