#
# Test failover slots
#
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Data::Dumper;
use Test::More;


# Initialize master node
my $node_provider = get_new_node('master');
$node_provider->init();
$node_provider->append_conf('postgresql.conf', "wal_level = 'logical'\n");
$node_provider->append_conf('postgresql.conf', "max_replication_slots = 12\n");
$node_provider->append_conf('postgresql.conf', "max_wal_senders = 12\n");
$node_provider->append_conf('postgresql.conf', "wal_sender_timeout = 5s\n");
$node_provider->append_conf('postgresql.conf', "max_connections = 20\n");
$node_provider->append_conf('postgresql.conf', "log_line_prefix = '%t %p '\n");
$node_provider->append_conf('postgresql.conf', "shared_preload_libraries = 'pglogical'\n");
$node_provider->append_conf('postgresql.conf', "track_commit_timestamp = on\n");
$node_provider->append_conf('postgresql.conf', "pglogical.synchronous_commit = true\n");
#$node_provider->append_conf('postgresql.conf', "log_min_messages = 'debug2'\n");
$node_provider->dump_info;
$node_provider->start;

#Initialize the subscriber
my $node_subscriber = get_new_node('subscriber');
$node_subscriber->init();
$node_subscriber->append_conf('postgresql.conf', "shared_preload_libraries = 'pglogical'\n");
$node_subscriber->append_conf('postgresql.conf', "wal_level = logical\n");
$node_subscriber->append_conf('postgresql.conf', "max_wal_senders = 10\n");
$node_subscriber->append_conf('postgresql.conf', "max_replication_slots = 10\n");
$node_subscriber->append_conf('postgresql.conf', "track_commit_timestamp = on\n");
$node_subscriber->append_conf('postgresql.conf', "fsync=off\n");
$node_subscriber->append_conf('postgresql.conf', "pglogical.synchronous_commit = true\n");
$node_subscriber->append_conf('postgresql.conf', "log_line_prefix = '%t %p '\n");
#$node_subscriber->append_conf('postgresql.conf', "log_min_messages = 'debug2'\n");
$node_subscriber->dump_info;
$node_subscriber->start;

# Create provider node on master:
$node_provider->safe_psql('postgres',
        "CREATE USER super SUPERUSER;");
$node_provider->safe_psql('postgres',
        "CREATE EXTENSION IF NOT EXISTS pglogical VERSION '1.0.0';");
$node_provider->safe_psql('postgres',
        "ALTER EXTENSION pglogical UPDATE;");

my $master_connstr = $node_provider->connstr;
print "node_provider - connstr : $master_connstr\n";

$node_provider->safe_psql('postgres',
        "SELECT * FROM pglogical.create_node(node_name := 'test_provider', dsn := '$master_connstr dbname=postgres user=super');");

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

$node_subscriber->safe_psql('postgres',
        "SELECT pglogical.create_subscription(
    subscription_name := 'test_subscription',
    synchronize_data := false,
    provider_dsn := '$master_connstr dbname=postgres user=super'
);");

my $subscription_status = 'reset';

while($subscription_status ne 'replicating'){

   print("subscription_status is $subscription_status\n");

   # poll for subscription_status
   $subscription_status = $node_subscriber->safe_psql('postgres',
        "SELECT status FROM pglogical.show_subscription_status() where subscription_name = 'test_subscription';");

   # pause program for 5 seconds
   sleep(5);

}
# reset subscription_status for future use.
$subscription_status = 'reset';
my $stdout;
$stdout = $node_subscriber->safe_psql('postgres',
        "SELECT subscription_name, status, provider_node, replication_sets, forward_origins FROM pglogical.show_subscription_status();");

print "Subscription status is : $stdout\n";
# print stdout
# Also verify that status is seen as 'replicating'

# Initialise pgbench on provider and print initial data count in tables
$node_provider->command_ok([ 'pgbench', '-i', '-s', '70'],
        'initialize pgbench');
my $account=$node_provider->safe_psql('postgres',"select count(*) from pgbench_accounts");
my $tellers=$node_provider->safe_psql('postgres',"select count(*) from pgbench_tellers");
my $history=$node_provider->safe_psql('postgres',"select count(*) from pgbench_history");

print "accounts count " . $account . "\n";
print "tellers count " . $tellers . "\n";
print "History count " . $history . "\n";

# pgbench tables are already created on provider
# Create empty tables on subscriber - with these DDLs

$node_subscriber->safe_psql('postgres',
        "CREATE TABLE pgbench_history (
    tid integer,
    bid integer,
    aid integer,
    delta integer,
    mtime timestamp without time zone,
    filler character(22)
);");

$node_subscriber->safe_psql('postgres',
        "CREATE TABLE pgbench_tellers (
    tid integer NOT NULL,
    bid integer,
    tbalance integer,
    filler character(84)
)
WITH (fillfactor='100');");
$node_subscriber->safe_psql('postgres',
        "CREATE TABLE pgbench_accounts (
    aid integer NOT NULL,
    bid integer,
    abalance integer,
    filler character(84)
)
WITH (fillfactor='100');");

$node_subscriber->safe_psql('postgres',
        "ALTER TABLE ONLY pgbench_tellers ADD CONSTRAINT pgbench_tellers_pkey PRIMARY KEY (tid);");
$node_subscriber->safe_psql('postgres',
        "ALTER TABLE ONLY pgbench_accounts ADD CONSTRAINT pgbench_accounts_pkey PRIMARY KEY (aid);");

# Make write-load active on the tables  pgbench_history
# with this TPC-B run

$node_provider->command_ok([ 'pgbench', '-T', '600', '-j', '2', '-s', '200', '-c', '10'],
        'activate pgbench on provider for 10 mins');

# allow sync of tables to happen - during writes
# by adding them to 'default'/default_insert_only replication_set

$node_provider->safe_psql('postgres',
        "SELECT * FROM pglogical.replication_set_add_table('default', 'pgbench_tellers', true);");
$node_provider->safe_psql('postgres',
        "SELECT * FROM pglogical.replication_set_add_table('default', 'pgbench_accounts', true);");
$node_provider->safe_psql('postgres',
        "SELECT * FROM pglogical.replication_set_add_table('default_insert_only', 'pgbench_history', true);");

# Keep polling and viewing results for this query till 10 minutes, after every 1 minute:
my $sync_status = 'reset';
my $counter = 0;
while($counter < 10 ){

   print("sync_status is $sync_status\n");

   # poll for subscription_status
   $sync_status = $node_subscriber->safe_psql('postgres',
        "SELECT sync_kind, sync_subid, sync_nspname, sync_relname, sync_status FROM pglogical.local_sync_status WHERE sync_relname IN ('pgbench_accounts','pgbench_tellers', 'pgbench_history');");

   # pause program for 60 seconds
   sleep(60);
   # increment counter
   $counter = $counter +1 ;
}

# Compare table entries on provider and subscriber.
my $rowcount = 0;
my $rowcount_accounts = $node_provider->safe_psql('postgres',
        "SELECT count(*) FROM pgbench_accounts;");
print "Row count for pgbench_acounts on provider is : $rowcount_accounts\n";

my $rowcount_tellers = $node_provider->safe_psql('postgres',
        "SELECT count(*) FROM pgbench_tellers;");
print "Row count for pgbench_tellers on provider is : $rowcount_tellers\n";

my $rowcount_history = $node_provider->safe_psql('postgres',
        "SELECT count(*) FROM pgbench_history;");
print "Row count for pgbench_history on provider is : $rowcount_history\n";

$rowcount = $node_subscriber->safe_psql('postgres',
        "SELECT count(*) FROM pgbench_accounts;");
print "Row count for pgbench_accounts on subscriber is : $rowcount\n";
is($rowcount,$rowcount_accounts,'Accounts table sync complete');

$rowcount = $node_subscriber->safe_psql('postgres',
        "SELECT count(*) FROM pgbench_tellers;");
print "Row count for pgbench_tellers on subscriber is : $rowcount\n";
is($rowcount,$rowcount_tellers,'tellerss table sync complete');

$rowcount = $node_subscriber->safe_psql('postgres',
        "SELECT count(*) FROM pgbench_history;");
print "Row count for pgbench_history on subscriber is : $rowcount\n";
is($rowcount,$rowcount_history,'History table sync complete');


#Teardown provider and subscriber nodes.
$node_subscriber->teardown_node;
$node_provider->teardown_node;
done_testing();
