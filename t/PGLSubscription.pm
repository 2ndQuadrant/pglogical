# Model of a pglogical subscription

use strict;
use warnings;
use v5.10.0;
package PGLSubscription;
use PostgresPGLNode;
use PGLDB;
use PGValues qw(
	quote_ident
	quote_literal
	to_pg_literal
	to_pg_named_arglist
	append_pg_named_arglist
	);
use Exporter 'import';
use vars qw(@EXPORT @EXPORT_OK);
use Scalar::Util qw(reftype);
use Data::Dumper;
use Test::More; #XXX

my $trace_sql = $ENV{TRACE_SQL} // 0;

use Carp 'verbose';
$SIG{__DIE__} = \&Carp::confess;

@EXPORT = qw();
@EXPORT_OK = qw();


# Prepare to create a subscription. The DB objects
# are created by ->init
sub new {
	my ($class, %kwargs) = @_;

	die 'must supply db to create subscription on'
		unless defined $kwargs{from} && $kwargs{from}->isa('PGLDB');

	die 'need subscription name'
		unless defined $kwargs{name};

	my $self = bless {
		'_subscriberdb' => $kwargs{from},
		'_name' => $kwargs{name},
	}, $class;

	return $self;
}

# Create a pglogical subscription
# 
# kwargs may be supplied to set extra options for pglogical.create_subscription(...)
#
sub create {
	my ($self, $provider, %kwargs) = @_;

	die 'provider must be a PGLDB'
		unless defined $provider && $provider->isa('PGLDB');

	if ($self->subscriberdb->node->pg_version_num / 100 == 904) {
		$self->psql('CREATE EXTENSION IF NOT EXISTS pglogical_origin');
	}
	my $query = "SELECT * FROM pglogical.create_subscription("
		. "subscription_name := " . quote_literal($self->name) . ", "
		. "provider_dsn := " . quote_literal($provider->connstr) . ", "
		. to_pg_named_arglist(\%kwargs)
		. ");";
	printf("subscribing [%s] to [%s] as [%s]\n", $self->subscriberdb->name, $provider->name, $self->name);
	print("Joining with query:\n$query\n");
	$self->subscriberdb->safe_psql($query);

	$self->{_providerdb} = $provider;
	$self->{_providername} = $provider->name;
}

# pglogical_create_subscriber created one or more subscriptions. Figure out
# the provider. We don't have any way to discover the provider object
# (no attempt is made to maintain a registry of them) but we can look up
# the name...
sub _init_from_physical {
	my $self = shift;

	$self->{_providername} = $self->subscription_status->{'provider_node'};
	die 'unable to determine provider name for subscriber ' . $self->name . ' after physical clone'
		unless defined $self->{_providername};

	# $self->{_providerdb} is intentionally not defined
}

sub wait_for_replicating {
	my $self = shift;
	print "waiting for [" . $self->subscriberdb->name . "] sub [" . $self->name . "] to start replicating ...";
	if (! $self->subscriberdb->poll_query_until("SELECT status = 'replicating' FROM pglogical.show_subscription_status(" . quote_literal($self->name) . ")"))
	{
		say "FAILED!";
		return 0;
	}
	say " done";
	return 1;
}

sub _format_qualified_table_list {
	my ($self, @tables) = @_;
	return 'VALUES (' . (join "), (", (map { quote_literal(quote_ident($_->[0]) . "." . quote_ident($_->[1])) } @tables)) . ")";
}

# Wait for a peer to be connected and have replayed past the lsn
# on its upstream at the time of this call, or the passed lsn if
# supplied.
#
sub wait_for_catchup {
	my ($self, $upstream_pub, $lsn) = @_;
	my $funcname = "pg_current_xlog_location";
	my $colname = "replay_location";
	if ($upstream_pub->node->pg_version_num / 100 >= 1000) {
		$funcname = "pg_current_wal_lsn";
		$colname = "replay_lsn";
	}
	$lsn = $upstream_pub->safe_psql("SELECT $funcname()")
		unless defined $lsn;
	# get subscriber slot name, which we can't assume is the same as subscriber
	# name.
	my $slotname = $self->subscriberdb->safe_psql('SELECT slot_name FROM pglogical.show_subscription_status(' . quote_literal($self->name) . ")");
	# TODO should sanity test lookup to make sure our expected publisher id = the one on the other node
	print "waiting for sub " . $self->name
		. " on pgl node " . $self->subscriberdb->name
		. " on instance " . $self->subscriberdb->node->name
		. " to replicate up to or past " . $lsn
		. " on slot " . $slotname
		. " from pgl node " . $upstream_pub->name
		. " on instance " . $upstream_pub->node->name;
	# We assume the downstream is connected so we're not going to bother with
	# the slot position, we just use it to find the right pg_stat_replication
	# entry on the upstream.
	if ($upstream_pub->node->pg_version_num / 100 < 905) {
		# on 9.4 we probably need to use application_name or something
		die 'need pid field in pg_replication_slots to';
	}
	my $ret = $upstream_pub->poll_query_until("SELECT $colname >= " . quote_literal($lsn) . '::pg_lsn FROM pg_replication_slots s INNER JOIN pg_stat_replication r ON (s.active_pid = r.pid) WHERE s.slot_name = ' . quote_literal($slotname));
	say " done";
	return $ret;
}

# Wait for the listed tables to sync up
#
# Tables are passed as 2-tuples in arrayref form [schemaname,tblname]
#
# The tables must already exist locally, so if they're created by ddl replication
# make sure you wait for the subscription to catch up first.
#
# Tables that are part of the initial data copy will never be counted as synced
# sine pglogical doesn't make local_sync_status entries for them
# (see 2ndQuadrant/pglogical_internal#134). They'll report 'unknown'
# and this test will time out then fail.
#
sub wait_for_table_sync {
	my ($self, @tables) = @_;
	print "waiting for [" . $self->subscriberdb->name . "] sub [" . $self->name . "] tables to finish syncing ...";

	$self->subscriberdb->poll_query_until(
		q[SELECT 'synchronized' = ALL (
			SELECT status
			FROM (] . $self->_format_qualified_table_list(@tables) . q[) table_list(table_qualname)
			CROSS JOIN LATERAL pglogical.show_subscription_table(] . quote_literal($self->name) . q[, table_qualname::regclass)
		  );])
		or die 'timed out, sync status is ' . $self->table_sync_status(@tables);

	return 1;
}

sub subscription_status {
	my $self = shift;
	my @fields = ('subscription_name', 'status', 'provider_node', 'replication_sets', 'forward_origins');
	my $ret = $self->subscriberdb->safe_psql('SELECT ' . join(", ", (map {quote_ident($_)} @fields)) . ' FROM pglogical.show_subscription_status(' . quote_literal($self->name) . ') ORDER BY 1');
	my %status;
	@status{@fields} = split(/\|/,$ret);
	return \%status;
}

sub table_sync_status {
	my ($self, @tables) = @_;
	my @fields = ('nspname', 'relname', 'status');
	my $sql = 'SELECT ' . join(", ", (map {quote_ident($_)} @fields)) . ' FROM (';
	if (@tables) {
		$sql .= $self->_format_qualified_table_list(@tables);
	}
	else {
		# this would be racey anyway since we'd only check ones we know about
		# but we could just query pglogical.TABLES and form quoted identifiers
		die "don't support checking all subscribed tables yet, pass a table list";
	}
	$sql .= ") table_list(table_qualname) CROSS JOIN LATERAL pglogical.show_subscription_table(" . quote_literal($self->name) . ", table_qualname::regclass) ORDER BY 1, 2, 3";
	my $ret = $self->subscriberdb->safe_psql($sql);
	my @lines;
	foreach my $line (split(/\n/, $ret)) {
		my %status;
		@status{@fields} = split(/\|/,$line);
		push @lines, \%status;
	}
	return \@lines;
}

sub name {
	my $self = shift;
	return $self->{'_name'};
}

sub subscriberdb {
	my $self = shift;
	return $self->{_subscriberdb};
}

sub providername {
	my $self = shift;
	return $self->{_providername};
}

sub providerdb {
	my $self = shift;
	die 'provider PGLDB unknown, created from pglogical_create_subscription? Look it up by name.'
		unless defined $self->{_providerdb};
	return $self->{_providerdb};
}

1;
