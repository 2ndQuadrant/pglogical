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
	$self->subscriberdb->poll_query_until("SELECT status = 'replicating' FROM pglogical.show_subscription_status(" . quote_literal($self->name) . ")");
	say " done";
	return 1;
}

sub wait_for_sync {
	# It'd be nice to accept a list of tables here, but meh for now
	my $self = shift;
	print "waiting for [" . $self->subscriberdb->name . "] sub [" . $self->name . "] to sync up ...";
	$self->subscriberdb->poll_query_until("SELECT 'r' = ALL (SELECT sync_status FROM pglogical.local_sync_status INNER JOIN pglogical.subscription ON (sync_subid = sub_id) WHERE sub_name = " . quote_literal($self->name) . ")");
	say " done";
	return 1;
}

sub subscription_status {
	my $self = shift;
	my @fields = ('subscription_name', 'status', 'provider_node', 'replication_sets', 'forward_origins');
	my $ret = $self->subscriberdb->safe_psql('SELECT ' . join(", ", (map {quote_ident($_)} @fields)) . ' FROM pglogical.show_subscription_status(' . quote_literal($self->name) . ')');
	my %status;
	@status{@fields} = split(/\|/,$ret);
	return \%status;
}

sub sync_status {
	my $self = shift;
	my @fields = ('sync_kind', 'sync_subid', 'sync_nspname', 'sync_relname', 'sync_status');
	my $sql = 'SELECT ' . join(", ", (map {quote_ident($_)} @fields)) . ' FROM pglogical.local_sync_status INNER JOIN pglogical.subscription ON (sync_subid = sub_id) WHERE sub_name = ' . quote_literal($self->name) . ' ORDER BY 2, 3, 4;';
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
