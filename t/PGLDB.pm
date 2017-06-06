# Model of a pglogical node on a database

use strict;
use warnings;
use v5.10.0;
package PGLDB;
use PostgresPGLNode;
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

sub new {
	my ($class, $pglnode, $dbname, $nodename) = @_;

	die 'need PostgresPGLNode as 1st arg'
		unless defined $pglnode && $pglnode->isa('PostgresPGLNode');

	die 'need dbname as 2nd arg'
		unless defined $dbname;

	die 'need node name as 3rd arg'
		unless defined $nodename;

	my $self = bless {
		'_node' => $pglnode,
		'_dbname' => $dbname,
		'_name' => $nodename,
	}, $class;

	return $self;
}

sub init {
	my $self = shift;
	my $exists = $self->node->safe_psql('postgres', 'SELECT 1 FROM pg_catalog.pg_database WHERE datname = ' . quote_literal($self->dbname));
	if ($exists eq '')
	{
		$self->node->safe_psql('postgres', 'CREATE DATABASE ' . quote_ident($self->dbname));
	}
	$self->psql('CREATE EXTENSION IF NOT EXISTS pglogical');
	$self->_create_node();
}

sub _create_node {
	my $self = shift;
	$self->safe_psql("SELECT * FROM pglogical.create_node("
		. "node_name := " . quote_literal($self->name) . ", "
		. "dsn := " . quote_literal($self->connstr) . ")");
}

sub create_replication_set {
	my ($self, $setname, %kwargs) = @_;
	die "set name must be supplied"
		unless defined $setname;
	$self->safe_psql("SELECT * FROM pglogical.create_replication_set(" . quote_literal($setname)
		. append_pg_named_arglist(%kwargs)
		. ")");
}

sub replication_set_add_table {
	my ($self, $setname, $tablename, $sync, %kwargs) = @_;
	$self->safe_psql("SELECT * FROM pglogical.replication_set_add_table(" . quote_literal($setname) . ", " . quote_literal($tablename) . ", " . quote_literal($sync) 
		. append_pg_named_arglist(%kwargs)
		. ")");
}

sub replication_set_remove_table {
	my ($self, $setname, $tablename) = @_;
	$self->safe_psql("SELECT * FROM pglogical.replication_set_remove_table(" . quote_literal($setname) . ", " . quote_literal($tablename) . ")");
}

sub create_subscription {
	my ($self, $provider, $subname, %kwargs) = @_;
	die "pass a PGLDB as 1st arg, not a dsn"
		if defined $kwargs{'provider_dsn'};
	die "subscriber name should be supplied as 2nd non-named argument"
		if defined $kwargs{'subscription_name'};
	die "provider must be a PGLDB instance"
		if !$provider->isa('PGLDB');
	if ($self->node->pg_version_num / 100 == 904) {
		$self->psql('CREATE EXTENSION IF NOT EXISTS pglogical_origin');
	}
	my $query = "SELECT * FROM pglogical.create_subscription("
		. "subscription_name := " . quote_literal($subname) . ", "
		. "provider_dsn := " . quote_literal($provider->connstr) . ", "
		. to_pg_named_arglist(\%kwargs)
		. ");";
	printf("subscribing [%s] to [%s]\n", $self->name, $provider->name);
	print("Joining with query:\n$query\n");
	$self->safe_psql($query);
}

sub wait_for_replicating {
	my ($self, $subname) = @_;
	die 'subname must be defined'
		unless defined($subname);
	say "waiting for [" . $self->name . "] sub [" . $subname . "] to start replicating";
	$self->node->poll_query_until($self->dbname, "SELECT status = 'replicating' FROM pglogical.show_subscription_status(" . quote_literal($subname) . ")");
	say "done waiting for [" . $self->name . "] sub [" . $subname . "] to start replicating";
	return 1;
}

sub wait_for_sync {
	# It'd be nice to accept a list of tables here, but meh for now
	my ($self,$subname) = @_;
	die 'subname must be defined'
		unless defined($subname);
	say "waiting for [" . $self->name . "] sub [" . $subname . "] to sync up";
	$self->node->poll_query_until($self->dbname, "SELECT 'r' = ALL (SELECT sync_status FROM pglogical.local_sync_status INNER JOIN pglogical.subscription ON (sync_subid = sub_id) WHERE sub_name = " . quote_literal($subname) . ")");
	say "done waiting for [" . $self->name . "] sub [" . $subname . "] to sync up";
	return 1;
}

sub subscription_status {
	my ($self,$subname) = @_;
	die 'subname must be defined'
		unless defined($subname);
	my @fields = ('subscription_name', 'status', 'provider_node', 'replication_sets', 'forward_origins');
	my $ret = $self->safe_psql('SELECT ' . join(", ", (map {quote_ident($_)} @fields)) . ' FROM pglogical.show_subscription_status(' . quote_literal($subname) . ')');
	my %status;
	@status{@fields} = split(/\|/,$ret);
	return \%status;
}

sub sync_status {
	my ($self,$subname) = @_;
	die 'subname must be defined'
		unless defined($subname);
	my @fields = ('sync_kind', 'sync_subid', 'sync_nspname', 'sync_relname', 'sync_status');
	my $sql = 'SELECT ' . join(", ", (map {quote_ident($_)} @fields)) . ' FROM pglogical.local_sync_status INNER JOIN pglogical.subscription ON (sync_subid = sub_id) WHERE sub_name = ' . quote_literal($subname) . ' ORDER BY 2, 3, 4;';
	my $ret = $self->safe_psql($sql);
	my @lines;
	foreach my $line (split(/\n/, $ret)) {
		my %status;
		@status{@fields} = split(/\|/,$line);
		push @lines, \%status;
	}
	return \@lines;
}

sub replicate_ddl {
	my ($self, $ddl, $repsets) = @_;
	die 'repsets must be arrayref or undef'
		unless !defined($repsets) || reftype($repsets) eq 'ARRAY';
	my $repsetssql = '';
	if (defined($repsets)) {
		$repsetssql .= ", " . to_pg_literal($repsets);
	}
	$self->safe_psql(qq[SELECT pglogical.replicate_ddl_command(\$DDL\$\n$ddl\n\$DDL\$$repsetssql);]);
}

sub name {
	my $self = shift;
	return $self->{'_name'};
}

sub node {
	my $self = shift;
	return $self->{'_node'};
}

sub dbname {
	my $self = shift;
	return $self->{'_dbname'};
}

sub connstr {
	my $self = shift;
	die 'cannot pass dbname to PGLDB::connstr'
		if defined $_[1];
	return $self->node->connstr($self->dbname);
}

sub safe_psql {
	my $self = shift;
	if ($trace_sql) {
		say "SQL: " . $self->name . ": safe_psql(" . Dumper(\@_) . ")";
	}
	return $self->node->safe_psql($self->dbname, @_);
}

sub psql {
	my $self = shift;
	return $self->node->safe_psql($self->dbname, @_);
}

sub poll_query_until {
	my $self = shift;
	return $self->node->poll_query_until($self->dbname, @_);
}

1;
