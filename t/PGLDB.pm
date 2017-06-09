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
	my ($class, %kwargs) = @_;

	die 'need node arg to be a PostgresPGLNode'
		unless defined $kwargs{node} && $kwargs{node}->isa('PostgresPGLNode');

	die 'need dbname arg'
		unless defined $kwargs{dbname};

	die 'need name arg'
		unless defined $kwargs{name};

	my $self = bless {
		'_node' => $kwargs{node},
		'_dbname' => $kwargs{dbname},
		'_name' => $kwargs{name},
	}, $class;

	return $self;
}

# SQL creation of a pglogical node
sub create {
	my ($self, %kwargs) = shift;
	my $exists = $self->node->safe_psql('postgres', 'SELECT 1 FROM pg_catalog.pg_database WHERE datname = ' . quote_literal($self->dbname));
	if ($exists eq '')
	{
		$self->node->safe_psql('postgres', 'CREATE DATABASE ' . quote_ident($self->dbname));
	}
	$self->psql('CREATE EXTENSION IF NOT EXISTS pglogical');
	# TODO: support options to pglogical.create_node
	$self->_create_node();
}

# We were created by pglogical_create_subscription
sub _init_from_physical {
	my $self = shift;
	# nothing to do for now
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

sub replication_set_add_all_tables {
	my ($self, $setname, $schemas, $sync, %kwargs) = @_;
	$self->safe_psql("SELECT * FROM pglogical.replication_set_add_all_tables(" . quote_literal($setname) . ", " . to_pg_literal($schemas) . ", " . quote_literal($sync) 
		. append_pg_named_arglist(%kwargs)
		. ")");
}

sub replication_set_add_sequence {
	my ($self, $setname, $sequencename, $sync, %kwargs) = @_;
	$self->safe_psql("SELECT * FROM pglogical.replication_set_add_sequence(" . quote_literal($setname) . ", " . quote_literal($sequencename) . ", " . quote_literal($sync) 
		. append_pg_named_arglist(%kwargs)
		. ")");
}

sub replication_set_remove_sequence {
	my ($self, $setname, $sequencename) = @_;
	$self->safe_psql("SELECT * FROM pglogical.replication_set_remove_sequence(" . quote_literal($setname) . ", " . quote_literal($sequencename) . ")");
}

sub replication_set_add_all_sequences {
	my ($self, $setname, $schemas, $sync, %kwargs) = @_;
	$self->safe_psql("SELECT * FROM pglogical.replication_set_add_all_sequences(" . quote_literal($setname) . ", " . to_pg_literal($schemas) . ", " . quote_literal($sync) 
		. append_pg_named_arglist(%kwargs)
		. ")");
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
