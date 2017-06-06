#!/usr/bin/env perl
#
# This class extends PostgresNode with pglogical-specific routines
# for setup.
#
# Use PGLDB.pm to create a pglogical node (pglogical-enabled database)
# bound to this postgres instance.
#

package PostgresPGLNode;
use base ("PostgresNode");
use strict;
use warnings;
use v5.10.0;
use Exporter 'import';
use vars qw(@EXPORT @EXPORT_OK);
use Scalar::Util qw(looks_like_number reftype);
use File::Temp ();
use File::Copy ();
use File::Basename;
use Data::Dumper;

use Carp 'verbose';
$SIG{__DIE__} = \&Carp::confess;

@EXPORT = qw(
	get_new_pgl_node
	);

@EXPORT_OK = qw();

my $_pg_version;
my $_pg_major_version;
my $_pg_minor_version;
my $_pg_version_num;

# get a new pglogical node, ready to init (initdb),
# restore from basebackup, or create with
# pglogical_create_subscriber
sub get_new_pgl_node
{
	my $name  = shift;

	pg_version_num();

	# We should just be able to pass get_new_node a class name
	# to instantiate, but nobody thought of that when they implemented
	# the factory. So instead we have to re-bless it after we get a copy.
	#
	# This is OK because it doesn't do anything exciting during creation.
	#
	my $self = PostgresNode::get_new_node($name);
	$self = bless $self, 'PostgresPGLNode';
	return $self;
}

# normal initdb as a new master
sub init {
	my ($self, %kwargs) = @_;
	# More sensible defaults for logical rep
	$kwargs{hba_permit_replication} = 1 unless defined $kwargs{hba_permit_replication};
	$kwargs{allows_streaming} = 1 unless defined $kwargs{allows_streaming};
	# shortcut to set some postgresql.conf options
	$kwargs{max_wal_senders} = 10 unless defined $kwargs{max_wal_senders};
	$kwargs{max_replication_slots} = 10 unless defined $kwargs{max_replication_slots};
	$kwargs{max_connections} = 20 unless defined $kwargs{max_connections};
	$kwargs{track_commit_timestamp} = 'on' unless defined $kwargs{track_commit_timestamp};
	die "max_wal_senders <= max_connections"
		unless $kwargs{max_connections} > $kwargs{max_wal_senders};
	$self->SUPER::init(%kwargs);
	# and some sensible config
	$self->append_conf('postgresql.conf', qq[
shared_preload_libraries = 'pglogical'
wal_level = logical
max_wal_senders = $kwargs{max_wal_senders}
max_replication_slots = $kwargs{max_replication_slots}
max_connections = $kwargs{max_connections}

DateStyle = 'ISO, DMY'
log_line_prefix='[%m] [%p] [%d] '
fsync=off

pglogical.synchronous_commit = true
]);

	if ($self->pg_version >= 90500)
	{
		$self->append_conf('postgresql.conf', qq[track_commit_timestamp = $kwargs{track_commit_timestamp}\n]);
	}
} 

# 
# Physical clone of an existing master with pglogical_init_copy, setting up one
# or more databases.
#
# To clone multiple DBs at once, specify all the dbnames (including those
# of the passed publisher) e.g.
#
#    dbnames => [$pub->dbname, 'otherdbname', 'extra']
#
sub init_physical_clone
{
	my ($self, %params) = @_;

	die "streaming rep not supported" if $params{has_streaming};
	die "archive restore rep not supported" if $params{has_restoring};
	die "publisher required" unless defined($params{publisher});
	die "subscriber_name required" unless defined($params{subscriber_name});

	my ($publisher_node, @dbnames, $publisher_dsn, $subscriber_dsn);
	if ($params{publisher}->isa('PostgresPGLNode')) {
		$publisher_node = $params{publisher};
		die "dbnames => [...] param must be supplied if passing a PostgresPGLNode"
			unless defined($params{dbnames}) && reftype($params{dbnames}) eq 'ARRAY';
		@dbnames = @{$params{dbnames}};
		$publisher_dsn = $publisher_node->connstr;
		$subscriber_dsn = $self->connstr;
	}
	elsif ($params{publisher}->isa('PGLDB')) {
		$publisher_node = $params{publisher}->node;
		@dbnames[0] = $params{publisher}->dbname;
		$publisher_dsn = $publisher_node->connstr($params{publisher}->dbname);
		# physical copy, so upstream and downstream dbnames are the same
		$subscriber_dsn = $self->connstr($params{publisher}->dbname);
	}
	else {
		die "publisher must be a PostgresPGLNode or PGLDB instance";
	}


	my $testname = basename($0);
	$testname =~ s/\.[^.]+$//;
	# Use the same log as Pg its self, since pglogical_create_subscriber leaves
	# the server running afterwards.
	my $logfile = $TestLib::log_path . "/" . ${testname} . "_" . $self->name . ".log";

	print "# Initializing node \"" . $self->name . "\" (subscriber name \"" . $params{subscriber_name} . "\") with pglogical_create_subscriber from node \"" . $publisher_node->name . "\", logs to $logfile\n";

	mkdir $self->backup_dir;

	my $data_path = $self->data_dir;
	rmdir($data_path);

	my $backup_path = File::Temp::tempdir(
		DIR => $self->backup_dir,
		TEMPLATE => 'pgl_create_sub_XXXXXX'
	);

	# we need to create a new postgresql.conf with the new port
	my ($fh, $temp_pg_conf) = File::Temp::tempfile(
		DIR => $self->backup_dir,
		TEMPLATE => 'pgl_pg_conf_XXXXXX'
	);
	File::Copy::copy($publisher_node->data_dir . "/postgresql.conf", $fh);
	print $fh "port = " . $self->port . "\n";
	say "templated postgresql.conf to $temp_pg_conf";

	my @cmd = (
		'pglogical_create_subscriber', '-D', $self->data_dir,
		'--subscriber-name=' . $params{subscriber_name},
		'--subscriber-dsn=' . $subscriber_dsn,
		'--provider-dsn=' . $publisher_dsn,
		'--drop-slot-if-exists',
		'-v',
		'--hba-conf=' . $publisher_node->data_dir . "/pg_hba.conf",
		'--postgresql-conf=' . $temp_pg_conf
	);

	push @cmd, ('--dbnames=' . join(',', @{$params{dbnames}}))
		if (defined($params{dbnames}));

	say "running: " . Dumper(\@cmd);

	IPC::Run::run \@cmd,
		'>', $logfile, '2>', $logfile
		or die("pglogical_create_subscriber failed: $!");

	# The server should be running, so probe for its pid to maintain
	# PostgresNode's state
	$self->_update_pid(1);

	# For every subscription created, return a PGLDB instance.
	my @subscriptions;
	foreach my $dbname (@dbnames) {
		push @subscriptions, PGLDB->new($self, $dbname, $params{subscriber_name});
	}

	return \@subscriptions;
}

sub start {
	my $self = shift;
	my @args = @_;
	$self->SUPER::start(@args);
}

#
# Why, oh why, don't we have pg_config --version-num
#
sub pg_version {
	if (!defined $_pg_major_version)
	{
		my ($stdout, $stderr);
		my $result = IPC::Run::run [ 'pg_config', '--version' ], '>', \$stdout, '2>',
		  \$stderr;
		# versions can look like 9.6beta1, 9.6.4, 10beta1, 10.1
		my ($ver, $major, $minor_or_tag) = ($stdout =~ qr/(([89]\.[0-9]{1,2}|[0-9]{2})(\.[0-9]+|[a-z]+[0-9]*))/);
		die "could not parse version string $stdout for postgres version"
			unless $major;
		$minor_or_tag = substr($minor_or_tag, 1) if (substr($minor_or_tag, 0, 1) eq '');
		$_pg_version = $ver;
		$_pg_major_version = $major;
		$_pg_minor_version = $minor_or_tag 
	}

	return $_pg_major_version;
}

sub pg_version_num {
	if (!defined $_pg_version_num)
	{
		pg_version();
		my ($major1, $major2) = ($_pg_major_version =~ qr/([0-9]+)\.?([0-9]*)/);

		die "cannot parse postgres high-major version '$_pg_major_version'; got " . ($major1//'undef')
			unless defined($major1) && looks_like_number($major1);

		$major2 = 0 unless defined($major2) && looks_like_number($major2);
		my $minor = $_pg_minor_version;
		$minor = 0 unless defined($minor) && looks_like_number($minor);

		$_pg_version_num = int($major1) * 10000 + int($major2) * 100 + int($minor);

		print "detected Pg version $_pg_version (major $_pg_major_version, minor $_pg_minor_version, numeric $_pg_version_num)\n";
	}

	return $_pg_version_num;
}

1;
