# Helpers for postgres values

package PGValues;
use strict;
use warnings;
use v5.10.0;
use Exporter 'import';
use vars qw(@EXPORT @EXPORT_OK);
use Scalar::Util qw(reftype);

use Carp 'verbose';
$SIG{__DIE__} = \&Carp::confess;

@EXPORT = qw(
	quote_literal
	quote_ident
	to_pg_literal
	to_pg_named_arglist
	append_pg_named_arglist
	);

@EXPORT_OK = qw();
sub quote_literal {
	my $l = shift;
	return "NULL"
		if !defined($l);
	$l =~ s/'/''/g;
	return "'" . $l . "'";
}

sub quote_ident {
	my $i = shift;
	die "identifiers may not be NULL/undef"
		if !defined($i);
	$i =~ s/"/""/g;
	return '"' . $i . '"';
}

sub quote_array_elem {
	my $e = shift;
	return "NULL"
		if !defined($e);
	$e =~ s/"/\"/g;
	return '"' . $e . '"';
}

sub to_pg_literal {
	my $perlval = shift;
	if (!defined($perlval) || !defined(reftype($perlval))) {
		return quote_literal($perlval);
	}
	if (reftype($perlval) eq 'SCALAR') {
		return quote_literal($$perlval);
	}
	if (reftype($perlval) eq 'ARRAY') {
		# It's OK if the value is intended to be an integer, we can still quote
		# everything; we'll implicitly cast from unknown-array to int[]
		# happily.
		#
		# Because Pg can't determine the type of an empty array, though, we
		# use the oldstyle array literals, not an ARRAY[] constructor.
		#
		# No attempt is made to support n-dim arrays.
		#
		return "'{" . join(',', (map {quote_array_elem($_)} @$perlval)) . "}'";
	}
	if (reftype($perlval) eq 'HASH') {
		die("don't know how to turn hash into postgres literal");
	}
	die("unreognised ref type " . reftype($perlval) . " for '" . $perlval . "'");
}

sub to_pg_named_arglist {
	my $args = shift;
	
	die "ref must be hash"
		unless reftype($args) eq 'HASH';

	my @x;

	while (my ($k,$v) = each %$args)
	{
		push @x, quote_ident($k) . " := " . to_pg_literal($v);
	}

	return join ", ", @x;
}

# Append a keyword list to a func only if the list is non-empty
sub append_pg_named_arglist {
	my $args = shift;

	if (defined $args && scalar(%$args))
	{
		return ", " . join(to_pg_named_arglist($args));
	}
	else
	{
		return "";
	}
}

1;
