#!perl -T

use Test::More tests => 1;

BEGIN {
	use_ok( 'DBIx::Declare' );
}

diag( "Testing DBIx::Declare $DBIx::Declare::VERSION, Perl $], $^X" );
