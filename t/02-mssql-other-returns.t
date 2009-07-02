#!perl -T
use strict;
use Test::More tests => 33;
use Data::Dumper;
no warnings 'uninitialized';

SKIP: {
	{
		my $drh = eval{DBI->install_driver("ODBC")};
		diag($@) if $@;
		skip ": DBD::ODBC not available", 25 unless $drh;
	}

	{
		my $dbh = DBI->connect('dbi:ODBC:Driver=SQL Server;Server=localhost;Database=master');
		unless ($dbh) {
			diag($DBI::errstr);
			skip ": local MSSQL server connection failed", 25 ;
		}
	}

use DBIx::Declare
	MyDB => {
#	code_cache => 'W:\jenda\packages\DBIx\Declare\t\cache',
	data_source  => "dbi:ODBC:Driver=SQL Server;Server=localhost;Database=master",
	type => 'mssql',
	attr => { 'RaiseError' => 0, PrintError => 0, 'AutoCommit' => 1 },
	on_errors => 'croak',
	methods => {
		'CreateDatabase' => {
			sql => 'CREATE DATABASE ? COLLATE SQL_Latin1_General_Cp1_CI_AS',
			args => ['name'],
			return => '$',
			noprepare => 1,
			noquote => 1,
		},
		'UseDatabase' => {
			sql => 'USE ?',
			args => ['name'],
			return => '$',
			noprepare => 1,
			noquote => 1,
		},
		'DropDatabase' => {
			sql => 'DROP DATABASE ?',
			args => ['name'],
			return => '$',
			noprepare => 1,
			noquote => 1,
		},
		'CreateTableUsers' => {
			sql => 'CREATE TABLE Users (Id int not NULL PRIMARY KEY IDENTITY(1,1), FirstName varchar(50) NOT NULL, LastName varchar(50) NOT NULL, Email varchar(100))',
			args => [],
			return => '$',
			noprepare => 1,
			#noquote => 1, # pointless with no arguments
		},
		'InsertUser' => {
			sql => 'INSERT INTO Users (FirstName,LastName,Email) VALUES (?,?,?)',
			args => [qw(FirstName LastName Email)],
			defaults => {
				Email => undef,
			},
			return => '++',
		},
		'InsertUser2' => {
			sql => 'INSERT INTO Users (FirstName,LastName,Email) VALUES (?,?,?); SELECT SCOPE_IDENTITY() as Id',
			args => [qw(FirstName LastName Email)],
			defaults => {
				Email => undef,
			},
			return => '_$',
		},
		FetchUserDetails => {
			sql => 'SELECT * FROM Users WHERE Id = ?',
			args => ['Id'],
			return => '_@',
		},
		'FetchUsersA,FetchUsersHNDL,FetchUsersSA,FetchUsersSRA,FetchUsersSH,FetchUsersSRH,
		 FetchUsersOA,FetchUsersORA,FetchUsersOH,FetchUsersORH,' => {
			sql => 'SELECT * FROM Users ORDER BY LastName, FirstName',
			args => [],
		},
		FetchUsersA => {return => '@'},
		FetchUsersHNDL => {return => '<>'},
		FetchUsersSA => {return => '&@', sub => sub {my ($id,$fn,$ln,$e) = @_; return "$id: $fn $ln <$e>" }},
		FetchUsersSRA => {return => '&\@', sub => sub {my ($a) = @_; return "$a->[0]: $a->[1] $a->[2] <$a->[3]>"}},
		FetchUsersSH => {return => '&%', sub => sub {my %h = @_; return "$h{Id}: $h{FirstName} $h{LastName} <$h{Email}>"}},
		FetchUsersSRH => {return => '&\%', sub => sub {my ($h) = @_; return "$h->{Id}: $h->{FirstName} $h->{LastName} <$h->{Email}>"}},
		FetchUsersOA => {return => '.@', class => 'ClsA'},
		FetchUsersORA => {return => '.\@', class => 'ClsRA'},
		FetchUsersOH => {return => '.%', class => 'ClsH'},
		FetchUsersORH => {return => '.\%', class => 'ClsRH'},
	},
};

#diag( "Testing DBIx::Declare $DBIx::Declare::VERSION, Perl $], $^X" );

my $db = MyDB->new() or die "failed to create the object";

ok(
$db->CreateDatabase( 'DBIx_Declare_Test')
and
$db->UseDatabase( 'DBIx_Declare_Test')
and
$db->CreateTableUsers()
, "set up the database and table");

is( $db->InsertUser('John', 'Doe', 'John.Doe@hotmail.com'), 1, "Inserted row ID");

is( $db->InsertUser(-FirstName => 'Jane', -LastName => 'Doe', -Email => 'Jane.Doe@hotmail.com'), 2, "Inserted row ID with named params");


is ( $db->InsertUser2('Ken', 'Plastic'), 3, "Inserted row including the SCOPE_IDENTITY()");
$db->InsertUser(-FirstName => 'Barbie', -LastName => 'Blond');
$db->InsertUser(-LastName => 'McDowel', -FirstName => 'Corrie');

is_deeply( scalar($db->FetchUserDetails(2)), ['2','Jane','Doe','Jane.Doe@hotmail.com'], "Fetch user details in scalar context");
is_deeply( scalar($db->FetchUserDetails(-Id => 3)), ['3','Ken','Plastic',undef], "Fetch user details in scalar context with named args wo Email");

{
	my $got = $db->FetchUsersA();
	my $good = [
		'4','Barbie','Blond',undef,
		'2','Jane','Doe','Jane.Doe@hotmail.com',
		'1','John','Doe','John.Doe@hotmail.com',
		'5','Corrie','McDowel',undef,
		'3','Ken','Plastic',undef
	];
	is_deeply($got, $good, "Fetch as array in scalar context");

	my @got = $db->FetchUsersA();
	is_deeply( \@got, $good, "Fetch as array of arrays in list context");
}

{
	my $sth = $db->FetchUsersHNDL();
	my $good = [
		['4','Barbie','Blond',undef],
		['2','Jane','Doe','Jane.Doe@hotmail.com'],
		['1','John','Doe','John.Doe@hotmail.com'],
		['5','Corrie','McDowel',undef],
		['3','Ken','Plastic',undef]
	];
	ok( $sth, "Fetch returning the statement" );

	while (my $row = $sth->fetchrow_arrayref) {
		is_deeply( $row, shift(@$good), "Fetch returning the statement (rows)" );
	}
}

{
	my $good = [
		'4: Barbie Blond <>',
		'2: Jane Doe <Jane.Doe@hotmail.com>',
		'1: John Doe <John.Doe@hotmail.com>',
		'5: Corrie McDowel <>',
		'3: Ken Plastic <>',
	];
	is_deeply( scalar($db->FetchUsersSA()), $good, "Fetch via a subroutine (ARRAY)" );
	my @got = $db->FetchUsersSA();
	is_deeply( \@got, $good, "Fetch via a subroutine (ARRAY) list context" );

	is_deeply( scalar($db->FetchUsersSRA()), $good, "Fetch via a subroutine (ARRAYREF)" );
	@got = $db->FetchUsersSRA();
	is_deeply( \@got, $good, "Fetch via a subroutine (ARRAYREF) list context" );

	is_deeply( scalar($db->FetchUsersSH()), $good, "Fetch via a subroutine (HASH)" );
	@got = $db->FetchUsersSH();
	is_deeply( \@got, $good, "Fetch via a subroutine (HASH) list context" );

	is_deeply( scalar($db->FetchUsersSRH()), $good, "Fetch via a subroutine (HASHREF)" );
	@got = $db->FetchUsersSRH();
	is_deeply( \@got, $good, "Fetch via a subroutine (HASHREF) list context" );
}

{
	my @good = (
		ClsA->new( 4, 'Barbie', 'Blond'),
		ClsRA->new( [2, 'Jane', 'Doe',  'Jane.Doe@hotmail.com']),
		ClsH->new( Id => 1, FirstName => 'John', LastName => 'Doe', Email => 'John.Doe@hotmail.com'),
		ClsRH->new( {Id => 5, FirstName => 'Corrie', LastName => 'McDowel', Email => undef} ),
		ClsA->new( 3, 'Ken', 'Plastic', undef),
	);

	is_deeply(
		[map $_->toString, @good],
		[
		'4: Barbie Blond <>',
		'2: Jane Doe <Jane.Doe@hotmail.com>',
		'1: John Doe <John.Doe@hotmail.com>',
		'5: Corrie McDowel <>',
		'3: Ken Plastic <>',
		],
		"all fake classes work"
	);

	is_deeply( scalar($db->FetchUsersOA()), \@good, "Fetch and create objects (ARRAY)" );
	my @got = $db->FetchUsersOA();
	is_deeply( \@got, \@good, "Fetch and create objects (ARRAY) list context" );

	is_deeply( scalar($db->FetchUsersORA()), \@good, "Fetch and create objects (ARRAYREF)" );
	@got = $db->FetchUsersORA();
	is_deeply( \@got, \@good, "Fetch and create objects (ARRAYREF) list context" );

	is_deeply( scalar($db->FetchUsersOH()), \@good, "Fetch and create objects (HASH)" );
	@got = $db->FetchUsersOH();
	is_deeply( \@got, \@good, "Fetch and create objects (HASH) list context" );

	is_deeply( scalar($db->FetchUsersORH()), \@good, "Fetch and create objects (HASHREF)" );
	@got = $db->FetchUsersORH();
	is_deeply( \@got, \@good, "Fetch and create objects (HASHREF) list context" );
}


END {
	if ($db) {
		ok($db->UseDatabase( 'master'), 'master database selected');
		ok($db->DropDatabase('DBIx_Declare_Test'), 'test database dropped');
	}
}

} # of SKIP
exit;

package Cls;

sub toString {
	my $self = shift();
	return "$self->{id}: $self->{fname} $self->{lname} <$self->{email}>"
}

package ClsA;

sub new {
	my $class = shift();
	my ($id, $fname, $lname, $email) = @_;
	bless {id => $id, fname => $fname, lname => $lname, email => $email}, 'Cls';
}

package ClsRA;

sub new {
	my $class = shift();
	my ($id, $fname, $lname, $email) = @{$_[0]};
	bless {id => $id, fname => $fname, lname => $lname, email => $email}, 'Cls';
}

package ClsH;

sub new {
	my $class = shift();
	my %data = @_;
	bless {id => $data{Id}, fname => $data{FirstName}, lname => $data{LastName}, email => $data{Email}}, 'Cls';
}

package ClsRH;

sub new {
	my $class = shift();
	my ($data) = @_;
	bless {id => $data->{Id}, fname => $data->{FirstName}, lname => $data->{LastName}, email => $data->{Email}}, 'Cls';
}

