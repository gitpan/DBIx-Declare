#!perl -T
use strict;
use Test::More tests => 53;
use Data::Dumper;
use warnings;
no warnings 'uninitialized';

BEGIN {
	use_ok( 'DBIx::Declare' );
}

SKIP: {
	my $drh = eval {DBI->install_driver("mysql")};
	diag($@) if $@;
	skip "DBD::mysql not available", 52 unless $drh;

	diag( "Testing DBIx::Declare $DBIx::Declare::VERSION, DBI $DBI::VERSION, DBD::mysql $DBD::mysql::VERSION, Perl $], $^X" );

	my $database = $ENV{mysql_test_database} || 'DBIx_Declare_Test';
	my $host = $ENV{mysql_test_host} || 'localhost';
	my $username = $ENV{mysql_test_user} || 'root';
	my $password = $ENV{mysql_test_pwd} || 'test';
	my $data_source = "DBI:mysql:database=$database;host=$host";
	my $created_db = 0;
	{
		my $dbh = eval{ DBI->connect($data_source, $username, $password, { 'RaiseError' => 0, PrintError => 0, 'AutoCommit' => 1 })};
		unless ($dbh) {
			my $conn_error = $DBI::errstr;
			if ($drh->func("createdb", $database, $host, $username, $password, 'admin')) {
				$created_db = 1;
				diag("Created test database $database");
			} else {
				diag(<<"*END*");
Failed to connect to $database on $host as $username: $conn_error
And failed to create the database: $DBI::errstr

If you want to run this test script, please set the system variables
mysql_test_host, mysql_test_user and mysql_test_password and optionaly
mysql_test_database. If the database doesn't work on the server it will be
created and dropped (the user must have the Create and Drop database
permission), if it exists it will be used and not dropped! The table
DBIx_Declare_Users should NOT exist and will be created and dropped
by the script.
*END*
				skip ": mysql server connection failed", 52;
			}
		}
	}

DBIx::Declare->import(
	MyDB => {
	data_source  => $data_source,
	user => $username,
	pass => $password,
	attr => { 'RaiseError' => 0, PrintError => 0, 'AutoCommit' => 1 },
	on_errors => 'croak',
	methods => {
		'CreateTableUsers' => {
			sql => 'CREATE TABLE DBIx_Declare_Users (Id int not NULL PRIMARY KEY, FirstName varchar(50) NOT NULL, LastName varchar(50) NOT NULL, Email varchar(100))',
			args => [],
			return => '$',
			noprepare => 1,
			#noquote => 1,
		},
		'DropTableUsers' => {
			sql => 'DROP TABLE DBIx_Declare_Users',
			args => [],
			return => '$',
			noprepare => 1,
			#noquote => 1,
		},
		'CreateTableUsers_Autoincrement' => {
			sql => 'CREATE TABLE DBIx_Declare_Users (Id int not NULL PRIMARY KEY auto_increment, FirstName varchar(50) NOT NULL, LastName varchar(50) NOT NULL, Email varchar(100))',
			args => [],
			return => '$',
			noprepare => 1,
			#noquote => 1, # pointless with no arguments
		},
		'InsertUser' => {
			sql => 'INSERT INTO DBIx_Declare_Users (Id,FirstName,LastName,Email) VALUES (?,?,?,?)',
			args => [qw(Id FirstName LastName Email)],
			defaults => {
				Email => undef,
			},
			return => '$',
		},
		'UpdateUser' => {
			sql => 'UPDATE DBIx_Declare_Users SET FirstName = COALESCE(?,FirstName), LastName = COALESCE(?,LastName), Email = COALESCE(?,Email) WHERE Id = ?',
			args => [qw(FirstName LastName Email Id)],
			defaults => {
				Email => undef,
				LastName => undef,
				FirstName => undef,
			},
			return => '$',
		},
		DeleteUser => {
			sql => 'DELETE FROM DBIx_Declare_Users WHERE Id = ?',
			args => ['Id'],
			return => '$',
		},
		DeleteAllUsers => {
			sql => 'DELETE FROM DBIx_Declare_Users',
			args => [],
			return => '$',
		},
		FetchUserDetails => {
			sql => 'SELECT * FROM DBIx_Declare_Users WHERE Id = ?',
			args => ['Id'],
			return => '%',
		},
		FetchUsers => {
			sql => 'SELECT * FROM DBIx_Declare_Users ORDER BY LastName, FirstName',
			args => [],
			return => '@@',
		},
		FetchUsersWithNames => {
			sql => 'SELECT * FROM DBIx_Declare_Users ORDER BY LastName, FirstName',
			args => [],
			return => '@%',
		},
		InsertUserID => {
			sql => 'INSERT INTO DBIx_Declare_Users (FirstName,LastName,Email) VALUES (?,?,?)',
			args => [qw(FirstName LastName Email)],
			defaults => {
				Email => undef,
			},
			return => '++',
		},
		FetchUserDetailsA => {
			sql => 'SELECT * FROM DBIx_Declare_Users WHERE Id = ?',
			args => ['Id'],
			return => '_@',
		},
		'FetchUsersA,FetchUsersHNDL,FetchUsersSA,FetchUsersSRA,FetchUsersSH,FetchUsersSRH,
		 FetchUsersOA,FetchUsersORA,FetchUsersOH,FetchUsersORH,' => {
			sql => 'SELECT * FROM DBIx_Declare_Users ORDER BY LastName, FirstName',
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
});

my $db = MyDB->new();

ok($db && ref($db), 'database access object created');

ok($db->CreateTableUsers(), 'table dbo.DBIx_Declare_Users created');

ok($db->InsertUser(1, 'John', 'Doe', 'John.Doe@hotmail.com'), 'Insert a user with positional arguments');
ok($db->InsertUser(-Id => 2, -FirstName => 'Jane', -LastName => 'Doe', -Email => 'Jane.Doe@hotmail.com'), 'Insert a user with named arguments');

ok($db->InsertUser(3, 'Ken', 'Plastic'), 'Insert a user with positional arguments and default');
ok($db->InsertUser(-Id => 4, -FirstName => 'Barbie', -LastName => 'Blond'), 'Insert a user with named arguments and a default');
ok($db->InsertUser(-FirstName => 'Corrie', -Id => 5, -LastName => 'McDowel'), 'Insert a user with named arguments and a default with different order');

is_deeply( scalar($db->FetchUserDetails(2)), {Id => 2, FirstName => 'Jane', LastName => 'Doe', Email => 'Jane.Doe@hotmail.com'}, "Fetch user details in scalar context");

{
	my %got = $db->FetchUserDetails(2);
	is_deeply( \%got, {Id => 2, FirstName => 'Jane', LastName => 'Doe', Email => 'Jane.Doe@hotmail.com'}, "Fetch user details in list context");
}

is_deeply( scalar($db->FetchUserDetails(-Id => 1)), {Id => 1, FirstName => 'John', LastName => 'Doe', Email => 'John.Doe@hotmail.com'}, "Fetch user details in scalar context with named args");
is_deeply( scalar($db->FetchUserDetails(-Id => 3)), {Id => 3, FirstName => 'Ken', LastName => 'Plastic', Email => undef}, "Fetch user details in scalar context with named args wo Email");

{
	my $got = $db->FetchUsers();
	my $good = [
		['4','Barbie','Blond',undef],
		['2','Jane','Doe','Jane.Doe@hotmail.com'],
		['1','John','Doe','John.Doe@hotmail.com'],
		['5','Corrie','McDowel',undef],
		['3','Ken','Plastic',undef]
	];
	is_deeply($got, $good, "Fetch as array of arrays in scalar context");

	my @got = $db->FetchUsers();
	is_deeply( \@got, $good, "Fetch as array of arrays in list context");
}

{
	my $got = $db->FetchUsersWithNames();
#print Dumper($got);
	my $good = [
		{'FirstName' => 'Barbie','Id' => '4','LastName' => 'Blond','Email' => undef},
		{'FirstName' => 'Jane','Id' => '2','LastName' => 'Doe','Email' => 'Jane.Doe@hotmail.com'},
		{'FirstName' => 'John','Id' => '1','LastName' => 'Doe','Email' => 'John.Doe@hotmail.com'},
		{'FirstName' => 'Corrie','Id' => '5','LastName' => 'McDowel','Email' => undef},
		{'FirstName' => 'Ken','Id' => '3','LastName' => 'Plastic','Email' => undef}
	];
	is_deeply($got, $good, "Fetch as array of hashes in scalar context");

	my @got = $db->FetchUsersWithNames();
	is_deeply( \@got, $good, "Fetch as array of hashes in list context");
}

ok( $db->UpdateUser( -Id => 5, -Email => 'Corrie@shotmail.com', -FirstName => 'Corrie', -LastName => 'McDowel'), 'update a user');
is_deeply( scalar($db->FetchUserDetails(-Id => 5)), {Id => 5, FirstName => 'Corrie', LastName => 'McDowel', Email => 'Corrie@shotmail.com'}, "Fetch updated user details");

ok( $db->UpdateUser( -Id => 3, -Email => 'Ken@mattel.com'), 'update a user with defaults');
is_deeply( scalar($db->FetchUserDetails(-Id => 3)), {Id => 3, FirstName => 'Ken', LastName => 'Plastic', Email => 'Ken@mattel.com'}, "Fetch updated user details");

ok( $db->DeleteUser( -Id => 4), 'delete a user');
{
	my $got = $db->FetchUsers();
	my $good = [
		['2','Jane','Doe','Jane.Doe@hotmail.com'],
		['1','John','Doe','John.Doe@hotmail.com'],
		['5','Corrie','McDowel','Corrie@shotmail.com'],
		['3','Ken','Plastic','Ken@mattel.com']
	];
	is_deeply($got, $good, "Fetch updated users");
}

ok( $db->DeleteAllUsers(), "delete all users");
{
	my $got = $db->FetchUsers();
	is_deeply($got, [], "Fetch empty resultset");
}

# various returns
$db->DropTableUsers;
$db->CreateTableUsers_Autoincrement();
#$db->{_show_generated_code} = 1;
is( $db->InsertUserID('John', 'Doe', 'John.Doe@hotmail.com'), 1, "Inserted row ID");
#$db->{_show_generated_code} = 0;

is( $db->InsertUserID(-FirstName => 'Jane', -LastName => 'Doe', -Email => 'Jane.Doe@hotmail.com'), 2, "Inserted row ID with named params");


$db->InsertUserID('Ken', 'Plastic');
$db->InsertUserID(-FirstName => 'Barbie', -LastName => 'Blond');
$db->InsertUserID(-LastName => 'McDowel', -FirstName => 'Corrie');

is_deeply( scalar($db->FetchUserDetailsA(2)), ['2','Jane','Doe','Jane.Doe@hotmail.com'], "Fetch user details in scalar context");
is_deeply( scalar($db->FetchUserDetailsA(-Id => 3)), ['3','Ken','Plastic',undef], "Fetch user details in scalar context with named args wo Email");

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
	if ($drh and $created_db) {
		$drh->func("dropdb", $database, $host, $username, $password, 'admin') and diag("Dropped the $database");
	} elsif ($db) {
		$db->DropTableUsers;
	}
}

} #of SKIP

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

