#  #!perl -T
use strict;
use Test::More tests => 31;
use Data::Dumper;
use Cwd;

BEGIN {
	use_ok( 'DBIx::Declare' );
}

#diag( "Testing DBIx::Declare $DBIx::Declare::VERSION, Perl $], $^X" );

SKIP: {
	{
		my $drh = eval{DBI->install_driver("ODBC")};
		diag($@) if $@;
		skip ": DBD::ODBC not available", 30 unless $drh;
	}

	{
		my $dbh = DBI->connect('dbi:ODBC:Driver=SQL Server;Server=localhost;Database=master');
		unless ($dbh) {
			diag($DBI::errstr);
			skip ": local MSSQL server connection failed", 30 ;
		}
	}

my $cache_file;
BEGIN {
	$cache_file = cwd() . '/code_cache_hash.dat';
	($cache_file) = ($cache_file =~ /(.*)/);
}
diag( "cache file=$cache_file");

skip ": the cache file $cache_file doesn't exist. Please run the 11-code_cache_hash_1.t first", 30
	if (! -f $cache_file);

my $code_cache = do $cache_file or skip ": could not import the code cache file: $^E", 30;

my ($hits, $misses, $sets) = (0,0,0);

ok( scalar(keys %$code_cache) == 10, "There should be 10 items in the code cache");

use DBIx::Declare
	MyDB => {
	code_cache => sub {
		my ($name, $code) = @_;
		if (defined $code) {
			$sets++;
			$code_cache->{$name} = $code;
		} elsif (exists $code_cache->{$name}) {
			$hits++;
			return $code_cache->{$name}
		} else {
			$misses++;
			return;
		}
	},
	data_source  => "dbi:ODBC:Driver=SQL Server;Server=localhost;Database=master",
	attr => { 'RaiseError' => 0, PrintError => 0, 'AutoCommit' => 1 },
	on_errors => 'croak',
	methods => {
		'CreateDatabase' => {
			sql => 'CREATE DATABASE ? COLLATE SQL_Latin1_General_Cp1_CI_AS  ',
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
			sql => 'CREATE TABLE Users (Id int not NULL PRIMARY KEY, FirstName varchar(50) NOT NULL, LastName varchar(50) NOT NULL, Email varchar(100))',
			args => [],
			return => '$',
			noprepare => 1,
			#noquote => 1,
		},
		'InsertUser' => {
			sql => 'INSERT INTO Users (Id,FirstName,LastName,Email) VALUES (?,?,?,?)',
			args => [qw(Id FirstName LastName Email)],
			defaults => {
				Email => undef,
			},
			return => '$',
		},
		'UpdateUser' => {
			sql => 'UPDATE Users SET FirstName = COALESCE(?,FirstName), LastName = COALESCE(?,LastName), Email = COALESCE(?,Email) WHERE Id = ?',
			args => [qw(FirstName LastName Email Id)],
			defaults => {
				Email => undef,
				LastName => undef,
				FirstName => undef,
			},
			return => '$',
		},
		DeleteUser => {
			sql => 'DELETE FROM Users WHERE Id = ?',
			args => ['Id'],
			return => '$',
		},
		FetchUserDetails => {
			sql => 'SELECT * FROM Users WHERE Id = ?',
			args => ['Id'],
			return => '%',
		},
		FetchUsers => {
			sql => 'SELECT * FROM Users ORDER BY LastName, FirstName',
			args => [],
			return => '@@',
		},
		FetchUsersWithNames => {
			sql => 'SELECT * FROM Users ORDER BY LastName, FirstName',
			args => [],
			return => '@%',
		},
	},
};

my $db = MyDB->new();

ok($db && ref($db), 'database access object created');

ok($db->CreateDatabase( 'DBIx_Declare_Test'), 'test database created');

ok($db->UseDatabase( 'DBIx_Declare_Test'), 'test database selected');

ok($db->CreateTableUsers(), 'table dbo.Users created');

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

if ($db) {
	ok($db->UseDatabase( 'master'), 'master database selected');
	ok($db->DropDatabase('DBIx_Declare_Test'), 'test database dropped');
	undef $db;
}

END {
	if ($db) {
		ok($db->UseDatabase( 'master'), 'master database selected');
		ok($db->DropDatabase('DBIx_Declare_Test'), 'test database dropped');
	}
}

ok( scalar(keys %$code_cache) == 10, "There should still be 10 items in the code cache");
is( $hits, 10, "There should be 10 cache hits");
is( $misses, 0, "There should be 0 cache misses logged");
is( $sets, 0, "and no code was stored");

} # of SKIP
