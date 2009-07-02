BEGIN {
#	$ENV{DBIx_Declare_Debug} = 1;
}

#!perl -T
use strict;
use Test::More tests => 75;
use Data::Dumper;

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
			defaults => {
				Email => undef,
			},
			return => '$R',
		},
		FetchUserDetails => {
			return => '_@',
		},
		'FetchUsers' => {
			return => '@@',
		},
		'GetUsersEmail' => '$$',
		'GetUsersDetails' => '$$',
		'GetUsersDetailsR' => {
			return => '$R',
			call => 'GetUsersDetails',
		},
		'GetUsersDetailsN' => {
			return => '$',
			call => 'GetUsersDetails',
		},
		'GetUsersDetailsNoNull' => {
			return => '$R',
			output_only => 1,
		},
		FetchUsersAndCount => '@@',
		FetchUsersAndRetval => {
			return => '@@',
			return_value => 'Count',
		}
	},
};

#diag( "Testing DBIx::Declare $DBIx::Declare::VERSION, Perl $], $^X" );

my $db = MyDB->new() or die "failed to create the object";

ok(
(
	$db->CreateDatabase( 'DBIx_Declare_Test')
	or
	$db->UseDatabase( 'master') and $db->DropDatabase('DBIx_Declare_Test') and $db->CreateDatabase( 'DBIx_Declare_Test')
)
and
$db->UseDatabase( 'DBIx_Declare_Test')
and
$db->CreateTableUsers()
, "set up the database and table");

$db->{_dbh}->do(<<'*SQL*');
CREATE PROCEDURE dbo.InsertUser (
	@FirstName varchar(50),
	@LastName varchar(50),
	@Email varchar(100)
) AS
BEGIN
--	SET NOCOUNT ON;
	INSERT INTO dbo.Users (FirstName,LastName,Email)
	VALUES (@FirstName,@LastName,@Email);

	RETURN SCOPE_IDENTITY();
END
*SQL*

$db->{_dbh}->do(<<'*SQL*');
CREATE PROCEDURE dbo.FetchUserDetails( @Id int )
as
BEGIN
	SELECT * FROM dbo.Users WHERE Id = @Id
END
*SQL*

$db->{_dbh}->do(<<'*SQL*');
CREATE PROCEDURE dbo.FetchUsers
as
BEGIN
	SELECT * FROM dbo.Users ORDER BY LastName, FirstName
END
*SQL*

$db->{_dbh}->do(<<'*SQL*');
CREATE PROCEDURE dbo.GetUsersEmail(
	@Id int,
	@Email varchar(100) OUTPUT
)
as
BEGIN
	SELECT @Email = Email FROM dbo.Users WHERE Id = @Id
END
*SQL*

$db->{_dbh}->do(<<'*SQL*');
CREATE PROCEDURE dbo.GetUsersDetails(
	@Id int,
	@FirstName varchar(50) OUTPUT,
	@LastName varchar(50) OUTPUT,
	@Email varchar(100) OUTPUT
)
as
BEGIN
	SET @FirstName = NULL; SET @LastName = NULL; SET @Email = NULL;
	SELECT @FirstName = FirstName, @LastName = LastName, @Email = Email FROM dbo.Users WHERE Id = @Id;

	IF exists (SELECT * FROM dbo.Users WHERE Id = @Id)
		RETURN 1
	ELSE
		RETURN 0
END
*SQL*

$db->{_dbh}->do(<<'*SQL*');
CREATE PROCEDURE dbo.GetUsersDetailsNoNull(
	@Id int,
	@FirstName varchar(50) OUTPUT,
	@LastName varchar(50) OUTPUT,
	@Email varchar(100) OUTPUT
)
as
BEGIN
	SELECT @FirstName = FirstName, @LastName = LastName, @Email = Email FROM dbo.Users WHERE Id = @Id;

	IF exists (SELECT * FROM dbo.Users WHERE Id = @Id)
		RETURN 1
	ELSE
		RETURN 0
END
*SQL*

$db->{_dbh}->do(<<'*SQL*');
CREATE PROCEDURE dbo.FetchUsersAndCount( @Count int OUTPUT)
as
BEGIN
	SELECT @Count = Count(*) FROM dbo.Users;
	SELECT * FROM dbo.Users ORDER BY LastName, FirstName;
END
*SQL*

$db->{_dbh}->do(<<'*SQL*');
CREATE PROCEDURE dbo.FetchUsersAndRetval
as
BEGIN
--	SET NOCOUNT ON;
	Declare @Count int;
	SELECT @Count = Count(*) FROM dbo.Users;
	SELECT * FROM dbo.Users ORDER BY LastName, FirstName;
	return @Count;
END
*SQL*

is( $db->InsertUser('John', 'Doe', 'John.Doe@hotmail.com'), 1, "Inserted row using stored proc and got the ID");

is( $db->InsertUser(-FirstName => 'Jane', -LastName => 'Doe', -Email => 'Jane.Doe@hotmail.com'), 2, "and with named params");

$db->InsertUser('Ken', 'Plastic');
$db->InsertUser(-FirstName => 'Barbie', -LastName => 'Blond');
$db->InsertUser(-LastName => 'McDowel', -FirstName => 'Corrie');

is_deeply( scalar($db->FetchUserDetails(2)), ['2','Jane','Doe','Jane.Doe@hotmail.com'], "Fetch user details in scalar context");
is_deeply( scalar($db->FetchUserDetails(-Id => 3)), ['3','Ken','Plastic',undef], "Fetch user details in scalar context with named args wo Email");

{
	my $got = $db->FetchUsers();
	my $good = [
		['4','Barbie','Blond',undef],
		['2','Jane','Doe','Jane.Doe@hotmail.com'],
		['1','John','Doe','John.Doe@hotmail.com'],
		['5','Corrie','McDowel',undef],
		['3','Ken','Plastic',undef]
	];
	is_deeply($got, $good, "Fetch as array in scalar context");
}


is( $db->GetUsersEmail(1), 'John.Doe@hotmail.com', "return output parameter");
is( $db->GetUsersEmail(-Id => 2), 'Jane.Doe@hotmail.com', "return output parameter with named params");
{
	my $got = $db->GetUsersEmail(1);
	is( $got, 'John.Doe@hotmail.com', "return output parameter (scalar context)");
}

{
	my ($res, $email);
	$res = $db->GetUsersEmail(1,$email);
	is( $res, 'John.Doe@hotmail.com', "return and set output parameter (retval)");
	is( $email, 'John.Doe@hotmail.com', "return and set output parameter (outparam)");

	$res = $db->GetUsersEmail(-Id => 2, -Email => $email);
	is( $res, 'Jane.Doe@hotmail.com', "return and set output parameter (retval)");
	is( $email, 'Jane.Doe@hotmail.com', "return and set output parameter (outparam)");
}


is_deeply( [$db->GetUsersDetails(1)], ['John','Doe','John.Doe@hotmail.com'], "return multiple output parameters");
is_deeply( [$db->GetUsersDetails(-Id => 2)], ['Jane','Doe','Jane.Doe@hotmail.com'], "return multiple output parameters with named params");
{
	my $got = $db->GetUsersDetails(1);
	is_deeply( $got, ['John','Doe','John.Doe@hotmail.com'], "return multiple output parameters (scalar context)");
}

{
	my ($res, $fname, $lname, $email);
	$res = $db->GetUsersDetails(1, $fname, $lname, $email);
	is_deeply( $res, ['John','Doe','John.Doe@hotmail.com'], "return and set output parameters (retval)");
	is_deeply( $fname, 'John', "return and set output parameters (outparam 1)");
	is_deeply( $lname, 'Doe', "return and set output parameters (outparam 2)");
	is_deeply( $email, 'John.Doe@hotmail.com', "return and set output parameters (outparam 3)");

	$res = $db->GetUsersDetails(-Id => 2, -Email => $email, -FirstName => $fname);
	is_deeply( $res, ['Jane','Doe','Jane.Doe@hotmail.com'], "return and set output parameter (retval)");
	is_deeply( $fname, 'Jane', "return and set output parameter (outparam FirstName)");
	is_deeply( $email, 'Jane.Doe@hotmail.com', "return and set output parameter (outparam Email)");
}


{
	my ($res, $fname, $lname, $email);
	$res = $db->GetUsersDetailsR(1, $fname, $lname, $email);
	is( $res, 1, "set output parameters and return if found (retval)");
	is_deeply( $fname, 'John', " ... (outparam 1)");
	is_deeply( $lname, 'Doe', " ... (outparam 2)");
	is_deeply( $email, 'John.Doe@hotmail.com', " ... (outparam 3)");

	$fname = undef;
	$res = $db->GetUsersDetailsR(99, $fname, $lname, $email);
	is( $res, 0, "set output parameters and return if found (retval) - not found");
	is_deeply( $fname, undef, " ... (outparam 1) - empty");
	is_deeply( $lname, undef, " ... (outparam 2) - empty");
	is_deeply( $email, undef, " ... (outparam 3) - empty");

	$res = $db->GetUsersDetailsR(-Id => 2, -Email => $email, -FirstName => $fname);
	is( $res, 1, "set output parameters (retval)");
	is_deeply( $fname, 'Jane', " ... (outparam FirstName)");
	is_deeply( $email, 'Jane.Doe@hotmail.com', " ... (outparam Email)");

	$res = $db->GetUsersDetailsR(-Id => 99, -Email => $email, -FirstName => $fname);
	is( $res, 0, "set output parameters (retval) - not found");
	is_deeply( $fname, undef, " ... (outparam FirstName) - empty");
	is_deeply( $email, undef, " ... (outparam Email) - empty");
}


{
	my ($res, $fname, $lname, $email);
	$res = $db->GetUsersDetailsNoNull(1, $fname, $lname, $email);
	is( $res, 1, "set output parameters and return if found (retval)");
	is_deeply( $fname, 'John', " ... (outparam 1)");
	is_deeply( $lname, 'Doe', " ... (outparam 2)");
	is_deeply( $email, 'John.Doe@hotmail.com', " ... (outparam 3)");

	$fname = undef;
	$res = $db->GetUsersDetailsNoNull(99, $fname, $lname, $email);
	is( $res, 0, "set output parameters and return if found (retval) - not found");
	is_deeply( $fname, undef, " ... (outparam 1) - empty");
	is_deeply( $lname, undef, " ... (outparam 2) - empty");
	is_deeply( $email, undef, " ... (outparam 3) - empty");

	$res = $db->GetUsersDetailsNoNull(-Id => 2, -Email => $email, -FirstName => $fname);
	is( $res, 1, "set output parameters (retval)");
	is_deeply( $fname, 'Jane', " ... (outparam FirstName)");
	is_deeply( $email, 'Jane.Doe@hotmail.com', " ... (outparam Email)");

	$res = $db->GetUsersDetailsNoNull(-Id => 99, -Email => $email, -FirstName => $fname);
	is( $res, 0, "set output parameters (retval) - not found");
	is_deeply( $fname, undef, " ... (outparam FirstName) - empty");
	is_deeply( $email, undef, " ... (outparam Email) - empty");
}


{
	my ($res, $fname, $lname, $email);
	$res = $db->GetUsersDetailsN(1, $fname, $lname, $email);
	ok( $res, "set output parameters and return if no error (retval)");
	is_deeply( $fname, 'John', " ... (outparam 1)");
	is_deeply( $lname, 'Doe', " ... (outparam 2)");
	is_deeply( $email, 'John.Doe@hotmail.com', " ... (outparam 3)");

	$fname = undef;
	$res = $db->GetUsersDetailsN(99, $fname, $lname, $email);
	ok( $res, "set output parameters and return if no error (retval) - not found");
	is_deeply( $fname, undef, " ... (outparam 1) - empty");
	is_deeply( $lname, undef, " ... (outparam 2) - empty");
	is_deeply( $email, undef, " ... (outparam 3) - empty");

	$res = $db->GetUsersDetailsN(-Id => 2, -Email => $email, -FirstName => $fname);
	ok( $res, "set output parameters (retval)");
	is_deeply( $fname, 'Jane', " ... (outparam FirstName)");
	is_deeply( $email, 'Jane.Doe@hotmail.com', " ... (outparam Email)");

	$res = $db->GetUsersDetailsN(-Id => 99, -Email => $email, -FirstName => $fname);
	ok( $res, "set output parameters (retval) - not found");
	is_deeply( $fname, undef, " ... (outparam FirstName) - empty");
	is_deeply( $email, undef, " ... (outparam Email) - empty");
}


{
	my $count;
	my $got = $db->FetchUsersAndCount($count);
	my $good = [
		['4','Barbie','Blond',undef],
		['2','Jane','Doe','Jane.Doe@hotmail.com'],
		['1','John','Doe','John.Doe@hotmail.com'],
		['5','Corrie','McDowel',undef],
		['3','Ken','Plastic',undef]
	];
	is_deeply($got, $good, "Fetch as array in scalar context and output param");
	is($count, 5, " ... the output param");

	undef $got; undef $count;
	$got = $db->FetchUsersAndCount(-Count => $count);
	is_deeply($got, $good, "Fetch as array in scalar context and named output param");
	is($count, 5, " ... the output param");
}

{
	my $count;
	my $got = $db->FetchUsersAndRetval($count);
	my $good = [
		['4','Barbie','Blond',undef],
		['2','Jane','Doe','Jane.Doe@hotmail.com'],
		['1','John','Doe','John.Doe@hotmail.com'],
		['5','Corrie','McDowel',undef],
		['3','Ken','Plastic',undef]
	];
	is_deeply($got, $good, "Fetch as array in scalar context and return value as a named param");
	is($count, 5, " ... the output param");

	undef $got; undef $count;
	$got = $db->FetchUsersAndRetval(-Count => $count);
	is_deeply($got, $good, "Fetch as array in scalar context and return value as a named param");
	is($count, 5, " ... the output param");
}

END {
	if ($db) {
		ok($db->UseDatabase( 'master'), 'master database selected');
		ok($db->DropDatabase('DBIx_Declare_Test'), 'test database dropped');
	}
}

} # of SKIP
