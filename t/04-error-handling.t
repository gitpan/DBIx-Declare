BEGIN {
#	$ENV{DBIx_Declare_Debug} = 1;
}

#!perl -T
use strict;
use Test::More tests => 32;
use Data::Dumper;

SKIP: {
	{
		my $drh = eval{DBI->install_driver("ODBC")};
		diag($@) if $@;
		skip ": DBD::ODBC not available", 32 unless $drh;
	}

	{
		my $dbh = DBI->connect('dbi:ODBC:Driver=SQL Server;Server=localhost;Database=master');
		unless ($dbh) {
			diag($DBI::errstr);
			skip ": local MSSQL server connection failed", 32 ;
		}
	}

use DBIx::Declare
	MyDB => {
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

$db->InsertUser('John', 'Doe', 'John.Doe@hotmail.com');
$db->InsertUser(-FirstName => 'Jane', -LastName => 'Doe', -Email => 'Jane.Doe@hotmail.com');
$db->InsertUser('Ken', 'Plastic');
$db->InsertUser(-FirstName => 'Barbie', -LastName => 'Blond');
$db->InsertUser(-LastName => 'McDowel', -FirstName => 'Corrie');

$db->{_dbh}->do(<<'*SQL*');
CREATE PROCEDURE dbo.TestFailBindIO ( @Number int OUTPUT)
as
BEGIN
	SET @Number = @Number * 2 / 0;
END
*SQL*

$db->{_dbh}->do(<<'*SQL*');
CREATE PROCEDURE dbo.TestFailBindIOx ( @Some varchar(40), @Number int OUTPUT)
as
BEGIN
	SET @Number = @Number * 2;
END
*SQL*

use DBIx::Declare
	MyDBDie => {
	data_source  => "dbi:ODBC:Driver=SQL Server;Server=localhost;Database=DBIx_Declare_Test",
	type => 'mssql',
	attr => { 'RaiseError' => 0, PrintError => 0, 'AutoCommit' => 1 },
	on_errors => 'die',
	methods => {
		'FailPrepare' => {
			sql => 'this is not (ever a val1d SQL',
			args => [],
			return => '$',
		},
		'FailExecute' => {
			sql => 'SELECT 1/0',
			args => [],
			return => '$',
		},
		'FailExecuteWParams' => {
			sql => q{SELECT 3*? as num, 'Hallo ' + ? as str, 1/0 as bad},
			args => [qw(foo bar)],
			return => '$',
		},
		'TestFailBindIO' => {
			return => '@',
		},
		'TestFailBindIOx' => {
			return => '@',
		},
	},
};

my $tdb = MyDBDie->new() or die "failed to create the object";


## die

#$tdb->{_show_generated_code} = 1;
eval {
	my $res  = $tdb->FailPrepare();
	1;
};
is($@, q{FailPrepare execution failed: [Microsoft][ODBC SQL Server Driver][SQL Server]Incorrect syntax near the keyword 'is'. (SQL-42000)
[Microsoft][ODBC SQL Server Driver][SQL Server]Statement(s) could not be prepared. (SQL-42000)
}, "Error for failed prepare (on_errors=croak)");

eval {
	my $res  = $tdb->FailExecute();
	1;
};
is($@, q{FailExecute execution failed: [Microsoft][ODBC SQL Server Driver][SQL Server]Divide by zero error encountered. (SQL-22012)
}, "Error for failed execute");

eval {
	my $res = $tdb->FailExecuteWParams(15,'Jenda');
	1;
};
is( $@, q{FailExecuteWParams execution failed: [Microsoft][ODBC SQL Server Driver][SQL Server]Divide by zero error encountered. (SQL-22012)
}, "Error for failed execute with params");

eval {
	my $res  = $tdb->TestFailBindIO('sdhfgwe');
	1;
};
is($@, q{The Number (argument no 0) is an OUTPUT parameter, you have to pass a variable!
}, "Constant for output parameter");

eval {
	my $res  = $tdb->TestFailBindIO(Number => 'sdhfgwe');
	1;
};
is($@, q{The Number (argument no 0) is an OUTPUT parameter, you have to pass a variable!
}, "Constant for named output parameter");

eval {
	my $num = 'sdfdfgsdfg';
	my $res  = $tdb->TestFailBindIO($num);
	1;
};
is($@, q{TestFailBindIO execution failed: [Microsoft][ODBC SQL Server Driver]Invalid character value for cast specification (SQL-22018)
}, "String passed to numerical parameter");


eval {
	my $num = 15;
	my $res  = $tdb->TestFailBindIO($num);
	1;
};
is($@, q{TestFailBindIO execution failed: [Microsoft][ODBC SQL Server Driver][SQL Server]Divide by zero error encountered. (SQL-22012)
}, "Number passed to numerical parameter");


### croak

$tdb->{_on_errors} = 'croak';

eval {
	my $res  = $tdb->FailPrepare();
	1;
};
ok( $@ =~ m{FailPrepare execution failed: \[Microsoft\]\[ODBC SQL Server Driver\]\[SQL Server\]Incorrect syntax near the keyword 'is'\. \(SQL-42000\)
\[Microsoft\]\[ODBC SQL Server Driver\]\[SQL Server\]Statement\(s\) could not be prepared\. \(SQL-42000\) at .*[/\\]04-error-handling\.t line \d+
}, "Error for failed prepare (on_errors=die)");

eval {
	my $res  = $tdb->FailExecute();
	1;
};
ok( $@ =~ m{FailExecute execution failed: \[Microsoft\]\[ODBC SQL Server Driver\]\[SQL Server\]Divide by zero error encountered\. \(SQL-22012\) at .*[/\\]04-error-handling\.t line \d+
}, "Error for failed execute");

eval {
	my $res = $tdb->FailExecuteWParams(15,'Jenda');
	1;
};
ok( $@ =~ m{FailExecuteWParams execution failed: \[Microsoft\]\[ODBC SQL Server Driver\]\[SQL Server\]Divide by zero error encountered\. \(SQL-22012\) at .*[/\\]04-error-handling\.t line \d+
}, "Error for failed execute with params");

eval {
	my $res  = $tdb->TestFailBindIO('sdhfgwe');
	1;
};
ok($@ =~ m{The Number \(argument no 0\) is an OUTPUT parameter, you have to pass a variable! at .*[/\\]04-error-handling\.t line \d+
}, "Constant for output parameter");

eval {
	my $res  = $tdb->TestFailBindIO(Number => 'sdhfgwe');
	1;
};
ok($@ =~ m{The Number \(argument no 0\) is an OUTPUT parameter, you have to pass a variable! at .*[/\\]04-error-handling\.t line \d+
}, "Constant for named output parameter");

eval {
	my $num = 'sdfdfgsdfg';
	my $res  = $tdb->TestFailBindIO($num);
	1;
};
ok($@ =~ m{TestFailBindIO execution failed: \[Microsoft\]\[ODBC SQL Server Driver\]Invalid character value for cast specification \(SQL-22018\) at .*[/\\]04-error-handling\.t line \d+
}, "String passed to numerical parameter");


eval {
	my $num = 15;
	my $res  = $tdb->TestFailBindIO($num);
	1;
};
ok($@ =~m{TestFailBindIO execution failed: \[Microsoft\]\[ODBC SQL Server Driver\]\[SQL Server\]Divide by zero error encountered. \(SQL-22012\) at .*[/\\]04-error-handling\.t line \d+
}, "Number passed to numerical parameter");


#### die + format_errors


$tdb->{_on_errors} = 'die';
$tdb->{_format_errors} = sub {
	my ($self, $msg, $errstr) = @_;
	return $msg unless $errstr;
	$errstr =~ s/^(?:\[[^\]]+\])*//mg;
	$errstr =~ s/\n/\n\t/gs;

	return "$msg\n\t$errstr";
};

eval {
	my $res  = $tdb->FailPrepare();
	1;
};
is($@, qq{FailPrepare execution failed
\tIncorrect syntax near the keyword 'is'. (SQL-42000)
\tStatement(s) could not be prepared. (SQL-42000)
}, "Error for failed prepare (on_errors=die+format_errors)");

eval {
	my $res  = $tdb->FailExecute();
	1;
};
is($@, qq{FailExecute execution failed
\tDivide by zero error encountered. (SQL-22012)
}, "Error for failed execute");

eval {
	my $res = $tdb->FailExecuteWParams(15,'Jenda');
	1;
};
is($@, qq{FailExecuteWParams execution failed
\tDivide by zero error encountered. (SQL-22012)
}, "Error for failed execute with params");


eval {
	my $res  = $tdb->TestFailBindIO('sdhfgwe');
	1;
};
is($@, q{The Number (argument no 0) is an OUTPUT parameter, you have to pass a variable!
}, "Constant for output parameter");

eval {
	my $res  = $tdb->TestFailBindIO(Number => 'sdhfgwe');
	1;
};
is($@, q{The Number (argument no 0) is an OUTPUT parameter, you have to pass a variable!
}, "Constant for named output parameter");

eval {
	my $num = 'sdfdfgsdfg';
	my $res  = $tdb->TestFailBindIO($num);
	1;
};
is($@, qq{TestFailBindIO execution failed
\tInvalid character value for cast specification (SQL-22018)
}, "String passed to numerical parameter");

eval {
	my $num = 15;
	my $res  = $tdb->TestFailBindIO($num);
	1;
};
is($@, qq{TestFailBindIO execution failed
\tDivide by zero error encountered. (SQL-22012)
}, "Number passed to numerical parameter");


## die + details

$tdb->{_on_errors} = 'die';
delete $tdb->{_format_errors};
$tdb->{_error_details} = '1';
eval {
	my $res  = $tdb->FailPrepare();
	1;
};
is($@, q{FailPrepare execution failed: [Microsoft][ODBC SQL Server Driver][SQL Server]Incorrect syntax near the keyword 'is'. (SQL-42000)
[Microsoft][ODBC SQL Server Driver][SQL Server]Statement(s) could not be prepared. (SQL-42000)
 in: this is not (ever a val1d SQL
}, "Error for failed prepare (on_errors=die+details)");

eval {
	my $res  = $tdb->FailExecute();
	1;
};
is($@, q{FailExecute execution failed: [Microsoft][ODBC SQL Server Driver][SQL Server]Divide by zero error encountered. (SQL-22012)
 in: SELECT 1/0
}, "Error for failed execute");

eval {
	my $res = $tdb->FailExecuteWParams(15,'Jenda');
	1;
};
{ my $err = $@;
#	$err =~ tr/\x00//d;
	is($err, q{FailExecuteWParams execution failed: [Microsoft][ODBC SQL Server Driver][SQL Server]Divide by zero error encountered. (SQL-22012)
 in: SELECT 3*'15' as num, 'Hallo ' + 'Jenda' as str, 1/0 as bad
}, "Error for failed execute with params");
}

eval {
	my $res  = $tdb->TestFailBindIO('sdhfgwe');
	1;
};
is($@, q{The Number (argument no 0) is an OUTPUT parameter, you have to pass a variable!
}, "Constant for output parameter");

eval {
	my $res  = $tdb->TestFailBindIO(Number => 'sdhfgwe');
	1;
};
is($@, q{The Number (argument no 0) is an OUTPUT parameter, you have to pass a variable!
}, "Constant for named output parameter");

eval {
	my $num = 'sdfdfgsdfg';
	my $res  = $tdb->TestFailBindIO($num);
	1;
};
is($@, q{TestFailBindIO execution failed: [Microsoft][ODBC SQL Server Driver]Invalid character value for cast specification (SQL-22018)
 in: Declare @Number int;
     SET @Number = 'sdfdfgsdfg';
     EXEC dbo.TestFailBindIO @Number = @Number OUTPUT
     SELECT @Number;
}, "String passed to numerical parameter");

eval {
	my $num = 'sdfdfgsdfg';
	my $res  = $tdb->TestFailBindIOx('what', $num);
	1;
};
is($@, q{TestFailBindIOx execution failed: [Microsoft][ODBC SQL Server Driver]Invalid character value for cast specification (SQL-22018)
 in: Declare @Number int;
     SET @Number = 'what';
     EXEC dbo.TestFailBindIOx @Some = NULL, @Number = @Number OUTPUT
     SELECT @Number;
}, "String passed to numerical parameter, more parameters");

eval {
	my $num = 15;
	my $res  = $tdb->TestFailBindIO($num);
	1;
};
is($@, q{TestFailBindIO execution failed: [Microsoft][ODBC SQL Server Driver][SQL Server]Divide by zero error encountered. (SQL-22012)
 in: Declare @Number int;
     SET @Number = '15';
     EXEC dbo.TestFailBindIO @Number = @Number OUTPUT
     SELECT @Number;
}, "Number passed to numerical parameter");


END {
	if ($db) {
		ok($db->UseDatabase( 'master'), 'master database selected');
		ok($db->DropDatabase('DBIx_Declare_Test'), 'test database dropped');
	}
}

} # of SKIP
