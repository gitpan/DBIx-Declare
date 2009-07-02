package DBIx::Declare;

use 5.008;
use strict;
use warnings; no warnings 'uninitialized';
use Carp;
use DBI;
use UNIVERSAL qw( isa );
use Scalar::Util qw(readonly);
use Clone qw();
use Data::Dumper;

our $VERSION = '0.01.006';

#Private constants
use constant PRIVATE_METHODS 		=> qw(new AUTOLOAD DESTROY DEBUG PRIVATE_METHODS _Connect _Disconnect _init_connection _Commit _Rollback _BeginTrans _Is_error _error _InferSQL _GetIncrement _Add _Subclass isa readonly croak carp warn die _format_error_details);

#DEBUG constant
use constant DEBUG => do { my $foo = ($ENV{DBIx_Declare_Debug} =~ /(\d+)/); int($foo)};

our %RETURN_CODE = (
	'@' => [q{
		while (my $row = $sth->fetchrow_arrayref) { push @ret, @$row }
	}, q{
		return (wantarray ? @ret : \@ret);
	}],
	'_@' => [q{
		@ret = $sth->fetchrow_array;
		$sth->finish();
	}, q{
		return (wantarray ? @ret : \@ret);
	}],
	'_$' => [q{
		@ret = $sth->fetchrow_array;
		$sth->finish();
	}, q{
		return $ret[0];
	}],
	'%' => [q{
		my $ret = $sth->fetchrow_hashref;
		if ((!defined $ret) || (ref $ret eq 'HASH')) {
			$sth->finish();
			@ret = ($ret);
		} else {
			return $self->_error("<<name>> is doing fetching on a non-SELECT statement");
		}
	}, q{
		return (wantarray ? %{$ret[0]} : $ret[0]);
	}],
	'@%' => [q{
		while (my $ref = $sth->fetchrow_hashref) {
			push @ret, $ref;
		}
	}, q{
		return (wantarray ? @ret : \@ret);
	}],
	'@@' => [q{
		while (my $ref = $sth->fetchrow_arrayref) {
			push @ret, [@$ref]; # we have to make a copy, fetchrow_arrayref returns the same arrayref each time
		}
	}, q{
		return (wantarray ? @ret : \@ret);
	}],
	'$' => [q{ # return value of the ->execute() call
		$sth->finish();
		@ret = ($rv);
	}, q{
		return $ret[0];
	}],
	'$R' => [q{ # return value of the stored procedure!   EXEC ? = dbo.StoredProc ...
		@ret = ($bound->[-1]);
	}, q{
		return $ret[0];
	}],
	'$$' => [q{ # return the output parameters
		$sth->finish();
		@ret = @$bound;
	}, q{
		return (wantarray ? @ret : (@ret == 1 ? $ret[0] : \@ret));
	}],
	'++' => [q{
		$sth->finish();
		@ret = $self->_GetIncrement(sth => $sth);
	}, q{
		return $ret[0];
	}],
	'<>' => [q{
		@ret = ($sth);
	}, q{
		return $ret[0];
	}],
	#subroutines
	'&@' => [q{
		my $sub = $<<class>>::SETTINGS{_methods}{<<name>>}{sub};
		while (my $ref = $sth->fetchrow_arrayref) {
			push @ret, $sub->(@$ref);
		}
	}, q{
		return (wantarray ? @ret : \@ret);
	}],
	'&\@' => [q{ # keep in mind that the subroutine receives the same reference all the time!
		my $sub = $<<class>>::SETTINGS{_methods}{<<name>>}{sub};
		while (my $ref = $sth->fetchrow_arrayref) {
			push @ret, $sub->($ref);
		}
	}, q{
		return (wantarray ? @ret : \@ret);
	}],
	'&%' => [q{
		my $sub = $<<class>>::SETTINGS{_methods}{<<name>>}{sub};
		while (my $ref = $sth->fetchrow_hashref) {
			push @ret, $sub->(%$ref);
		}
	}, q{
		return (wantarray ? @ret : \@ret);
	}],
	'&\%' => [q{
		my $sub = $<<class>>::SETTINGS{_methods}{<<name>>}{sub};
		while (my $ref = $sth->fetchrow_hashref) {
			push @ret, $sub->($ref);
		}
	}, q{
		return (wantarray ? @ret : \@ret);
	}],
	#classes
	'.@' => [sub { q#
		while (my $ref = $sth->fetchrow_arrayref) {
			push @ret, # . $_[0]->{class} . q#->new(@$ref);
		}
	#}, q{
		return (wantarray ? @ret : \@ret);
	}],
	'.%' => [sub {
		my $name='';
		if ($_[0]->{return} =~ /uc$/) {
			$name = '("NAME_uc")'
		} elsif ($_[0]->{return} =~ /lc$/) {
			$name = '("NAME_lc")'
		}
		q{
		while (my $ref = $sth->fetchrow_hashref} . $name . q#) {
			push @ret, # . $_[0]->{class} . q#->new(%$ref);
		}
	#}, q{
		return (wantarray ? @ret : \@ret);
	}],

	'.\@' => [sub { q#
		while (my $ref = $sth->fetchrow_arrayref) {
			push @ret, # . $_[0]->{class} . q#->new($ref);
		}
	#}, q{
		return (wantarray ? @ret : \@ret);
	}],
	'.\%' => [sub {
		my $name='';
		if ($_[0]->{return} =~ /uc$/) {
			$name = '("NAME_uc")'
		} elsif ($_[0]->{return} =~ /lc$/) {
			$name = '("NAME_lc")'
		}
		q{
			while (my $ref = $sth->fetchrow_hashref} . $name . q#) {
				push @ret, # . $_[0]->{class} . q#->new($ref);
			}
		#
	}, q{
		return (wantarray ? @ret : \@ret);
	}],
);
{
	foreach (keys %RETURN_CODE) {
		next if (ref $RETURN_CODE{$_}[0]);
		for (@{$RETURN_CODE{$_}}) {
			s/^\t\t/\t/gm;
			s/\s+$//s;
			$_ .= "\n";
		}
	}
	foreach (keys %RETURN_CODE) {
		next unless /\%$/;
		if (ref $RETURN_CODE{$_}[0]) {
			$RETURN_CODE{$_.'uc'} = $RETURN_CODE{$_};
			$RETURN_CODE{$_.'lc'} = $RETURN_CODE{$_};
		} else {
			$RETURN_CODE{$_.'uc'} = do {(my $s = $RETURN_CODE{$_}[0]) =~ s/fetchrow_hashref/fetchrow_hashref("NAME_uc")/g; [$s, $RETURN_CODE{$_}[1]]};
			$RETURN_CODE{$_.'lc'} = do {(my $s = $RETURN_CODE{$_}[0]) =~ s/fetchrow_hashref/fetchrow_hashref("NAME_lc")/g; [$s, $RETURN_CODE{$_}[1]]};
		}
	}
	foreach (keys %RETURN_CODE) {
		next unless /^\.(.*)$/;
		$RETURN_CODE{'->'.$1} = $RETURN_CODE{$_};
	}
}

sub _GenerateClassName {
	no strict 'refs';
	my $name = 'DBIx::Declare::Gen::C';
	while (1) {
		my $id = int(rand(10000));
		my $class = sprintf "%s%04d", $name, $id;
		return $class unless exists ${$class.'::'}{new};
	}
}

sub import {
	my $base_class = shift;

	return unless @_; # used with no parameters, nothing to do.
	croak "use DBIx::Declare called with odd list of parameters" if @_ % 2;

	while (@_) {
		if (!defined $_[0]) {
			$_[0] = _GenerateClassName();
		}
		my $class = shift();
		croak "The second parameter to use DBIx::Declare must be a hashref: use DBIx::Declare ClassName => { options }" unless ref($_[0]) eq 'HASH';
		my %args = %{shift()};

		if (! exists $args{type}) {
			($args{type}) = $args{'data_source'} =~ /^dbi:(.*?):/i;
		}
		$args{type} = lc $args{type} || 'unknown';
		$args{type} = 'pg' if $args{type} eq 'postgresql';

		my %methods;
		{ # let's split and merge the method options
			my (@list);
			if (ref $args{methods} eq 'HASH') {
				@list = %{$args{methods}}
			} elsif (ref $args{methods} eq 'ARRAY') {
				@list = @{$args{methods}};
				if (@list % 2) {
					croak "invalid methods definition: odd number of elements in the methods array";
				}
			} else {
				croak "invalid methods definition: argument methods must be a hashref or arrayref";
			}
			unless (@list) {
				croak "no methods in methods hash/array";
			}
			while( my $names = shift(@list)) {
				my $opt = shift(@list);
				if (ref $opt) {
					croak "Method $names: the options for the method must be a hashref" unless isa( $opt, "HASH");
				} else {
					$opt = {return => $opt};
				}
				foreach my $name (split /\s*,\s*/, $names) {
					if (exists $methods{$name}) {
						foreach (keys %$opt) {
							$methods{$name}{$_} = $opt->{$_};
						}
					} else {
						$methods{$name} = {%$opt}; # we want to make a copy. We are going to add stuff to the hash! Shallow copy should be enough.
					}
				}
			}
		}
		if (exists $args{infer}) {
			foreach my $rule ((ref $args{infer} ne 'ARRAY') ? $args{infer} : @{$args{infer}}) {
				if (! ref $rule) {
					$rule = UNIVERSAL::can( 'DBIx::Declare::Infer', $rule) || croak "The infer rule '$rule' was not found in package DBIx::Declare::Infer";
				} elsif (ref $rule ne 'CODE') {
					croak "The infer rule must be either a name or a code ref, not an " . ref($rule) . " reference.";
				}

				foreach my $name (keys %methods) {
					local @_ = ($name, $methods{$name});
					&$rule; # I need to pass the parameters this strange way so that I could see the changes to @_ done within the subroutine!

					if (@_ == 2) {
						if ($_[0] eq $name) {
							$methods{$name} = $_[1];
						} else {
							delete $methods{$name};
							$methods{$_[0]} = $_[1];
						}
					} elsif (@_) { #assuming it's more
						delete $methods{$name};
						while (my $name = shift(@_) and my $opt = shift(@_)) {
							$methods{$name} = $opt
						}
					}
				}
			}
		}

		while ( my ($name, $opt) = each %methods) {
			#check for internal names / reserwed words in method names
			if (grep { $name eq $_ } PRIVATE_METHODS) {
				croak "Method name $name is a reserved method name";
			}
			if (defined($opt->{args})) {
				unless (ref($opt->{args}) eq 'ARRAY') {
					croak "Method $name: argument list must be an array";
				}
			}
			if (defined($opt->{out_args})) {
				unless (ref($opt->{out_args}) eq 'HASH') {
					croak "Method $name: output argument list must be an hash";
				}
				delete $opt->{out_args} unless keys %{$opt->{out_args}}; # delete it, if it's empty
			}

			unless (exists $opt->{return}) {
				croak "Method $name: missing return data definition";
			}
			if (ref($opt->{return})) {
				if (isa( $opt->{return}, 'CODE')) {
					$opt->{sub} = $opt->{return};
					$opt->{return} = '&@';
				} else {
					croak "Method $name: invalid return value specification. Use either the sigils, a classname or a coderef"
				}
			} elsif (exists $RETURN_CODE{$opt->{return}}) {
			} elsif ($opt->{return} =~ /^\w/) {
				$opt->{class} = $opt->{return};
				$opt->{return} = '.@';
			} else {
				croak "Method $name: invalid return value specification. Use either the sigils, a classname or a coderef"
			}

			# Since 'noprepare' causes us to do a $dbh->do, we cannot return anything else than $ (return value)
			if (($opt->{return} ne '$') && ($opt->{'noprepare'})) {
				croak "return value for $name must be $ (return value) if 'noprepare' option is used";
			}

			# Use of 'noquote' option is depending on 'noprepare' option. Check that it is set.
			if (($opt->{'noquote'}) && (! $opt->{'noprepare'})) {
				carp "useless use of 'noquote' option without required 'noprepare' option for method $name";
			}
		}

		if (defined $args{attr} and !(ref $args{attr} eq 'HASH')) {
			croak "argument 'attr' must be hashref";
		}

		croak "the format_errors must be a subroutine reference"
			if (exists $args{'format_errors'} and ref($args{'format_errors'}) ne 'CODE');

		if ($args{'code_cache'}) {
			if (ref $args{'code_cache'} eq 'CODE') {
				# OK
			} elsif (ref $args{'code_cache'} eq 'HASH') {
				# OK
			} elsif (ref $args{'code_cache'}) {
				croak "'code_cache' must be either a code or hash reference or the path to a directory"
			} elsif (-d $args{'code_cache'}) {
				# OK, set the maximal age
				if (defined $args{'code_cache_max_age'}) {
					if ($args{'code_cache_max_age'} =~ /(\d+(?:\.\d+)?)\s*m$/) {
						$args{'code_cache_max_age'} = $1 * 60
					} elsif ($args{'code_cache_max_age'} =~ /(\d+(?:\.\d+)?)\s*h$/) {
						$args{'code_cache_max_age'} = $1 * 60 * 60
					} elsif ($args{'code_cache_max_age'} =~ /^(\d+(?:\.\d+)?)\s*d?$/) {
						$args{'code_cache_max_age'} = $1 * 60 * 60 * 24
					} else {
						croak "'code_cache_max_age' must match /^\\d+(?:\\.\\d+)?\\s*[dhm]?\$/";
					}
				} else {
					$args{'code_cache_max_age'} = 1*60*60*24; # 1 day
				}

			} else {
				croak "the directory specified by the code_cache option doesn't exist or is not a directory"
			}
		}

		no strict 'refs';
		%{$class.'::SETTINGS'} = (
			_methods 			=> \%methods,
			_data_source 		=> $args{'data_source'},
			_user					=> $args{'user'},
			_pass				=> $args{'pass'},
			_attr					=> $args{'attr'},
			_on_errors			=> $args{'on_errors'} || 'croak',
			_error_details		=> $args{'error_details'},
			_case				=> $args{'case'} || 'sensitive',
			_format_errors		=> $args{'format_errors'},
			_warn_useless		=> $args{'warn_useless'},
			_disconnected		=> $args{'disconnected'},
			_code_cache		=> $args{'code_cache'},
			_code_cache_max_age		=> $args{'code_cache_max_age'},
		);

		*{$class.'::DEBUG'} = \&DEBUG;

		if (exists $DBIx::Declare::DB::{$args{type}.'::'}) {
			print '@'.$class."::ISA = ('DBIx::Declare::DB::$args{type}');\n" if DEBUG;
			@{$class.'::ISA'} = ('DBIx::Declare::DB::'.$args{type});
		} else {
			if (eval "use DBIx::Declare::DB::$args{type}") {
				print '@'.$class."::ISA = ('DBIx::Declare::DB::$args{type}')\n" if DEBUG;
				@{$class.'::ISA'} = ('DBIx::Declare::DB::'.$args{type});
			} else {
				print '@'.$class."::ISA = ('DBIx::Declare::DB::unknown')\n" if DEBUG;
				@{$class.'::ISA'} = ('DBIx::Declare::DB::unknown');
			}
		}
	}
}

package DBIx::Declare::DB::unknown;
use Carp;
#use List::MoreUtils ();
use constant DEBUG => DBIx::Declare::DEBUG;

sub new {
	my $class = shift();
	my $self;
	{
		no strict 'refs';
		$self = {
			_statements => {},
			_session => {},
			_dbh => undef,
			_on_errors => ${$class."::SETTINGS"}{_on_errors},
			_format_errors => ${$class."::SETTINGS"}{_format_errors},
			_error_details => ${$class."::SETTINGS"}{_error_details},
			DEFAULTS => {
				_data_source => ${$class."::SETTINGS"}{_data_source},
				_user => ${$class."::SETTINGS"}{_user},
				_pass => ${$class."::SETTINGS"}{_pass},
				_attr => ${$class."::SETTINGS"}{_attr},
				_case => ${$class."::SETTINGS"}{_case},
			},
			SETTINGS => \%{$class."::SETTINGS"},
		};
	}

	bless $self, $class; no strict 'refs';
	$self->_Connect(@_) unless ${$class."::SETTINGS"}{_disconnected};
	return $self;
}

sub _InferSQL {
	$_[0]->_error( "Method $_[1] missing SQL");
}

sub _GetIncrement {
	return $_[0]->_error("DBD specific AUTO_INCREMENT on unsupported DBD");
}

sub _Session {
	my $self = shift();
	if (@_ == 0) {
		return $self->{_session}
	} elsif (@_ == 1){
		return $self->{_session}{$_[0]}
	} elsif (@_ == 2){
		return ($self->{_session}{$_[0]} = $_[1])
	} else {
		croak "Too many arguments for ->_Session()"
	}
}

use vars qw($AUTOLOAD);
sub AUTOLOAD {
	my $self = shift;
	my ($class, $meth) = ($AUTOLOAD =~ /^(.*)::([\w_]+)$/);

	return $self->_error("No such method: $AUTOLOAD")
		unless (defined $self->{SETTINGS}{_methods}{$meth});

	my $Meth = $self->{SETTINGS}{_methods}{$meth};

	my ($sub,$from_cache);
	if ($self->{SETTINGS}{_code_cache}) {
		require Digest::MD5;
		my $code = Digest::MD5::md5_hex(
			"args=" . join( $;, @{$Meth->{args}})
			."sql=" . $Meth->{sql}
			."return=" . $Meth->{return}
			.$; . $Meth->{noprepare} . $; . $Meth->{noquote}
		);
		$Meth->{cache_code} = $code;
		if (ref $self->{SETTINGS}{_code_cache} eq 'CODE') {
			$sub = $self->{SETTINGS}{_code_cache}->($meth.$;.$Meth->{cache_code});
			if ($sub) {
				($sub) = ($sub =~ /(.*)/s);
				$from_cache = 1;
				goto EVAL_AND_RUN;
			}
		} elsif (ref $self->{SETTINGS}{_code_cache} eq 'HASH') {
			$sub = $self->{SETTINGS}{_code_cache}{$meth.$;.$Meth->{cache_code}};

			if ($sub) {
				($sub) = ($sub =~ /(.*)/s);
				$from_cache = 1;
				goto EVAL_AND_RUN;
			}
		} else {
			# it's a directory, import() tests that
			my $file = $self->{SETTINGS}{_code_cache} . '/' . $meth . '-' . $code . '-generated.pl';
			if (-e $file) {
				my $age = time - (stat($file))[9];
				if ($age > $self->{SETTINGS}{_code_cache_max_age}) {
print STDERR "CODE CACHE STALE for $meth in $self->{SETTINGS}{_code_cache}\n" if DEBUG;
					unlink $file;
				} else {
print STDERR "CODE CACHE HIT for $meth in $self->{SETTINGS}{_code_cache}\n" if DEBUG;
					$sub = do { local $/; open my $IN, '<', $file; <$IN>};
					($sub) = ($sub =~ /(.*)/s);
					$from_cache = 1 if $sub;
					goto EVAL_AND_RUN if $sub;
				}
			}
print STDERR "CODE CACHE MISS for $meth in $self->{SETTINGS}{_code_cache}\n" if DEBUG;
		}
	}

	$Meth->{case} = $self->{DEFAULTS}{_case} if ! defined $Meth->{case};

	unless (exists $Meth->{sql}) {
		print STDERR "[DBIx::Declare] DEBUG: infering the SQL for $meth\n" if DEBUG;
		$Meth->{sql} = $self->_InferSQL( $meth, $Meth); # this is expected to croak()/die() if it can't infer the SQL statement to use
	} elsif ($Meth->{case} eq 'insensitive' and @{$Meth->{args}}) {
		$_ = lc($_) for @{$Meth->{args}};
	}

	$Meth->{args} = [] unless $Meth->{args};

	my %Template;
	$Template{fullname} = $meth;#$AUTOLOAD;
	$Template{name} = $meth;
	$Template{class} = $class;
	$Template{arg_count} = ($Meth->{in_args} ? @{$Meth->{in_args}} : @{$Meth->{args}});
	$Template{arg_max_id} = $Template{arg_count}-1;
	$Template{single_arg} = ($Template{arg_count} == 1) ? $Meth->{args}[0] : undef;
	$Template{arg_list} = "qw(@{$Meth->{args}})";
	$Template{in_arg_list} = $Template{arg_list};
	$Template{in_arg_arr} = 'arg_list';
	if (exists $Meth->{in_args}) {
		$Template{in_arg_list} = "qw(@{$Meth->{in_args}})";
		if ($Template{in_arg_list} ne $Template{arg_list}) {
			$Template{in_arg_arr} = 'in_arg_list';
			$Template{input2args} = join ',', do {
				my $i=0;
				my %map = map {$_ => $i++} @{$Meth->{args}};
				grep defined($_), map $map{$_}, @{$Meth->{in_args}}; # in case some are missing in the @in_args
			};
		}
	}
	$Template{warn_useless} = int(!!(defined($Meth->{warn_useless}) ? $Meth->{warn_useless} : $self->{'_warn_useless'})); # I need either 0 or 1, not undef!
	$Template{has_cache} = exists $Meth->{cache};

	$Template{has_defaults} = exists $Meth->{defaults};
	$Template{has_out_args} = (exists $Meth->{out_args} and $Meth->{out_args} and %{$Meth->{out_args}});
	delete $Template{single_arg} if ($Template{has_out_args});
	$Template{lc} = ($Meth->{case} eq 'insensitive' ? 'lc' : '');

	$Meth->{in_args} = $Meth->{args} unless $Meth->{in_args};

	{
		my $i=0;
		my %map = map {$_ => $i++} @{$Meth->{args}};

		$i=0;
		for (grep exists $Meth->{out_args}{$_}, @{$Meth->{in_args}}) {
			$Meth->{out_args}{$_}[3] = $i++;
			$Meth->{out_args}{$_}[4] = $map{$_};
		}
		for (grep( (exists($Meth->{out_args}{$_}) && !defined($Meth->{out_args}{$_}[3])), @{$Meth->{args}})) {
			$Meth->{out_args}{$_}[3] = $i++;
			$Meth->{out_args}{$_}[4] = $map{$_};
		}
	}

	if ($Meth->{'out_args'} and %{$Meth->{'out_args'}}) {	# make sure we know where in the list of arguments are the output ones
		my $pos = '';
		my $args = $Meth->{in_args} || $Meth->{args};
		for my $id (0 .. $#$args) {
			if (exists $Meth->{'out_args'}{ $args->[$id] }) {
				$pos .= $id . ',';
			}
		}
		chop $pos;
		$Template{output_positions} = $pos;
	}

	$sub = <<'*END*';
{
my $sql = <<'*SqL-^43#*';
<<= $Meth->{sql}>>
*SqL-^43#*
my $defaults = <<= local $Data::Dumper::Terse=1; local $Data::Dumper::Indent =0; Data::Dumper::Dumper($Meth->{defaults});>>; 				<<IF has_defaults>>
my @arg_list = <<arg_list>>;																																						<<IF @{$Meth->{args}} > 1 or $Template{has_out_args}>>
my @in_arg_list = <<in_arg_list>>;																																				<<IF input2args>>
my $out_args = <<= local $Data::Dumper::Terse=1; local $Data::Dumper::Indent =0; Data::Dumper::Dumper($Meth->{out_args});>>; 				<<IF has_out_args>>
sub <<fullname>> {
	my $self = shift();
	$self->_error(); # clear the flag
*END*

	if (! @{$Meth->{args}}) { # no parameters
		if ($Template{warn_useless}) {
			$sub .= <<'*END*';
	if (@_) {
		carp "Too many arguments for method <<name>>. No arguments expected.";
	}
*END*
		}
	} elsif ($Template{output_positions} eq '') {
		if ($Template{single_arg}) {
			$sub .= <<'*END*';
	my @args;
	if (@_ <= 1 or ($_[0] eq '_' and shift(@_)) or $_[0] !~ /^-[a-zA-Z_]\w*$/) {
		@args = @_;
		if (@args < 1) {
			if (exists $self->{session} and exists $self->{session}{<<single_arg>>}) {
				push @args, $self->{session}{<<single_arg>>}
			} elsif (exists $defaults->{<<single_arg>>}) {																					<<IF has_defaults>>
				push @args, $defaults->{<<single_arg>>}																					<<IF has_defaults>>
			} elsif (exists $out_args->{<<single_arg>>}) {																					<<IF has_out_args>>
				# output parameter may be missing. Maybe we are not interested, maybe the return is $$					<<IF has_out_args>>
				push @args, undef																													<<IF has_out_args>>
			} else {
				return $self->_error("required argument '<<single_arg>>' not specified")
			}
		} elsif (@args > <<arg_count>>) {																										<<IF warn_useless>>
			carp "useless arguments for method <<name>>! expected <<arg_count>>, got " . scalar(@args);				<<IF warn_useless>>
		}
		$#args = 0;
	} else { #named parameters
		my %args = @_;																			<<IF !lc>>
		my %args; while (@_) { $args{lc shift()] = shift() };							<<IF lc>>
		if (exists $args{"-<<single_arg>>"}) {
			push @args, $args{"-<<single_arg>>"};
			delete $args{"-<<single_arg>>"};
		} elsif (exists $self->{session} and exists $self->{session}{<<single_arg>>}) {
			push @args, $self->{session}{<<single_arg>>}
		} elsif (exists $defaults->{<<single_arg>>}) {																						<<IF has_defaults>>
			push @args, $defaults->{<<single_arg>>}																						<<IF has_defaults>>
		} elsif (exists $out_args->{<<single_arg>>}) {																						<<IF has_out_args>>
			# output parameter may be missing. Maybe we are not interested, maybe the return is $$						<<IF has_out_args>>
			push @args, undef																														<<IF has_out_args>>
		} else {
			return $self->_error("required argument '<<single_arg>>' not specified")
		}
		if (%args) {																																	<<IF warn_useless>>
			carp "useless arguments for method <<name>>: ".join(', ', keys %args);												<<IF warn_useless>>
		}																																					<<IF warn_useless>>
	}
*END*
		} else {
			$sub .= <<'*END*';
	my @args;
	if (@_ <= 1 or ($_[0] eq '_' and shift(@_)) or $_[0] !~ /^-[a-zA-Z_]\w*$/) {
		@args = @_;
		if (@args < <<arg_count>>) {
			foreach my $arg (map $<<in_arg_arr>>[$_], (scalar(@args)..<<arg_max_id>>)) {
				if (exists $self->{session} and exists $self->{session}{$arg}) {
					push @args, $self->{session}{$arg}
				} elsif (exists $defaults->{$arg}) {					<<IF has_defaults>>
					push @args, $defaults->{$arg}					<<IF has_defaults>>
				} elsif (exists $out_args->{$arg}) {																							<<IF has_out_args>>
					# output parameter may be missing. Maybe we are not interested, maybe the return is $$				<<IF has_out_args>>
					push @args, undef																												<<IF has_out_args>>
				} else {
					return $self->_error("required argument '$arg' not specified")
				}
			}
		} elsif (@args > <<arg_count>>) {																										<<IF warn_useless>>
			carp "useless arguments for method <<name>>! expected <<arg_count>>, got " . scalar(@args);				<<IF warn_useless>>
		}
		$#args = <<arg_max_id>>; # at most arg_count arguments! Can't pass more to ->execute()
		{my @tmp; @tmp[<<input2args>>] = @args; @args = @tmp}							<<IF input2args>>
	} else { #named parameters
		my %args = @_;																			<<IF !lc>>
		my %args; while (@_) { $args{lc shift()] = shift() };							<<IF lc>>
		foreach my $arg (<<arg_list>>) {
			if (exists $args{"-$arg"}) {
				push @args, $args{"-$arg"};
				delete $args{"-$arg"};
			} else {
				if (exists $self->{session} and exists $self->{session}{$arg}) {
					push @args, $self->{session}{$arg}
				} elsif (exists $defaults->{$arg}) {					<<IF has_defaults>>
					push @args, $defaults->{$arg}					<<IF has_defaults>>
				} elsif (exists $out_args->{$arg}) {																							<<IF has_out_args>>
					# output parameter may be missing. Maybe we are not interested, maybe the return is $$				<<IF has_out_args>>
					push @args, undef																												<<IF has_out_args>>
				} else {
					return $self->_error("required argument '$arg' not specified")
				}
			}
		}
		if (%args) {																																	<<IF warn_useless>>
			carp "useless arguments for method <<name>>: ".join(', ', keys %args);												<<IF warn_useless>>
		}																																					<<IF warn_useless>>
	}
*END*
		}
	} else { # there are output parameters

		$sub .= <<'*END*';
	my (@args, $named_params);
	if (@_ <= 1 or ($_[0] eq '_' and shift(@_)) or $_[0] !~ /^-[a-zA-Z_]\w*$/) {
		for (<<output_positions>>) {
			$self->_error( "The $<<in_arg_arr>>[$_] (argument no $_) is an OUTPUT parameter, you have to pass a variable!") if  Scalar::Util::readonly( $_[ $_ ]);
		}
		@args = @_;
		if (@args < <<arg_count>>) {
			foreach my $arg (map $<<in_arg_arr>>[$_], (scalar(@args)..<<arg_max_id>>)) {
				if (exists $self->{session} and exists $self->{session}{$arg}) {
					push @args, $self->{session}{$arg}
				} elsif (exists $defaults->{$arg}) {					<<IF has_defaults>>
					push @args, $defaults->{$arg}					<<IF has_defaults>>
				} elsif (exists $out_args->{$arg}) {
					# output parameter may be missing. Maybe we are not interested, maybe the return is $$
					push @args, undef
				} else {
					return $self->_error("required argument '$arg' not specified")
				}
			}
		}
		{my @tmp; @tmp[<<input2args>>] = @args; @args = @tmp}							<<IF input2args>>
		$named_params = 0;
	} else { #named parameters
		for (my $i=1; $i <= $#_; $i+=2) {
			next unless Scalar::Util::readonly($_[$i]);
			my $arg = substr($_[$i-1], 1);
			Carp::croak "The $arg is an OUTPUT parameter, you have to pass a variable!" if exists $out_args->{$arg};
		}
		my %args = @_;																							<<IF !lc>>
		my %args; for(my $i=0;$i<=$#_;$i+=2) { $args{lc $_[$i]} = $_[$i+1] };				<<IF lc>>
		foreach my $arg (<<arg_list>>) {
			if (exists $args{"-$arg"}) {
				push @args, $args{"-$arg"};
				delete $args{"-$arg"};
			} else {
				if (exists $self->{session} and exists $self->{session}{$arg}) {
					push @args, $self->{session}{$arg}
				} elsif (exists $defaults->{$arg}) {					<<IF has_defaults>>
					push @args, $defaults->{$arg}					<<IF has_defaults>>
				} elsif (exists $out_args->{$arg}) {
					# output parameter may be missing. Maybe we are not interested, maybe the return is $$
					push @args, undef
				} else {
					return $self->_error("required argument '$arg' not specified")
				}
			}
		}
		if (%args) {																								<<IF warn_useless>>
			carp "useless arguments for method <<name>>: ".join(', ', keys %args);			<<IF warn_useless>>
		}																												<<IF warn_useless>>
		$named_params = 1;
	}
*END*
	}

	if ($Template{has_cache}) {
		$sub .= <<'*END*';
	{
		my $key = join $;, @args;
		$self->{cache}{<<name>>} = $<<class>>::SETTINGS{_methods}{<<name>>}{cache}->() unless exists $self->{cache}{<<name>>};								<<IF ref $Meth->{cache} eq 'CODE'>>
		if (my $ret = $self->{cache}{<<name>>}{$key}) {
			if (ref $ret eq 'ARRAY'){
				return (wantarray ? @$ret : [@$ret]);
			} elsif  (ref $ret eq 'HASH'){
				return (wantarray ? %$ret : {%$ret});
			} else {
				return $ret
			}
		}
	}
*END*
	}

	$sub .= <<'*END*';
	unless (exists $self->{_dbh} && ref $self->{_dbh} eq 'DBI::db') { return $self->_error("DBI handle missing"); }
	if (! $self->{_dbh}->{'Active'}) { # the connection is broken
		$self->_Connect();
	}

*END*
	if ($Meth->{'noprepare'}) {
		if (@{$Meth->{args}}) {
			if ($Meth->{'noquote'}) {
				$sub .= <<'*END*';
	my $sql = $sql;
	$sql =~ s/\?+?/(shift @args)/oe while (@args);
	my $rv = $self->{_dbh}->do($sql) or return $self->_error('<<name>> execution failed', $DBI::errstr, $sql);
*END*
			} else {
				$sub .= <<'*END*';
	my $rv = $self->{_dbh}->do( $sql, undef, @args) or return $self->_error('<<name>> execution failed', $DBI::errstr, $sql);
*END*
			}
		} else { # noprepare and no arguments
			$sub .= <<'*END*';
	my $rv = $self->{_dbh}->do( $sql) or return $self->_error('<<name>> execution failed', $DBI::errstr, $sql);
*END*
		}
	} else {
		$sub .= <<'*END*';

	my $sth = $self->{'_statements'}{<<name>>};
	if (! $sth) {
		print STDERR "[DBIx::Declare] DEBUG: preparing method <<name>>: $sql\n" if DEBUG;

		$sth = $self->{'_statements'}{<<name>>} =  $self->{_dbh}->prepare($sql)
			or return $self->_error("<<name>> prepare failed", $DBI::errstr, $sql);
		#																																															<<IF has_out_args>>
		$self->{_bound}{<<name>>} = [];																																				<<IF has_out_args>>
		while (my ($param, $Param) = each %$out_args) {																														<<IF has_out_args>>
			$self->{_bound}{<<name>>}[ $Param->[3] ] = undef;																												<<IF has_out_args>>
			$sth->bind_param_inout( $Param->[4]+1, \$self->{_bound}{<<name>>}[ $Param->[3] ], $Param->[0], { TYPE => $Param->[1] } )		<<IF has_out_args>>
				or return $self->_error( "Failed to bind param $param for method <<name>>", $sth->errstr, $sth);													<<IF has_out_args>>
		}																																															<<IF has_out_args>>
	}
*END*

		if (! @{$Meth->{args}}) { # no parameters
			$sub .= <<'*END*';
	print STDERR "[DBIx::Declare] DEBUG: executing <<name>>()\n" if DEBUG;
	my $rv = $sth->execute() or return $self->_error("<<name>> execution failed", $sth->errstr, $sth);
*END*
		} elsif (!exists $Meth->{'out_args'} or !$Meth->{'out_args'} or ! %{$Meth->{'out_args'}}) { # no output parameters
			$sub .= <<'*END*';
	print STDERR "[DBIx::Declare] DEBUG: executing <<name>>( '" . join("', '", @args) . "')\n" if DEBUG;
	my $rv = $sth->execute(@args) or return $self->_error("<<name>> execution failed", $sth->errstr, $sth);
*END*
		} else {
			$sub .= <<'*END*';
	my $bound = $self->{_bound}{"<<name>>"};
*END*
			my $bound_idx = 0;
			for (0 .. $#{$Meth->{args}}) {
				if (exists $Meth->{'out_args'}{ $Meth->{args}[$_] }) {
					$sub .= qq{	\$bound->[$bound_idx] = \$args[$_];\n} unless $Meth->{output_only};
					$bound_idx++;
				} else {
					$sub .= qq{	\$sth->bind_param( $_+1, \$args[$_]) or return \$self->_error( "Failed to bind param $Meth->{args}[$_] for method <<name>>", \$sth->errstr, \$sth);\n};
				}
			}
			$sub .= <<'*END*';
	print STDERR "[DBIx::Declare] DEBUG: executing <<name>>( '" . join("', '", @args) . "') via bound variables\n" if DEBUG;
	my $rv = $sth->execute() or return $self->_error("<<name>> execution failed", $sth->errstr, $sth);
*END*
		}
	}
	$sub .= <<'*END*';
	my @ret;
*END*

	if ($Meth->{'noprepare'}) {
		$sub .= <<'*END*';
	return $rv;
*END*
	} elsif (ref $RETURN_CODE{$Meth->{return}}[0]) {
		$sub .= $RETURN_CODE{$Meth->{return}}[0]->($Meth)
	} else {
		$sub .= $RETURN_CODE{$Meth->{return}}[0]
	}

	if ($Template{has_cache}) {
		$sub .= <<'*END*';
	{
		my $key = join $;, @args;
		$self->{cache}{<<name>>}{$key} = \@ret;
	}
*END*
	}

	if ($Template{output_positions} ne '') {
		$sub .= <<'*END*';
	if ($named_params) {
		for (my $id = 0; $id < $#_; $id+=2) {
			my $name = <<lc>> $_[$id];
			substr($name,0,1,''); # pohc($name)
			if (exists $out_args->{$name}) {
				$_[$id+1] = $bound->[$out_args->{$name}[3]]
			}
		}
	} else {
		no warnings 'syntax';
		@_[<<output_positions>>] = @$bound;
	}
	$_ = undef for @$bound;
*END*
	}

	$sub .= $RETURN_CODE{$Meth->{return}}[1] unless $Meth->{'noprepare'};
	$sub .= <<'*END*';
}
}
1;
*END*

	$sub =~ s{^(.*?)\s*<<IF\s+(.*?)>>\s*\n}{
		my ($line, $cond) = ($1,$2);
		if ($cond =~ /^(\w+)\s*$/) {
			if ($Template{$1}) {$line."\n"} else {''}
		} elsif ($cond =~ /^!\s*(\w+)\s*$/) {
			if (! $Template{$1}) {$line."\n"} else {''}
		} else {
			if (eval $cond) {$line."\n"} else {''}
		}
	}gem;
	$sub =~ s/\n\n\n+/\n\n/g;
	$sub =~ s/<<(\w+)>>/$Template{$1}/g;
	$sub =~ s/<<=(.+?)>>/eval $1/ge;

EVAL_AND_RUN:	{
#		no warnings 'closure';
		my $ret;
		{
			$ret = eval "package $class;\n" . $sub;
		}
		if ($ret) {
			print STDERR "===================code=for=$meth==================\npackage $class;\n$sub\n=============================================\n\n" if DEBUG or $self->{_show_generated_code};

			if ($self->{SETTINGS}{_code_cache} and !$from_cache) {
				if (ref $self->{SETTINGS}{_code_cache} eq 'CODE') {
					$self->{SETTINGS}{_code_cache}->($meth.$;.$Meth->{cache_code}, $sub);
				} elsif (ref $self->{SETTINGS}{_code_cache} eq 'HASH') {
					$self->{SETTINGS}{_code_cache}{$meth.$;.$Meth->{cache_code}} = $sub;
				} else {
print STDERR "CODE CACHE SET for $meth in $self->{SETTINGS}{_code_cache}\n" if DEBUG;
					#assuming it's a directory
					my $file = $self->{SETTINGS}{_code_cache} . '/' . $meth . '-' . $Meth->{cache_code} . '-generated.pl';
					open my $OUT, '>', $file or $self->_error("Failed to store $meth in code cache!", $^E);
					print $OUT $sub;
					close $OUT;
				}
			}

		} else {

			if ($self->{SETTINGS}{_code_cache}) {
				if (! ref $self->{SETTINGS}{_code_cache}) {
					#assuming it's a directory
					my $file = $self->{SETTINGS}{_code_cache} . '/' . $meth . '-' . $Meth->{cache_code} . '-invalid.pl';
					open my $OUT, '>', $file or $self->_error("Failed to store $meth in code cache!", $^E);
					print $OUT $sub;
					close $OUT;
				}
			}

			croak "Failed to compile the generated subroutine: $@\n\n===================code=for=$meth===================\npackage $class;\n$sub\n=============================================\n\n";
		}
	}

	unshift(@_,$self);
	goto &$AUTOLOAD;
}

sub DESTROY ($) {
	my $self = shift;
	#remember to bury statement handles
	if (defined $self->{'_statements'}) {
		foreach (keys %{$self->{'_statements'}}) {
			#finish the sth
			$self->{'_statements'}{$_}->finish;
			print STDERR "[DBIx::Declare] DEBUG: meth DESTROY - finished _sth_".$_." handle\n" if DEBUG;
		}
	}
	#and hang up if we have a connection
	if (defined $self->{'_dbh'}) { $self->_Disconnect(); }
}

sub _Connect {
	my $self = shift;
	return $self->{_dbh} if $self->{_dbh} and $self->{_dbh}{Active};

	delete $self->{'_statements'}; # need to forget we ever prepared and bound anything
	delete $self->{'_bound'};

	my $data_source =	$self->{'_data_source'} || $self->{DEFAULTS}{'_data_source'};
	my $user 	= 	$self->{'_user'} || $self->{DEFAULTS}{'_user'};
	my $auth  	= 	$self->{'_pass'} || $self->{DEFAULTS}{'_pass'};
	my $attr  	= 	$self->{'_attr'} || $self->{DEFAULTS}{'_attr'};

	if (ref($_[-1]) eq 'HASH') {
		$attr = { %$attr, %{pop(@_)}};
	}
	if (@_) {
		if ($_[0] =~ /^dbi:/i) {
			$data_source = shift(@_);
		} else {
			$data_source =~ s/^(dbi:[^:]+).*$/$1/;
			$data_source .= shift(@_);
		}
	}
	$user = shift() if @_;
	$auth = shift() if @_;

	@{$self}{qw(_data_source _user _pass _attr)} = ($data_source, $user, $auth, $attr) unless $self->{_data_source};

	print STDERR "[DBIx::Declare] DEBUG: DBIx::Declare doing: DBI->connect($data_source, $user, $auth, $attr);\n" if DEBUG;
	my $dbh  = DBI->connect($data_source, $user, $auth, $attr) or return $self->_error("Connection failure", $DBI::errstr);

	$self->{_dbh} = $dbh;
	$self->_init_connection;

	return $dbh;
}

sub _init_connection {
}

sub _Commit {
	my $self = shift;
	return $self->_error("Can't commit without an active connection") unless $self->{_dbh} and $self->{_dbh}{Active};
	$self->{_dbh}->commit();
}
sub _Rollback {
	my $self = shift;
	return $self->_error("Can't rollback without an active connection") unless $self->{_dbh} and $self->{_dbh}{Active};
	$self->{_dbh}->rollback();
}
sub _BeginTrans {
	my $self = shift;
	return $self->_error("Can't begin a transaction without an active connection") unless $self->{_dbh} and $self->{_dbh}{Active};
	$self->{_dbh}->begin_work();
}
sub _BeginWork; *_BeginWork = \&_BeginTrans;

sub _Disconnect {
	my $self = shift;
	my $dbh = $self->{'_dbh'};

	unless (defined $dbh) { return 1 }

	if ($self->{'_statements'} and %{$self->{'_statements'}}) {
		%{$self->{'_statements'}} = ();
	}

	if (!$dbh->disconnect) {
		return $self->_error("Disconnect failed", $dbh->errstr);
	} else {
		print STDERR "[DBIx::Declare] DEBUG: Disconnected dbh\n" if DEBUG;
		delete $self->{'_statements'};
		delete $self->{'_bound'};
		delete $self->{'_dbh'};
	}
	return 1;
}

sub _Connected {
	my $self = shift;
	return $self->{'_dbh'} if $self->{'_dbh'} and $self->{'_dbh'}{'Active'};
	return;
}

sub _format_error_details {
	my ($self, $statement, $params, $meth) = @_;

	if ($params and %{$params}) {
		my $num_only = 1;
		my $max_num = -1;
		for (keys %{$params}) {
			if (! /^\d+$/) {
				$num_only = 0;
				last;
			} else {
				$max_num = $_ if $max_num < $_
			}
		}
		if ($num_only) {
			$statement .= "\n     (" . join( ', ', map $self->{_dbh}->quote($params->{$_}), (1 .. $max_num)) . ')';
		} else {
			$statement .= "\n     (" . join( ', ', map "$_: " . $self->{_dbh}->quote($params->{$_}), (sort keys %$params)) . ')';
		}
	}
	return $statement;
}

sub _error {
	my ($self, $my_msg, $db_msg, $sth) = @_;
	if (! defined $my_msg) {
		delete $self->{'errorstate'};
		$self->{'errormessage'} = "[no error]";
		return;
	}
	my $on_error = $self->{'_on_errors'}; # method specific or global

	my $msg;

	if ($self->{'_format_errors'}) {
		$msg = $self->{'_format_errors'}->( $self, $my_msg, $db_msg,
			( $self->{'_error_details'}  ? ((ref($sth) ? $sth->{Statement} : $sth), (ref($sth) ? $sth->{ParamValues} : {}), (caller(1))[3] ) : ())
		) or return;

	} else {
		$msg = $my_msg;
		$msg .= ': ' . $db_msg if $db_msg ne '';

		if ($self->{'_error_details'}) { #  we can get the values for the placeholders from $sth->{ParamValues} (hash ref, read-only) and the statement from $sth->{Statement}
			my $details = (ref($sth) ? $sth->{Statement} : $sth);
			$details =~ s/\s+$//s;
			$details = $self->_format_error_details( $details, $sth->{ParamValues}, (caller(1))[3], $sth->{ParamTypes});
			if ($details ne '') {
				chomp($details);
				$details =~ s/\n/\n     /gs;
				$msg .= "\n in: ". $details;
			}
		}
	}

	$self->{'errorstate'} = 1;
	$self->{'errormessage'} = $msg;
	if (ref($on_error eq 'CODE')) {
		return $on_error->($msg)
	} elsif ($on_error eq 'die') {
		die $msg."\n";
	} elsif ($on_error eq 'croak') {
		croak $msg;
	} elsif ($on_error =~ /\b_ERROR_\b/) {
		$on_error =~ s/\b_ERROR_\b/$msg/g;
		if ($on_error =~ /\n$/) {
			die $on_error;
		} else {
			carp $on_error;
		}
	} else {
		carp "[DBIx::Declare] ERROR: " . $msg;
	}
	return;
}

sub _Is_error ($) {
	my $self = shift;
	return (defined $self->{'errorstate'});
}

package DBIx::Declare::DB::mssql;
use Carp;
our @ISA = qw(DBIx::Declare::DB::unknown);
use constant DEBUG => DBIx::Declare::DEBUG;

sub _init_connection {
	my $self = shift;
	$self->{_dbh}->do('SET NOCOUNT ON');
}

our (%TYPEMAP, %SIZEMAP);
sub _load_typemap {
	my $self = shift;
	my $info = $self->{_dbh}->type_info_all() or croak "Can't load the type map!\n";

	my %map = %{shift(@$info)};
	foreach my $type (@$info) {
		$TYPEMAP{$type->[$map{TYPE_NAME}]} = $type->[$map{DATA_TYPE}];
		$SIZEMAP{$type->[$map{TYPE_NAME}]} = $type->[$map{COLUMN_SIZE}];
	}
}

sub _InferSQL {
	my ($self, $meth, $Meth) = @_;

	my $proc = $Meth->{call} || $meth;
	print STDERR "[DBIx::Declare] DEBUG: infering the SQL for a mssql procedure $proc\n" if DEBUG;

	if (! $Meth->{args}) {
	my $sth = $self->{'_statements'}{_mssql_schema_sth};
	if (! $sth) {
		$sth = $self->{_dbh}->prepare(<<"*END*");
SELECT Parameter_name, Parameter_mode, Data_type, Character_maximum_length
  FROM INFORMATION_SCHEMA.PARAMETERS
 WHERE Specific_catalog = DB_NAME() and Specific_schema = 'dbo' and Specific_name = ?
   and Is_result = 'NO'
 ORDER BY Ordinal_position
*END*
			$self->{'_statements'}{_mssql_schema_sth} = $sth;
		}
		$sth->execute($proc) or die "$proc is not an existing procedure!\n";
		my $params = $sth->fetchall_arrayref();

		$Meth->{'args'} = [];
		my $lc = (defined $Meth->{case} ? $Meth->{case} : $self->{DEFAULTS}{_case});
		$lc = ($lc eq 'lowercase' or $lc eq 'insensitive');
		foreach my $param (@$params) {
			(my $name = $lc ? lc($param->[0]) : $param->[0]) =~ s/^\@//;
			push @{$Meth->{'args'}}, $name;

			if ($param->[1] eq 'INOUT') {
				$self->_load_typemap() unless keys %TYPEMAP;
				$Meth->{'out_args'}->{$name} = [ (defined $param->[3] ? $param->[3]+0 : $SIZEMAP{$param->[2]}+1), $TYPEMAP{$param->[2]} || DBI::SQL_VARCHAR, (defined $param->[3] ? "$param->[2]($param->[3])" : $param->[2])]; # [ maxlength, type, value(for binding), type_string]
			}
		}
	}

	my $sql;
	my $count = scalar(@{$Meth->{args}});
	if ($Meth->{return} eq '$R' or $Meth->{return_value}) {
		if ($count) {
			$sql = "{? = CALL dbo.$proc( ?" . (", ?" x ($count-1)) . ')}';
		} else {
			$sql = "{? = CALL dbo.$proc}";
		}

		my $retval_name = $Meth->{return_value} || '_reTurn_VaLue';
		$Meth->{out_args}{$retval_name} = [16, DBI::SQL_INTEGER, 'int'];
		$Meth->{defaults}{$retval_name} = undef;

		$Meth->{in_args} = [@{$Meth->{args}}] unless $Meth->{in_args};
		push @{$Meth->{in_args}}, $retval_name if $Meth->{return_value} and ! grep $Meth->{return_value} eq $_, @{$Meth->{in_args}}; # add at the end f the input if it's not there already, but only for the return_value=>'name' option
		unshift @{$Meth->{args}}, $retval_name;
	} else {
		if ($count) {
			$sql = "{CALL dbo.$proc( ?" . (", ?" x ($count-1)) . ")}";
		} else {
			$sql = "{CALL dbo.$proc}";
		}
	}
	print STDERR "[DBIx::Declare] DEBUG: sql=$sql\n" if DEBUG;

	return $sql;
}

sub _GetIncrement {
	my $self = shift();
	unless (exists $self->{_dbh} && ref $self->{_dbh} eq 'DBI::db') { return $self->_error("DBI handle missing"); }
	if (! $self->{_dbh}->{'Active'}) { # the connection is broken
		$self->_error("Connection broken, cannot get the last increment value");
	}

	if (! exists $self->{'_statements'}{_GetIncrement}) {
		$self->{'_statements'}{_GetIncrement} = $self->{_dbh}->prepare('SELECT @@IDENTITY as [Inc]')
			or return $self->_error("_GetIncrement prepare failed", $self->{_dbh}->errstr);
	}
	my $sth = $self->{'_statements'}{_GetIncrement};
	$sth->execute() or return $self->_error("GetIncrement failed: ", $sth->errstr);
	my $row = $sth->fetchrow_arrayref() or die $sth->err;
	return $row->[0];
}

sub _format_error_details {
	my ($self, $statement, $params, $meth, $types) = @_;

	if ($types and grep {$_->{TYPE} == DBI::SQL_WVARCHAR || $_->{TYPE} == DBI::SQL_WCHAR || $_->{TYPE} == DBI::SQL_WLONGVARCHAR} values %$types) { #SQL_W... - UNICODE
		require Encode;
		my $newparams = {}; # we cannot modify the original hash!
		for my $id (keys %$params) {
			my $type = $types->{$id}{TYPE};
			if ($type == DBI::SQL_WVARCHAR || $type == DBI::SQL_WCHAR || $type == DBI::SQL_WLONGVARCHAR) {
				$newparams->{$id} = Encode::decode( 'UCS-2LE', $params->{$id})
			} else {
				$newparams->{$id} = $params->{$id}
			}
		}
		$params = $newparams;
	}

	if ($statement =~ /^\s*\{\s*(\?\s*=\s*)?CALL ([^\(\}]+).*}$/) {
		# stored procedure, auto
		my ($has_return, $proc) = ($1,$2);
		$meth =~ s/^.*:://;
		my $Meth = $self->{SETTINGS}{_methods}->{$meth}
			or die "The method $meth was not found. Please do not call the _format_error_details() directly!\n";
		my $retval_name = $Meth->{return_value} || '_reTurn_VaLue';
		my $dbh = $self->{_dbh};

		if ($Meth->{'out_args'}) {
			my $OutArgs = $self->{SETTINGS}{_methods}{$meth}{'out_args'};

			$statement = 'Declare ' . join(', ', map "\@$_ $OutArgs->{$_}[2]", sort keys %{$OutArgs}) . ";\n";
			foreach my $arg (sort keys %{$OutArgs}) {
				next unless defined $params->{$OutArgs->{$arg}[3]+1};
				my $v = $dbh->quote($params->{$OutArgs->{$arg}[3]+1});
				$statement .= "SET \@$arg = $v;\n";
			}
			if ($has_return) {
				$statement .= "EXEC \@$retval_name = $proc " . join(", ", map {
					if (exists($OutArgs->{$_})) {
						"\@$_ = \@$_ OUTPUT"
					} else {
						my $v = $dbh->quote($params->{$_});
						"\@$_ = $v"
					}
				} grep $_ ne $retval_name, @{$Meth->{args}});
			} else {
				$statement .= "EXEC $proc " . join(", ", map {
					if (exists($OutArgs->{$_})) {
						"\@$_ = \@$_ OUTPUT"
					} else {
						my $v = $dbh->quote($params->{$_});
						"\@$_ = $v"
					}
				} @{$Meth->{args}});
			}
			$statement .= "\nSELECT " . join(', ', map "\@$_", sort keys %{$OutArgs}) . ";\n";
		} else {
			$statement .= "\nEXEC $proc " . join(", ", map {my $v = $dbh->quote($params->{$_}); "\@$_ = $v"} sort keys %$params);
		}
	} else {
		if ($params and %{$params}) {
			my $num_only = 1;
			my $max_num = -1;
			for (keys %{$params}) {
				if (! /^\d+$/) {
					$num_only = 0;
					last;
				} else {
					$max_num = $_ if $max_num < $_
				}
			}
			if ($num_only) {
				my @params = map $params->{$_}, (1 .. $max_num);
				$statement =~ s{\G((?>'[^']*'|--.*?\n|/\*.*?\*/|[^?])+|\?)}{if ($1 eq '?') {$self->{_dbh}->quote(shift @params)} else {$1}}ge;
			} else {
				$statement .= "\n     (" . join( ', ', map "$_: " . $self->{_dbh}->quote($params->{$_}), (sort keys %$params)) . ')';
			}
		}
	}
	chomp($statement);
	return $statement;
}


package DBIx::Declare::DB::sqlite;
use Carp;
our @ISA = qw(DBIx::Declare::DB::unknown);
use constant DEBUG => DBIx::Declare::DEBUG;

sub _GetIncrement {
	my $self = shift();
	unless (exists $self->{_dbh} && ref $self->{_dbh} eq 'DBI::db') { return $self->_error("DBI handle missing"); }
	if (! $self->{_dbh}->{'Active'}) { # the connection is broken
		$self->_error("Connection broken, cannot get the last increment value");
	}

	return $self->{_dbh}->func('last_insert_rowid');
}


package DBIx::Declare::DB::mysql;
use Carp;
our @ISA = qw(DBIx::Declare::DB::unknown);
use constant DEBUG => DBIx::Declare::DEBUG;

sub _GetIncrement {
	my ($self, %arg) = @_;
	my $sth = $arg{sth} or croak "->_GetIncrement() called without the statement handle";
	if (defined $sth->{'mysql_insertid'}) {
		return ($sth->{'mysql_insertid'});
	} else {
		return; # $self->_error("could not get mysql_insertid from mysql DBD");
	}
};

package DBIx::Declare::DB::pg;
use Carp;
our @ISA = qw(DBIx::Declare::DB::unknown);
use constant DEBUG => DBIx::Declare::DEBUG;

sub _GetIncrement {
	my ($self, %arg) = @_;
	my $sth = $arg{sth} or croak "->_GetIncrement() called without the statement handle";
	if (defined $sth->{'pg_oid_status'}) {
		return ($sth->{'pg_oid_status'});
	} else {
		return; # $self->_error("could not get pg_oid_status from Pg DBD");
	}
};

package DBIx::Declare::Infer;
use Carp;

sub VerbNamingConvention {
	my ($name,$opt) = @_;
	return if $opt->{return};

	if ($name =~ /^Get[A-Z]/) {
		$opt->{return} = '$$';
	} elsif ($name =~ /^(Fetch|Search|Export)[A-Z]/) {
		$opt->{return} = '@%';
	} elsif ($name =~ /^(Check|Has)[A-Z]/) {
		$opt->{return} = '$';
	} elsif ($name =~ /^(Insert|Update|Set|Import|Delete)[A-Z]/) {
		$opt->{return} = '$';
	}
}

sub ReturnByStatement {
	my ($name,$opt) = @_;
	return if $opt->{return};

	if ($opt->{sql} =~ /^select\b/i) {
		$opt->{return} = '@%';
	} elsif ($opt->{sql} =~ /^(insert|update|delete)\b/i) {
		$opt->{return} = '$';
	}
}

sub SimpleTemplate {
	my ($name,$opt) = @_;
	return unless $name =~ /\$\{\w+\}/;

	my $vars = delete $opt->{vars} or croak "The method $name references template variables, but no variables are defined via the vars=> option!";
	ref($vars) ne 'HASH' and croak "The method ${name}'s vars=> option must be a hashref!";
	for (keys %$vars) {
		croak "The method ${name}'s template variables must match /^\\w+\$/ !" unless /^\w+$/;
	}
	for (values %$vars) {
		croak "The method ${name}'s template variables must contain a list of values!" unless ref($_) eq 'ARRAY';
	}

	{ # test whether the method name contains all known variables and no other ones
		my $used={};
		$used->{$1}++ while $name =~ /\$\{(\w+)\}/g;
		for (keys %$vars) {
			croak "Variable \${$_} defined for method $name, but is not present in the method name!" if ! exists $used->{$_};
		}
		$used->{$1}++ while $opt->{sql} =~ /\$\{(\w+)\}/g;
		for (@{ $opt->{args} }) {
			$used->{$1}++ while /\$\{(\w+)\}/g;
		}
		for (keys %$used) {
			croak "Undefined variable \${$_} in method $name" if ! exists $vars->{$_};
		}
	}

	if (keys(%$vars) == 1) {
		my ($varname) = keys %$vars;
		@_ = map {
			my ($name, $opt) = ($name, Clone::clone $opt);
			for my $s ($name, $opt->{sql}, @{ $opt->{args} }) {
				$s =~ s/\$\{\w+\}/$_/g
			}
			($name,$opt)
		} @{$vars->{$varname}};
	} else {

		@_=($name,$opt);
		for my $var (keys %$vars) {

			my $re = qr/\$\{$var\}/;
			my @result;
			while (my $name = shift(@_) and my $opt = shift(@_)) {
				push @result, map {
					my ($name, $opt) = ($name, Clone::clone $opt);
					for my $s ($name, $opt->{sql}, @{ $opt->{args} }) {
						$s =~ s/$re/$_/g;
					}
					($name, $opt)
				} @{ $vars->{$var} }
			}
			@_ = @result;
		}
	}
}

1;

__END__

=head1 NAME

DBIx::Declare - declare the database access object, specify the sql/stored proc names, arguments and return and have the code generated for you

=head1 VERSION

Version 0.01.006

=head1 SYNOPSIS

  use DBIx::Declare
	MyDB => {
		data_source  => "dbi:ODBC:Driver=SQL Server;Server=Some;Database=MyDB",
		type => 'MSSQL', # may be MSSQL, MySQL, SQLite, Pg/PostgreSQL
		  # - this is case insensitive and may be used to allow database specific options/implementation.
		  #   Normaly taken from the data_source, but needed for ODBC or DBD::Proxy
		user => '...',
		pass => '....',
		attr => { 'RaiseError' => 0, 'AutoCommit' => 1, LongReadLen => 65536 },
		generate => 'now', # or 'when connected' or 'as needed'. The last one is the default.
		  # The 'now' may not work  if the data_source is incomplete and you ask the module to infer the arguments for stored procedures. In such cases some methods trigger a warning.
		case => 'sensitive',
		infer => ['VerbNamingConvention'],
		methods => {
			set_people_name_by_id => {
				sql => "UPDATE people SET name = ? WHERE id = ?",
				args => [ qw(name id) ],
				return => '$',
			},
			get_site_entry_by_id => {
				sql => "SELECT * FROM sites WHERE SiteId = ?",
				args => [ qw(id) ],
				return => '%', # only one row
			},
			get_people_entry_by_last_name => {
				sql => "SELECT * FROM people WHERE last_name = ?",
				args => [ qw(name) ],
				return => '@%', # multiple rows
			},
			# Although not really recommended, you can also change the database schema
			drop_table => {
				sql => "DROP TABLE ?",
				args => [ qw(table) ],
				return => '$',
				noprepare => 1, # For non-prepareable queries
				noquote => 1, 	# For non-quoteable arguments (like table names)
			},

			GetATSName => {
				return => '$',
			},
			GetSiteName => {
				defaults => {
					SiteID => 3,
				},
	#			return => '$$',  # inferred
			},
		},
	};

  my $db = MyDB->new();

  my $SiteName = $db->GetSiteName( 457 );
  my $OtherSiteName = $db->GetSiteName( SiteID => 123 );

  my %SiteInfoHash = $db->get_site_entry_by_id( 123 );
  my $SiteInfoHref = $db->get_site_entry_by_id( 123 );

=head1 DESCRIPTION

Specify the connection and type, a few options and the SQL (or - for MSSQL only so far - the stored procedures)
and have the database access object generated for you.

=head2 What does that mean?

DBIx::Declare uses AUTOLOAD to create methods and statement handles based on the
data in the hashref supplied in the argument 'methods'.
Statement handles are persistent in the lifetime of the instance or till the instance gets
disconnected from the database. The generated code of the methods is persistent
until your program exits.
It is an easy way to set up accessor methods for a fixed (in the sense of
database and table layout) schema.

When a method defined in the 'methods' hashref is invoked, it is verified that the arguments
in 'args' are provided. The arguments are then applied to the persistent
statement handle that is created from the value 'sql'
statement.

=head1 CLASS DECLARATION

The C<use DBIx::Declare> statement is used to declare and build the class of your
database access objects. The first parameter is the name of the class to build (or
an undefined variable that will hold a generated unique class name), the second
parameter is a hash reference containing the options and methods.

The options are

=over 4

=item C<data_source> STRING

The data source to be fed to DBI->connect. May be overwritten or appended to by the constructor
of the generated class. Optional.

=item C<type> STRING

The type of database to connect to. This may allow some database specific functionality.
If you do not specify the type, the module takes the DBD name from the data_source.
The type will be lowercased and the generated class will inherit either from
DBIx::Declare::DB::<the type> (if it exists!) or from DBIx::Declare::DB::unknown.

The '++' (get the generated ID of the last inserted row) is available only for
MySQL, Pg (Postgres), MSSQL and SQLite. The stored procedure call automatic
SQL generation is only available for MSSQL. (To add support for that create
package DBIx::Declare::DB::<your type>, set its @ISA to qw(DBIx::Declare::DB::unknown)
and define a method C<$db->_InferSQL( $method_name, $method_options).
This souboutine will be called whenever you attempt to use a declared method
with no sql=> specified. It should return the generated SQL and set the $method_options->{args}
and $method_options->{out_args} accordingly.

Optional.

=item C<user> STRING

The default database connection username. Optional.

=item C<pass> STRING

The default database connection password. Optional.

=item C<attr> HASH

The default database connection attributes. Leave blank for DBI defaults.

The attributes specified later when calling the constructor are MERGED with this hash!
Only the individual attributes are overwritten, not the whole hash.

Optional.

=item C<methods> HASH

The methods hash reference. Also see the KEYS IN METHODS DEFINITION description.

Required.

=item C<case> STRING

Specifies whether the names of the parameters passed to the methods are case sensitive or not
and whether the arguments discovered for the stored procedures are lowercased or left intact.

  sensitive - arguments are case sensitive, no case mangling for discovered arguments
  insensitive - arguments are case insensitive (very slightly slower)
  lowercase - the discovered arguments are lowercased, apart from that all handling is
    case sensitive

Optional. Default is "sensitive".

=item C<warn_useless> BOOLEAN

Specifies whether to warn if you include a named parameter that's not expected by a method in its call.
By default true.

=item C<disconnected> BOOLEAN

If set to true, the object doesn't connect to the database until you either call $obj->_Connect
or a generated method.

=item C<infer> CODEREF|STRING|ARRAY of (CODEREF|STRING)

If you use a consistent naming convention, your return type can be infered from the SQL or
want to use some kind of templating you can specify one or several subroutines to be called
for each method specification and set some options automatically or convert one generic
method specification to several.

Please see the C<Automation> section.

=item C<format_errors> CODEREF

	format_errors => sub { my ($self, $msg, $errstr) = @_; ...
	format_errors => sub { my ($self, $msg, $errstr, $sql, \%params, $method) = @_; ...

This subroutine gets called for each error and may be used to reformat it. It gets two to sixarguments. The first
is the database access object, the second the error message from DBIx::Declare
(things like "Method Foo failed to execute", "bind_param failed for parameter Bar for method Foo" etc.),
the third (if applicable) is the errstr from DBI.

If the C<error_details> option is true you also receive the SQL statement being evaluated, the hash of parameters
(from $sth->{ParamValues}, please see this property in C<DBI>!) and the full name of the method including
the classname. You probably want only the part after the last doublecolon.

It can use the C<< $self->_format_error_details($sql, \%params, $method) >> method to get a formatted version of
the statement plus parameters. This way this works depends on the database type!

The subroutine should return a formatted message.

=item C<error_details> BOOLEAN

If set to a true value, DBIx::Declare includes the executed SQL in the error message.

=item C<on_errors>

This may be either a reference to a procedure to be called to report the errors or one of the strings
'carp', 'croak' or 'die' or an error message template.

If you specify a code reference then DBIx::Declare calls your subroutine passing the error message
(after formatting and with the error details added) as the first argument and returns whatever that subroutine returns.

If you specify 'carp', 'croak' or 'die' them DBIx::Declare calls the respective function with the error message.

If you specify some other string then DBIx::Declare replaces the token _ERROR_ in the string by the actuall
error message and then either die()s (if the error template ends with a newline) or carp()s.

=item C<code_cache>

  code_cache => '/path/to/cache/directory'
  code_cache => \%tied_hash
  code_cache => \&cacher

The code generated for the methods (including the SQL and argument lists infered for the stored procedures)
may be stored in a code cache. The cache may either be a directory on the disk, a (tied) hash or a subroutine,
functioning both as a getter and setter.

In the first case each generated method will be written into a separate file in the specified directory.
The file will be named using the method name and the MD5 hash of the SQL (or nothing if not specified),
the argument list, the return type and the values of noquote and noprepare options in this format:
<MethodName>-<MD5_hash>-generated.pl

You may specify the maximal age of the cached code using the code_cache_max_age option. The value of this option
may be either in minutes ( /($number)\s*m/), in hours ( /($number)\s*h/ ) or days ( /($number)\s*d?/ ). You can use
floats.

In the second case the key will be C<$MethodName . $; . $MD5_hash> and the value will be the code (as string of course).

In the last case the subroutine will either be called with a single parameter, the C<$MethodName . $; . $MD5_hash>
to get the code if available or with two parameters to add the generated code to the cache.


It only makes sense to cache the code using this mechanism BETWEEN invocations of the script(s), not within one run.
Within one run the code is eval""ed just once and reused ever since. The only exception may be multithreaded scripts
where it may make sense to have a cache shared between the threads because the code eval""ed in one thread is not
made available to others. The caching makes even more sense for stored procedures that you call using the _InferSQL functionality
(without explicitely specifying the number, names and types of parameters, because the _InferSQL has to query the database
catalog to find out the info about the stored procedure.

If you make changes to the sql, args or return options, a new version will be generated even if there is an old version in the cache,
but if you change the signature of a stored procedure you have to clear the cache to force the scripts started after the change
to requery the database and rediscover the arguments!

=back

=head1 STATIC METHODS

=over 4

=item C<new>

  my $db = MyDB->new();
  my $db = MyDB->new( $data_source);
  my $db = MyDB->new( $data_source, {attributes});
  my $db = MyDB->new( $data_source, $username, $password);
  my $db = MyDB->new( $data_source, $username, $password, {attributes});

If you call the constructor with no arguments, then the data_source, user, password and
attributes are used. If the data_source doesn't start with 'dbi:' it's considerent a "partial data source"
and is appended to the "dbi:Something:" part of the data_source specified when declaring
the class.

The attributes are merged with the ones specified in the class declaration and overwrite
the individual DBI options, not the whole hash.

=item C<_Connect>

Connects the object to the database if not connected already. Returns whether
the object is connected.

=item C<_Disconnect>

Disconnects the object from the database.

=item C<_Connected>

Returns whether the object is connected. (Or rather if the object is connected returns the DBI object.)

=item C<_Is_error>

Returns whether the last method call resulted in an error.

=item C<_Session>

  my $session_hash = $db->_Session();
  my $value = $db->_Session( "argument name");
  $db->_Session( "argument name", "value");

Returns or sets a session variable. The session is something like global defaults. Whenever
you omit an argument when calling a method it's first looked up in the session and then
in the method-specific default values. Only if found in neither place, the method complains
about missing required argument. If you name your arguments consistently you can use this
to pass things like UserId or UserType behind the scenes.

=item _Commit

Commit (make permanent) the most recent series of database changes if the database supports transactions and AutoCommit is off.

If AutoCommit is on, then calling commit will issue a "commit ineffective with AutoCommit" warning. Please see the transactions section of the C<DBI> docs.

=item _Rollback

Rollback (undo) the most recent series of uncommitted database changes if the database supports transactions and AutoCommit is off.

If AutoCommit is on, then calling rollback will issue a "rollback ineffective with AutoCommit" warning.

=item _BeginTrans

Enable transactions (by turning AutoCommit off) until the next call to commit or rollback. After the next commit or rollback, AutoCommit will automatically be turned on again.

If AutoCommit is already off when begin_work is called then it does nothing except return an error. If the driver does not support transactions then when begin_work attempts to set AutoCommit off the driver will trigger a fatal error.

=back

=head1 GENERATED METHODS

The generated methods may be called either using named parameters or positional ones.
You use the named parameters like this

 my $result = $db->SomeMethod( -ArgName => $value, -OtherArg => $something);

You may skip the optional parameters (those that have a default value specified in
the method specification or that have a value stored in the session. You can also skip
the OUTPUT parameters that you are not interested in.

If a parameter is declared as output, you have to use a variable (or rather ... a lvalue), but you
should not pass a reference:

 my $result = $db->GetSiteName( -SiteId => 13, -SiteName => $name);

if you use a constant as the value of an OUTPUT parameter you receive an error.

If you use the positional parameters (in the order specified in the args=> option in the method specification)
you can either do it like this

 my $result = $db->SomeMethod( 12, 45, $whatever);

or like this

 my $result = $db->SomeMethod( _ => $variable, 45, $whatever);

You should use the second format whenever it is not guaranteed that the first argument will not look
like a named parameter (match /^-\w+$/) or be an underscore.

=head1 KEYS IN METHODS DEFINITION

Thanks to the various C<Automation> features, there are no really mandatory options.
As long as at least the 'sql' and 'return' can be infered, you should be fine. Without the 'args'
option you will not be able to use the named parameters style, specify defaults or use the session.

=head2 args

The value of 'args' is an array of key names. The keys must be in the same order as the matching SQL placeholders ("?").
All elements of this array MUST match /^\w+$/! You can specify the same name several times if you want several
placeholders to always be assigned the same value.

=head2 sql

The 'sql' key holds the query or statement to execute. For MSSQL, if you do not specify this option
DBIx::Declare assumes you want to call the stored procedure named as this method or the one
specified by the 'call' option. In this case DBIx::Declare asks the database for the list of parameters
of that procedure and sets the 'sql', 'args' and 'out_args' accordingly.

=head2 call

This option is used only if you do not specify the 'sql' and specifies the stored procedure to call.

=head2 return

The value of 'return' (return value) can be:

=over 4

=item *
	'@' - returns a list (or array ref in scalar context) containing all the rows concatenated together.
	This is especially handy for queries returning just one column. (SELECT)

=item *
	'_@' - returns a list (or array ref in scalar context) containing the data of the first row.
	Other rows are not returned! The statement handle is finish()ed. (SELECT)

=item *
	'%' - returns a list containing the name and value pairs (or a hash ref in scalar context) containing the data of the first row.
	Other rows are not returned! The statement handle is finish()ed. (SELECT)

=item *
	'@%' - returns an array/arrayref of hashrefs (SELECT)

=item *
	'@%lc' - returns an array/arrayref of hashrefs. The column names are lowercased (SELECT)

=item *
	'@%uc' - returns an array/arrayref of hashrefs. The column names are uppercased (SELECT)

=item *
	'@@' - returns an array of arrayrefs (SELECT)

=item *
	'$' - returns the ->execute() return value (NON-SELECT)

=item *
	'$R' - returns the stored procedure return value (MSSQL only - EXEC @RETURN_VALUE = dbo.ProcName ...)

=item *
	'$$' - returns the OUTPUT parameters (will return a list or arrayref containing the values of the arguments specified
		in the 'out_args' option. This option is built automaticaly for stored procedures for MSSQL)

=item *
	'++' - returns the new auto_increment value of an INSERT
	  (MySQL/Pg/MSSQL/SQLite specific. Please send me an email if you know how to implement this for other databases).

=item *
	'<>' - returns the DBI statement handle. You may call whatever fetchxxx_xxx()
	  methods you need then (SELECT)
	  This doesn't work together with output parameters in MSSQL!

=item *
	'&@' - call the subroutine specified in the "sub" option for each row fetched,
	  pass each row as a list of items, return a list of return values of the subroutine applications. (SELECT)

=item *
	'&\@' - call the subroutine specified in the "sub" option for each row fetched,
	  pass each row as an array reference, return a list of return values of the subroutine applications. (SELECT)
	  Please note that the refererence will point to the same array for all subroutine calls!
	  See fetchrow_arrayref() in the DBI docs.

=item *
	'&%' - call the subroutine specified in the "sub" option for each
	  row fetched, pass row as a list of column names and items (to assign to hash).
	  Return a list of return values of the subroutine applications. (SELECT)

=item *
	'&\%' - call the subroutine specified in the "sub" option for each
	  row fetched, pass row as a hash ref.
	  Return a list of return values of the subroutine applications. (SELECT)

=item *
	'.@' / '->@' - call the constructor of the class specified in the "class" option
	  for each row fetched, pass the row as a list of items, return a list of the created objects (SELECT)

=item *
	'.\@' / '->\@' - call the constructor of the class specified in the "class" option
	  for each row fetched, pass the row as an arrayref, return a list of the created objects (SELECT)

=item *
	'.%' / '->%' - call the constructor of the class specified in the "class" option
	  for each row fetched, pass the row as a list of column names and items, return a list of the created objects (SELECT)

=item *
	'.\%' / '->\%' - call the constructor of the class specified in the "class" option
	  for each row fetched, pass the row as a hash ref, return a list of the created objects (SELECT)

=item *
	\&subroutine - equivalent to '&@' with this subroutine in the 'sub' option (SELECT)

=item *
	'Class::Name' - equivalent to '.@' with this class in the 'class' option (SELECT)

=item *
	'CDF' - this will produce XML in the TIBCO General Interface CDF format.
	By default the jsxid attribute is the ordinary number of the record. You can use the,
	'jsxid' option to specify the column to use as the id or a subroutine that computes the id
	and 'jsxmap' to specify the mapping from the columns to record attributes. The jsxmap may be either
	an array ref or a hash ref. (NOT IMPLEMENTED YET!)

=back

=head2 return_value

If you specify this option, the return status of the stored procedure will be returned as if it was an output
parameter named according to the value of this option.

=head2 noprepare

The 'noprepare' key indicates that the method should not use a prepared statement handle for execution.
This is really just slower. It should be used when executing queries that cannot be prepared. (Like 'DROP TABLE ?').
It only works with non-SELECT statements. So setting 'return' to anything else than WANT_RETURN_VALUE will cause an error.
See the 'bind_param' section of the 'Statement Handle Methods' in the DBI documentation for more information.

=head2 noquote

The 'noquote' key indicates that the arguments listed should not be quoted.
This is for dealing with table names (Like 'DROP TABLE ?'). It's really a hack.
The 'noquote' key has no effect unless used in collaboration with the 'noprepare' key on a method.

You should think at least twice before using this option! It's your duty to ensure the values passed to the methods
created with this option are safe!!!

=head2 out_args

This is a hash in format

  {
    arg_name => [ $max_len, $dbi_type, $type_string],
	...
  }

the first two are then passed to DBI's bind_param_inout as the third argument and as the TYPE attribute,
the third is used in error details. You have to use a variable as the argument for the output parameters, but
unlike some other modules you DO NOT have to pass a reference.

This is usually only used with stored procedures and it populated automatically.

=head2 on_errors

This parameter overrides the global C<on_errors> parameter with one exception. If you specify an error template
and the global C<on_errors> is a subroutine reference then DBIx::Declare first fills in the template and then calls that subroutine.

=head2 Multiple methods sharing part of the options

You may define several methods at once if you separate their names by commas and you may define the method
in several steps like this:

  'FetchSomethingA,FetchSomethingH' => {
	sql => 'SELECT ...',
	args => [...],
  },
  FetchSomethingA => {return => '@@'},
  FetchSomethingH => {return => '@%'},

Please keep in mind that if you use a hashref for the methods then the order of the definitions is NOT defined!
This means that if you specify some option twice for a method, then it's not clear which of the values will be in effect.
Use the arrayref to ensure the order and the last one wins.

=head1 Automation

There are two stages in which some of the method definition options may be set automatically. First there is the C<infer>
option that's evaluared during the class construction and then the database type specific _InferSQL() that gets called just before
the method is first prepare()d in case you do not specify the C<sel> option.

=head2 _InferSQL

The _InferSQL() is so far defined only for MSSQL and it assumes that if you do not specify the SQL, you want to call
a stored procedure with the same name as your method. The _InferSQL() queries the database for the parameters of
the stored procedure and sets the C<sql>, the C<args> and the C<out_args> accordingly.

For all other types of databases the _InferSQL() only reports that you did not specify the C<sql>. You can define your own
database type by creating a package DBIx::Declare::DB::<yourname>, setting it's @ISA to C<qw(DBIx::Declare::DB::unknown)>
and implementing the _InferSQL(). And whatever other methods you want to make available to all declared classes with
the C<type> attribute set to this type.

I would love to include versions of _InferSQL for all widely used databases supporting stored procedures, please
contact me if you can help me with this!

=head2 infer

The infer=> option may be either a string containing the name of one of the builtin infer subroutines (or another
subroutine in the DBIx::Declare::infer package) or a subroutine reference or an array containing several strings or
subroutine references. If you specify an array, then all the infer rules are evaluated in the specified order.

The builtin rules are:

=over 4

=item VerbNamingConvention

This rule infers the return type for the methods for which you did not specify it. It expects that the names of the methods
start with a verb and that you use the LeadingCaps convention. It assumes that methods starting with 'Get' have some output
parameters and return no resultset (return => '$$'), those starting with 'Fetch', 'Search' or 'Export' return a recordset (return => '@%'),
those starting with 'Check' or 'Hash' are stored procedures that return the result as the procedure's return value
(MSSQL only) (return => '$R'), those starting with 'Insert' insert a row and you want the ID of the inserted row (return => '++')
and those starting with 'Update', 'Set', 'Import' or 'Delete' do not return any data and you want the execute()'s return value
(return => '$').

You will probably want to use this just as an example. It's implemented like this:

	sub VerbNamingConvention {
		my ($name,$opt) = @_;
		return if $opt->{return};

		if ($name =~ /^Get[A-Z]/) {
			$opt->{return} = '$$';
		} elsif ($name =~ /^(Fetch|Search|Export)[A-Z]/) {
			$opt->{return} = '@%';
		} elsif ($name =~ /^(Check|Has)[A-Z]/) {
			$opt->{return} = '$R';
		} elsif ($name =~ /^(Insert)[A-Z]/) {
			$opt->{return} = '++';
		} elsif ($name =~ /^(Update|Set|Import|Delete)[A-Z]/) {
			$opt->{return} = '$';
		}
	}

=item ReturnByStatement

This rule infers the return type based on the SQL. If the SQL starts by 'SELECT' the return=> will be set to '@%', if it starts
with 'INSERT', 'UPDATE' or 'DELETE' it's assumed you want the execute()'s return value ('$').

=item SimpleTemplate

This rule allows you to define several methods as one using very simple templating for example if you want to declare
a SelectById method for several tables you can do it like this:

  'Fetch${Table}ById} => {
    sql => 'SELECT * FROM ${Table} WHERE Id = ?',
	args => [ 'id' ],
	return => '%', # we assume just one row
	vars => { Table => [qw(Foo Bar Baz)] },
  }

This will create three methods, FetchFooById, FetchBarById and FetchBazById.

You can specify as many variables as you want, but you have to include them all in the name of the method!
You can reference the template variables in the method name, the sql and the args:

  'Fetch${Table}ById} => {
    sql => 'SELECT * FROM ${Table} WHERE ${Table}Id = ?',
	args => [ '${Table}Id' ],
	return => '%', # we assume just one row
	vars => { Table => [qw(Foo Bar Baz)] },
  }

Please notice the single quotes and the curlies around the template variables! Both are required.

=item Custom infer rules

You can specify a subroutine reference in the C<use DBIx::Declare ...> statement or define a subroutine in the DBIx::Declare::infer
package. In both cases the subroutine gets called for each method specification and will receive the method name as the first and
the options as the second parameter. It can modify the options in the second parameter, it even modify the values in @_
(eg. it can rename the method by modifying $_[0]) and it can even push more method specifications onto @_.

This means that if (like the SimpleTemplate above) you want to duplicate the method specification, you can add another
methodname and options hash to @_ and it will be added to the list of methods to be defined.

=back

=head1 ERROR HANDLING

By default all methods carp() in case there is an error. You can specify that they should die() or croak() instead or that a function you specify should be called.
You may also provide a formatting subroutine that will be called on the error messages before the carp(), croak(), die() or the callback function.
Apart from this you may ask DBIx::Declare to include the call details in the error message. If you do so then the error message will
(if possible) contain a snippet of SQL that is being executed, including the values. This is usualy NOT exactly the thing being executed
against the database though, DBIx::Declare prepares the statements and uses placeholders.

You may then change the way errors are reported when defining the methods or when calling them.

Whenever an error occures (and is not forced to be ignored by the format_errors subroutine) the module sets an internal flag in the object
that may be queried by method is_error() and stores the error message in C< $db-E<gt>{errormessage}; >.

=head1 COPYRIGHT

Copyright (c) 2009 Jenda Krynicky <Jenda@Krynicky.cz>.  All rights
reserved.

This program is free software; you can redistribute it and/or
modify it under the same terms as Perl itself.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
Artistic License for more details.

=head1 AUTHOR

Jenda Krynicky, C<< <Jenda at Krynicky.cz> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-dbix-declare at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=DBIx-Declare>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

=head1 TODO

=over

=item More DBD specific functions (Oracle/Pg).

=item Better documentation.

=item More "failure" tests.

=item Testing expired statement handles.

=back

=head1 ACKNOWLEDGEMENTS

Thanks to Casper Warming <cwg@usr.bin.dk> for his DBIx::LazyMethod.

=head1 SEE ALSO

DBIx::LazyMethod

DBI(1).

=head1 COPYRIGHT & LICENSE

Copyright 2009 Jenda Krynicky, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut
