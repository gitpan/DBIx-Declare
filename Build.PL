use strict;
use warnings;
use Module::Build;

# require perl 5.8.1

my $builder = Module::Build->new(
    module_name         => 'DBIx::Declare',
    license             => 'perl',
    dist_author         => 'Jenda Krynicky <Jenda@Krynicky.cz>',
    dist_version_from   => 'lib/DBIx/Declare.pm',
    build_requires => {
        'Test::More' => 0,
        'DBI' =>  1.28,
        'Scalar::Util' => 0,
        'Clone' => 0,
    },
    add_to_cleanup      => [ 'DBIx-Declare-*' ],
    create_makefile_pl => 'traditional',
    create_license  => 1,
);

$builder->create_build_script();
