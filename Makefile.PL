use inc::Module::Install;

name     'AnyEvent-Queue';
all_from 'lib/AnyEvent/Queue.pm';
author   'Mons Anderson <mons@cpan.org>';
license  'perl';

build_requires 'Test::More';
build_requires 'lib::abs', '0.91';
tests_recursive;

requires 'AnyEvent';
requires 'Object::Event';
requires 'constant::def';
requires 'JSON::XS';
requires 'YAML::Syck';
requires 'Queue::PQ';
requires 'Compress::Zlib';
requires 'Dash::Leak'; # TODO: remove
requires 'R::Dump'; # TODO: remove


auto_install;

WriteAll;

