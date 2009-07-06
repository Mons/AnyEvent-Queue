use inc::Module::Install;

name     'AnyEvent-Queue';
all_from 'lib/AnyEvent/Queue.pm';
author   'Mons Anderson <mons@cpan.org>';
license  'perl';

build_requires 'Test::More';
build_requires 'ex::lib';

requires 'AnyEvent';
requires 'Object::Event';
requires 'constant::def';
requires 'JSON::XS';
requires 'YAML::Syck';

auto_install;

WriteAll;

