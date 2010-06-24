#!/usr/bin/perl

use strict;
use warnings;
use Test::More;
use Module::Find;
use lib::abs qw(../lib);

setmoduledirs( $INC[0] );
my @modules = grep { !/^$/ } findallmod 'AnyEvent';
plan tests => scalar( @modules );
use_ok ($_) foreach @modules;

diag( "Testing AnyEvent::Queue $AnyEvent::Queue::VERSION, Perl $], $^X" );
