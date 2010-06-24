#!/usr/bin/perl

use strict;
use warnings;
use Test::More tests => 5;
use Encode;
use lib::abs qw(../../lib);

use_ok 'AnyEvent::Queue::Encoder::Gzip';

ok my $e = AnyEvent::Queue::Encoder::Gzip->new(), 'object';

my $data = {
	utf => Encode::decode( utf8 => "тестовая строка" ),
	raw => "\001\002\003",
	ary => [1,2,3],
	hash => { 1 => 2 => 3 => 4 },
};

my $cloned = $e->decode($e->encode($data));
is_deeply $data,$cloned, 'clone';
ok utf8::is_utf8($cloned->{utf}), 'utf stay utf';
ok !utf8::is_utf8($cloned->{raw}), 'raw stay raw';
