#!/usr/bin/perl

use strict;
use warnings;
use Test::More tests => 14;
use Encode;
use Data::Dumper;
use lib::abs qw(../../lib);
$Data::Dumper::Useqq=1;

use_ok 'AnyEvent::Queue::Encoder::JSON';
use_ok 'AnyEvent::Queue::Encoder::YAML';
use_ok 'AnyEvent::Queue::Encoder::Gzip';

use_ok 'AnyEvent::Queue::Encoder::Any';

ok my $e = AnyEvent::Queue::Encoder::Any->new(), 'object';

my $data = {
	utf => Encode::decode( utf8 => "тестовая строка" ),
	raw => "\001\002\003",
	ary => [1,2,3],
	hash => { 1 => 2 => 3 => 4 },
};

my %enc;
$enc{JSON} = AnyEvent::Queue::Encoder::JSON->new->encode(Storable::dclone($data));
$enc{YAML} = AnyEvent::Queue::Encoder::YAML->new->encode(Storable::dclone($data));
$enc{Gzip} = AnyEvent::Queue::Encoder::Gzip->new->encode(Storable::dclone($data));

for (keys %enc) {
	my $cloned = $e->decode($enc{$_});
	is_deeply $data,$cloned, "$_ => Any: clone";
	#diag $enc{$_}."\n".Dumper($cloned);
	
	ok utf8::is_utf8($cloned->{utf}), "$_ => Any: utf stay utf";
	if ($_ ne 'YAML') {
		ok !utf8::is_utf8($cloned->{raw}), "$_ => Any: raw stay raw";
	} else {
		TODO: {
			local $TODO = "YAML and byte strings???";
			ok !utf8::is_utf8($cloned->{raw}), "$_ => Any: raw stay raw";
		}
	}
}

