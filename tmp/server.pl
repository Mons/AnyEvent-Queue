#!/usr/bin/env perl -w

package main;
BEGIN { $ENV{CB_DEBUG} = 1; }
use strict;
use lib::abs '../lib', '../../Queue-PQ/lib', '../../AnyEvent-Connection/lib';
use AnyEvent::Impl::Perl;
#use AnyEvent::Impl::EV;
use AnyEvent::Queue::Server::PMQ;
use AnyEvent::Queue::Client::PMQ;
use Time::HiRes qw(time);
use R::Dump;
use Devel::FindRef;sub findref;*findref = \&Devel::FindRef::track;
use Scalar::Util qw(weaken);
use Devel::Refcount qw( refcount );
use AnyEvent::Util;
use Sub::Identify ':all';
use AnyEvent::cb;
use Sub::Name;

#my $inverval = 0.001;
my $inverval = 0.01;
my $rate = 10;
#my $rate = 3;
my $reload = 1;
#my @queues = qw(test1 test2 test3);
my @queues = qw(test1);
my @range = 1..300;
#my @range = 1..1;
#our %history;
our %taken;
our %seen;
our $port = 12345;
our ($server,$loader,$client);

sub mem () {
	my $ps = (split /\n/,`ps auxwp $$`)[1];
	my ($mem) = $ps =~ /\w+\s+\d+\s+\S+\s+\S+\s+(\d+)\s+(\d+)/;
	return $mem;
}
our $cmem = 0;
sub measure ($) {
	my $op = shift;
	my $mem = mem;
	my $delta = $mem - $cmem;
	if ($delta != 0) {
		$cmem = $mem;
		warn sprintf "%s: %+d\n",$op,$delta;
		#warn Dump $cl;
	}
}

unless ($server = fork) {
	#exit 0;
	local $SIG{__WARN__} = sub { CORE::warn("SERVER @_"); };
	my $cv = AnyEvent->condvar;
	$SIG{INT} = sub { $cv->send };
	$0 = "test-pmq - server";
	my $server = AnyEvent::Queue::Server::PMQ->new(
			port         => $port,
			devel        => 0,
		#debug_proto  => 1,
		#debug_engine => 1,
	);
	$server->start;
	$cv->recv;
	exit 0;
}

unless ($loader = fork) {
	#exit 0;
	local $SIG{__WARN__} = sub { CORE::warn("LOADER @_"); };
	my $cv = AnyEvent->condvar;
	$SIG{INT} = sub { $cv->send };
	$cv->begin(sub { $cv->send });
	$0 = "test-pmq - loader";
	my $c = AnyEvent::Queue::Client::PMQ->new(
		servers => ['localhost:'.$port],
		reconnect => 0,
		debug => 0,
	);
	$cv->begin;
	$c->connect(cb {
		warn "Loader client connected: @_";
		for my $dst (@queues) {
			for my $id (@range) {
				$cv->begin;
				$c->put(
					dst => $dst,
					id  => $id,
					data => { x => 'x'x10 },
					cb => sub {
						#push @{$history{$id}},"insert: $! ".( @_ > 1 ? "$_[1]" :'').( $taken{$id} ? ' +taken' : '' );
						shift or $!{EEXIST} or warn "create $dst.$id failed: @_";
						$cv->end;
					},
				);
			}
		}
		$cv->end;
	});
	$cv->end;
	$cv->recv;
	warn "Loader end";
	undef $c;
	exit 0;
}

$SIG{INT} = sub { kill INT => $server,$loader };

waitpid($server,0) if $server;
waitpid($loader,0) if $loader;

END {
	kill TERM => -$$ if $server and $loader;
}
