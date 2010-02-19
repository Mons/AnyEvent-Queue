#!/usr/bin/perl

use lib::abs '../lib', '../../AnyEvent-Connection/lib';
{
	package t::srv;
	use base 'AnyEvent::Queue::Server';
}
package main;
use strict;
use AnyEvent;
use AnyEvent::Socket;

use Test::TCP;
use Test::More tests => 10;

test_tcp(
	server => sub {
		my $port = shift;
		my $g = AnyEvent::Util::guard { diag "server gone" };
		diag "$$: sr port: $port";
		my $server = t::srv->new(
			port => $port,
			engine => 1,
		);
		my $cv = AE::cv;
		my %init;
		my $recv = 0;
		my $sig = AE::signal TERM => sub { $cv->send; };
		$server->reg_cb(
			connect => sub {
				shift;
				my $c = shift;
				$init{int $c}++;
				$recv++;
				ok 1, 'server: client connected fn='.fileno($c->{fh});
			},
			command => sub {},
			disconnect => sub {
				my ($s,$c) = @_;
				delete $init{int $c};
				ok 1, 'server: client disconnected fn='.fileno($c->{fh});
			},
		);
		$server->start;
		$cv->recv;
		ok !%init, 'no leaked connections';
		is $recv, 3, 'received all connections';
	},
	client  => sub {
		my $port = shift;
		my $g = AnyEvent::Util::guard { diag "client gone" };
		diag "$$: cl port: $port";
		my $cv = AE::cv;
		my $count = 2;
		my $con;$con = sub {
			$cv->begin;
			tcp_connect '127.1',$port, sub {
				my $fh = shift;
				ok($fh, 'client: connected '.$count.' / fn='.fileno($fh));
				close $fh;
				undef $fh;
				if (--$count > 0) {
					my $t;$t = AE::timer 0.05,0,sub {
						undef $t;
						$con->();
						$cv->end;
					};
				} else {
					$cv->end;
				}
			};
		};
		$con->();
		$cv->recv;
	}
);

__END__
$| = 1;
my $x;
do {
	open my $f,'<',undef or die "$!";
	$x = fileno $f;
	#print $x,"\n";
	if ($x > 0) {
		print "." if !( $x % 20 );
		print $x if !( $x % 100 );
	}
	else {
		die "Shit happens: $x";
	}
	my $s;$s = sub{$f;$s;};
} while ($x < 32768);
print "\nDONE\n";
exit;