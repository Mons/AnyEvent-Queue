#!/usr/bin/perl

use strict;
BEGIN { $ENV{CB_DEBUG} = 1; }
use lib::abs '../lib', '../../Queue-PQ/lib', '../../AnyEvent-Connection/lib';
use AnyEvent::Impl::Perl;
#use AnyEvent::Impl::EV;
use AnyEvent;
use AnyEvent::cb;
use AnyEvent::Socket;
use AnyEvent::Handle;
use R::Dump;
use Time::HiRes qw(time);
use BSD::Process;
use Devel::Leak;
use Errno;

our $cmem = 0;
our $memc = 0;
sub measure ($;$) {
	my $op = shift;
	use integer;
	my $mem = BSD::Process->new->{size} / 1024;
	my $delta = $mem - $cmem;
	if ($delta != 0) {
		$cmem = $mem;
		warn sprintf "%s: %+d\n",$op,$delta;
		$_[0]() if @_;
	}
}
my $cv;
$cv = AnyEvent->condvar;
if (my $pid = fork) {

my $tcg;$tcg = tcp_connect localhost => 11311, sub {
	!$_[0] and $! == Errno::ECONNREFUSED and return pop->();
	pop;
	my $fh = shift or warn("$!"),exit;
	my $h = AnyEvent::Handle->new(
		fh => $fh,
	);
	#$self->{con}->command("put $pri $delay $ttr $length$NL$data", cb => sub {
	my $count = 0;
	my $put; $put = sub {
		$h->push_write("put 16 0 300 4\015\012test\015\012");
		$h->push_read(regex => qr<\015\012> => sub {
			shift;
			#warn "@_";
			if( $count++ > 100 ) {
				$h->destroy;
				undef $h;
				undef $tcg;
				$cv->send;
			} else {
				$put->();
			}
		});
	};
	$h->push_write("use test1\015\012");
	$h->push_read(regex => qr<\015\012> => sub {
		shift;
		warn "@_";
		$put->() for 1..2;
	});
	warn "connected @_";
}, sub { 3 };

$cv->recv;

waitpid $pid,0;

}
else {
	exec('beanstalkd','-p',11311)
}