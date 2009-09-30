#!/usr/bin/env perl -w

package guardx;

use AnyEvent::Util;

sub new {
	my $guard = AnyEvent::Util::guard(sub {
		warn "Guarded";
	});
	return bless \$guard;
}

sub test {
	my $self = shift;
	my $cb = shift;
	my $t; $t = AnyEvent->timer(after => 1, cb => sub {
		undef $t;
		$cb->();
		warn "Test $self";
	} );
	return;
}

sub DESTROY {
	
}

package object;

package main;

use AnyEvent::Impl::Perl;
use AnyEvent;

{
	my $g = guardx->new;
	$g->test(sub {
		$g;
	});
	print "end\n";
}
print "END\n";
AnyEvent->condvar->recv;