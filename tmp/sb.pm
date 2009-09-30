package sb;

use strict;
use Devel::Refcount qw(refcount);
use Devel::FindRef;sub findref;*findref = \&Devel::FindRef::track;
use Scalar::Util qw(weaken);
use Sub::Name;
use Sub::Identify 'sub_fullname';
use AnyEvent;
use AnyEvent::Util;

BEGIN { $INC{__PACKAGE__.'.pm'} = __FILE__; }

our %DEF;
sub import {
	no strict 'refs';
	for (qw(sb cb after periodic)) {
		*{ caller().'::'.$_ } = \&$_;
	}
}

sub sb (&;$) {
	#warn refcount($_[0]);
	#$DEF{int $_[0]} = [ join(' ',(caller)[1,2]), $ref ];
	#$DEF{int $_[0]} = [ join(' ',(caller)[1,2]), weaken($ref) ];
	#$DEF{int $_[0]} = [ join(' ',(caller)[1,2]), weaken($_[0]) ];
	$DEF{int $_[0]} = [ join(' ',(caller)[1,2]), $_[0] ];weaken($DEF{int $_[0]}[1]);
	#warn refcount($_[0]);
	#$DEF{int $_[0]} = [ join(' ',(caller)[1,2]) ];
	subname caller.'::'.$_[1],$_[0] if @_ > 1;
	return bless shift,'__callback__';
}

sub cb (&;@) {
	$DEF{int $_[0]} = [ join(' ',(caller)[1,2]), $_[0] ];weaken($DEF{int $_[0]}[1]);
	return cb => bless( shift,'__callback__'), @_;
}

sub after (&$)    {
	#$DEF{int $_[0]} = [ join(' ',(caller)[1,2]), $_[0] ];weaken($DEF{int $_[0]}[1]);
	#my $cb = bless( shift,'__callback__');
	my $cb = shift;
	my $t;$t = AnyEvent->timer(
		after=> $_[0],
		cb { undef $t; $cb->(); undef $cb } # undef $cb;
	);
	return;
}
sub periodic(&$)  {
	#warn refcount($_[0]);
	#warn findref $_[0];
	#$DEF{int $_[0]} = [ join(' ',(caller)[1,2]), $_[0] ];weaken($DEF{int $_[0]}[1]);
	#my $cb = bless( shift,'__callback__');
	my $cb = shift;
	my $t;$t = AnyEvent->timer(
		after => $_[0],
		interval => $_[0],
		cb { $cb->() },
	);
	return AnyEvent::Util::guard(sub {
		#warn "Guarding";
		undef $t;
		undef $cb;
	});
}

sub __callback__::DESTROY {
	delete($DEF{int $_[0]});
	#warn "Destroying callback @_ defined ".delete($DEF{int $_[0]})->[0]. " by @{[ (caller)[1,2] ]}\n";
}

sub COUNT {
	for (keys %DEF) {
		$DEF{$_}[1] or next;
		my $name = sub_fullname($DEF{$_}[1]);
		warn "Not destroyed: $_ ".($name ? $name : 'ANON')." (".refcount($DEF{$_}[1]).") defined $DEF{$_}[0]\n".findref($DEF{$_}[1]);
		#delete $DEF{$_};
	}
}
END {
	warn "sb::END";
	COUNT();
}

1;
