#!/usr/bin/perl

use strict;
BEGIN { $ENV{CB_DEBUG} = 1; }
use lib::abs '../lib', '../../Queue-PQ/lib', '../../AnyEvent-Connection/lib';
use AnyEvent::Impl::Perl;
#use AnyEvent::Impl::EV;
use AnyEvent;
use AnyEvent::cb;
use AnyEvent::Handle;
use Data::Dumper;
use IO::Handle;
use R::Dump;
use Rambler::Config;
use Time::HiRes qw(time);
use BSD::Process;
use Devel::Leak;

our $cmem = 0;
sub measure ($) {
	my $op = shift;
	use integer;
	my $mem = BSD::Process->new->{size} / 1024;
	my $delta = $mem - $cmem;
	if ($delta != 0) {
		$cmem = $mem;
		warn sprintf "%s: %+d\n",$op,$delta;
		#warn Dump $cl;
	}
}
my $handle;
my $count;
=for rem
#Devel::Leak::NoteSV($handle);
my $self = { q => [] };
push @{ $self->{q} }, sub {} for 1..10;
while (1) {
	shift @{ $self->{q} };
	push @{ $self->{q} }, sub {};
	last if ++$count > 10000;
	::measure('loop');
}
#Devel::Leak::CheckSV($handle);
exit 0;
=cut
open my $f, '+>', undef;
print $f "line1\nline2\n";
my $cv = AnyEvent->condvar;
Devel::Leak::NoteSV($handle);
my $h;$h = AnyEvent::Handle->new( fh => $f, on_error => sb { warn "$_[2]";shift->destroy;$cv->send; } );
$h->push_read(line => sb {
	warn "@_";
	$cv->send;
});
$cv->recv;
$h->destroy;
undef $h;
Devel::Leak::CheckSV($handle);

__END__

my $cfg = Rambler::Config->instance->get_config( config => 'daemon' ) or die "Config not found!\n";

#warn "Starting requeue on @{$cfg->{$cfg->{listener}{source}}{servers}}: listener-fail => listener\n";

my $debug = 0;
my $cv = AnyEvent->condvar;
my $do_exit = 1;
my $count = 0;
use AnyEvent::Queue::Client::PMQ;
use AnyEvent::Queue::Client::Beanstalk;
my $src = AnyEvent::Queue::Client::Beanstalk->new(
	servers => 'localhost:11311',
#my $src = AnyEvent::Queue::Client::PMQ->new(
#	servers => 'localhost:12345',
	timeout => 2,
	encoder => 'AnyEvent::Queue::Encoder::Any',
);
$| = 1;
$SIG{INT} = sub { $cv->send; };
$src->reg_cb(
	connected => sub {
		my ($q,$c,$host,$port) = @_;
		warn "source $host:$port connected";
		my $handle;
		my $start = time;
		my $count = 0;
		$SIG{TERM} = $SIG{INT} = sub {
			$cv->send;
			#$w->stop(cb => sub{ $cv->send });
		};
		$src->__watch_only('test1',sb {
			#use Devel::Leak;
			my $reader;$reader = sb {
				$src->{con}{h}->push_write("reserve-with-timeout 1\015\012");
				$src->{con}{h}->push_read( regex => qr<\015?\012> => sb {
					shift;
					local $_ = shift;
					#warn "Got line $_";
					if (/RESERVED (\d+) (\d+)/) {
						my ($job,$size) = ($1,$2);
						::measure('reserve ok, reading');
						$src->{con}{h}->unshift_read(chunk => $size+2, cb => sub {
							::measure('read data');
							$src->{con}{h}->push_write("release $job 0 0\015\012");
							$src->{con}{h}->push_read(regex => qr<\015?\012> => sb {
								shift;
								#warn "Released: @_";
							});
						});
						#Devel::Leak::CheckSV($handle);
						#$reader->();
					} else {
						warn "Fuck $_";
					}
				});
			};
			#Devel::Leak::NoteSV($handle);
			$reader->();
		});
		return;
		
		$src->watcher(
			src => 'test1',
			prefetch => 2,
			job => sub {
				my $w = shift;
				if ( my $job = shift ) {
					printf "\r".(' 'x10)."\r%0.2f",++$count/(time-$start);# if !$count % 10;
					return $w->release(job => $job, sub => {});
				}
			},
			nomore => sub { $cv->send if $do_exit },
		);
	},
);

$src->connect;

$cv->recv;
#warn Dump $src;
warn Dump $src->{watchers};
undef $src;
warn "Finished ($count)\n";
