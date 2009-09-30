#!/usr/bin/perl

use strict;
BEGIN { $ENV{CB_DEBUG} = 1; }
use lib::abs '../lib', '../../Queue-PQ/lib', '../../AnyEvent-Connection/lib';
use AnyEvent::Impl::Perl;
#use AnyEvent::Impl::EV;
use AnyEvent;
use AnyEvent::cb;
use AnyEvent::Socket;
use Data::Dumper;
use IO::Handle;
use R::Dump;
use Rambler::Config;
use Time::HiRes qw(time);
use BSD::Process;
use Devel::Leak;

our $cmem = 0;
our $memc = 0;
sub measure ($;$) {
	my $op = shift;
	use integer;
	#return $cmem if $cmem and $memc++ % 100;
	#my $ps = (split /\n/,`ps auxwp $$`)[1];
	#my ($mem) = $ps =~ /\w+\s+\d+\s+\S+\s+\S+\s+(\d+)\s+(\d+)/;
	my $mem = BSD::Process->new->{size} / 1024;
	my $delta = $mem - $cmem;
	if ($delta != 0) {
		$cmem = $mem;
		warn sprintf "%s: %+d\n",$op,$delta;
		$_[0]() if @_;
		#warn Dump $cl;
	}
}
my $cfg = Rambler::Config->instance->get_config( config => 'daemon' ) or die "Config not found!\n";

#warn "Starting requeue on @{$cfg->{$cfg->{listener}{source}}{servers}}: listener-fail => listener\n";
my $cv;
my $debug = 0;
$cv = AnyEvent->condvar;
my $do_exit = 0;
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
		my $reader;
=for rem
		$SIG{TERM} = $SIG{INT} = sub {
			undef $reader;
			$cv->send;
			#$w->stop(cb => sub{ $cv->send });
		};
		my $cnt = 0;
		$src->__watch_only('test1',sb {
			#use Devel::Leak;
			$reader = sb {
				$src->{con}{h}->push_write("reserve-with-timeout 1\015\012");
				$src->{con}{h}->push_read( regex => qr<\015?\012> => sb {
					shift;
					local $_ = shift;
					printf "\r".(' 'x10)."\r%0.2f",++$count/(time-$start);# if !$count % 10;
					#warn ".";
					#warn "Got line $_";
					if (/RESERVED (\d+) (\d+)/) {
						my ($job,$size) = ($1,$2);
						::measure('reserve ok, reading');
						$src->{con}{h}->unshift_read(chunk => $size+2, cb => sub {
							::measure('read data');
							$src->{con}{h}->push_write("release $job 0 0\015\012");
							$src->{con}{h}->push_read(regex => qr<\015?\012> => sub {
								shift;
								#warn "Released: @_";
							});
						});
						#Devel::Leak::CheckSV($handle);
						#$reader->();
						#return;
						if ($reader){# and ++$cnt < 10) {
							$reader->();
						} else {
							warn "!!! STOP !!!";
							$cv->send;
						}
					} else {
						warn "Fuck $_";
					}
				});
			};
			#Devel::Leak::NoteSV($handle);
			$reader->() for 1..3;
		});
		return;
=cut		
		my $w = $src->watcher(
			src => 'test1',
			prefetch => 2,
			#rate => 3,
			job => sub {
				my $w = shift;
				if ( my $job = shift ) {
					warn "job = ".$job->id;
					#printf "\r".(' 'x10)."\r%0.2f",++$count/(time-$start);# if !$count % 10;
					return $w->release(job => $job, sub => {});
				}
			},
			nomore => sub { warn "no more"; $cv->send if $do_exit },
		);
		$SIG{TERM} = $SIG{INT} = sub {
			$w->stop(cb => sub {
				undef $w;
				$cv->send;
			});
			#$w->stop(cb => sub{ $cv->send });
		};
	},
);

$src->connect;

$cv->recv;
#warn Dump $src;
#warn Dump $src->{watchers};
undef $src;
warn "Finished ($count)\n";
