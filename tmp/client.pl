#!/usr/bin/env perl -w

package main;
BEGIN { $ENV{CB_DEBUG} = 1; }
use strict;
use lib::abs '../lib', '../../Queue-PQ/lib', '../../AnyEvent-Connection/lib';
#use AnyEvent::Impl::Perl;
use AnyEvent::Impl::EV;
use AnyEvent::Queue::Server::PMQ;
use AnyEvent::Queue::Client::PMQ;
use AnyEvent::Queue::Client::Beanstalk;
use Time::HiRes qw(time);
use R::Dump;
use Devel::FindRef;sub findref;*findref = \&Devel::FindRef::track;
use Scalar::Util qw(weaken);
use Devel::Refcount qw( refcount );
use AnyEvent::Util;
use Sub::Identify ':all';
use AnyEvent::cb;
use Sub::Name;
use BSD::Process;

#my $inverval = 0.001;
my $inverval = 0.01;
my $rate = 10;
#my $rate = 3;
my $reload = 10;
#my @queues = qw(test1 test2 test3);
my @queues = qw(test1);
my @range = 1..30;
#my @range = 1..1;
#our %history;
our %taken;
our %seen;
our $port = 12345;
our ($server,$loader,$client);

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

	#exit 0;
	local $SIG{__WARN__} = sub { CORE::warn("CLIENT @_"); };
	my $cv = AnyEvent->condvar;
	my $c2;
	$SIG{INT} = sub { $cv->send };
	$0 = "test-pmq - client";
	measure('start');
	my $see = 
	$c2 = AnyEvent::Queue::Client::PMQ->new(
		servers => ['localhost:12345'],
	#$c2 = AnyEvent::Queue::Client::Beanstalk->new(
	#	servers => ['localhost:11311'],
		reconnect => 1,
		timeout => 1,
	);
	weaken($see);
	measure('client');


	our %watchers;
	our %see;

	$c2->connect(cb => subname 'connect.inline.cb' => sb {
		warn "Client connected @_";
		measure('connect');
		$c2->reg_cb( got_stats => subname 'got.stats' => sb {
			shift;
			my $stats = shift;
			#warn("Got stats $stats->{queue}{test1}{taken} $stats->{queue}{test2}{taken} $stats->{queue}{test3}{taken}");
			measure('stats');
			$c2->queues(cb => subname 'list.queues' => sb {
				my $q = shift or return warn("queues failed: @_");
				measure('queues');
				warn("Got stats ".join(' ',map { $stats->{queue}{$_}{taken} } @$q ));
				#warn "Source have queues: [@$q]";
				for my $sv (@$q) {
					my $key;
					warn "Restart $sv with taken: ".Dump $c2->{taken};
					measure('begin create watcher');
					$watchers{$sv} = 
					$c2->watcher(
						src      => $sv,
						prefetch => 3,#$rate,
						rate     => 3,
						job     => (subname 'watcher.job' => sb {
							my $w = shift;
							my ($job,$err) = @_;
							measure('job');
							#$cv->send;
							warn "job ".$job->id;
							if ($job) {
								return $w->release(job => $job);
								$seen{$job->{id}}++;
								if ($taken{$sv}{$job->{id}}++) {
									warn "$sv.$job->{id} already taken but have one more\n";
								}
								#warn "$w ++$job->{id}";
								#return;
								$see{int $w}++;
								#after {
										#warn "End timer $w / ".refcount($w);
										#$w->requeue(job => $job);return;
										$w->requeue(job => $job, cb => sub {
											#shift or warn "@_";
											delete $taken{$sv}{$job->{id}};
										});
										$see{int $w}--;
								#} 0.1;
							} else {
								warn "WTF @_?";
							}
						}),
						nomore => sub {
							#warn("No more items for $sv");
						},
					);
					warn "New watcher: ".int($watchers{$sv});
					measure('watcher');
					$c2->after( 100, sb {
						measure('timer end');
						$cv->send;
					});
					measure('timer');
					#warn "(".int($watchers{$sv})." : ".refcount($watchers{$sv}).") New watcher";
					#print #Devel::FindRef::track $see{$key} if $key and $see{$key};
				}
			});
			return;
			measure('call queues');
			$c2->after($reload, sb {
				measure('invoke fullstats');
				$c2->fullstats(cb => sb { $c2->event( got_stats => @_ ) })
			});
			measure('delay fullstats');
		});
		$c2->event( got_stats => () );return;
		#$c2->fullstats(cb => $statscb);return;
	});
	$cv->recv;
	warn "Finishing";
	undef $c2;
	%watchers = ();
	#warn Dump \%watchers,\%see, $c2;
	if ($see) {
		print findref $see;
	}
	exit 0;

