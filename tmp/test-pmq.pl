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
my @range = 1..30;
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

#if (0) {
unless ($client = fork) {
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
=for rem
		$c2->fullstats(cb => subname 'full.stats' => sb {
			$c2;
			warn "stats";
		});
		$c2->queues(cb => subname 'list.queues' => sb {
			$c2;
			$c2->reg_cb( test => subname 'test.cb' => sb {
				$c2;
				warn "test ok";
			});
			$c2->fullstats( cb { $c2->event( test => @_ ) } );
			warn "queues";
		});
		return;
=cut

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
					if( $watchers{$sv} ) {
						#warn "Watcher: $watchers{$sv}";
						#warn "Watcher: taken @{[ %{$watchers{$sv}{taken}} ]}";
						#warn "Watcher have taken: $watchers{$sv}{taken}{count} ".$watchers{$sv}->taken_keys;
						if ($watchers{$sv}{taken}{count} == 0) {
							warn Dump $c2->{taken};
							#$cv->send if %{ $c2->{taken} };
						}
						#warn Dump $watchers{$sv};
						#print Devel::FindRef::track $watchers{$sv};
						#$cv->send;
						#weaken($see{ $key = int $watchers{$sv} } = $watchers{$sv});
					};
=for rem
					weaken( my $wx = $watchers{$sv} );
					warn "Restart with taken: ".Dump $c2->{taken};
					measure('begin create watcher');
					$watchers{$sv} = $c2->watcher(
						src      => $sv,
						prefetch => $rate,
						#rate     => $rate,
						job => sub {
							my $w = shift;
							my ($job,$err) = @_;
							measure('job');
							if ($job) {
								$seen{$job->{id}}++;
								if ($taken{$sv}{$job->{id}}++) {
									warn "$sv.$job->{id} already taken but have one more\n";
									#$cv->send;
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
						},
						nomore => sub {
							#warn("No more items for $sv");
						},
					);
					measure('watcher');
					$c2->after( 3, sb {
						if ($wx) {
							warn "Not cleaned watcher ".eval{ refcount($wx) };
							warn "See: ".$see{int $wx};
							print findref $wx;
							#$cv->send;
							die "FTW";
						} else {
							warn "Cleaned";
						}
						measure('timer end');
					});
=cut
					measure('timer');
					#warn "(".int($watchers{$sv})." : ".refcount($watchers{$sv}).") New watcher";
					#print #Devel::FindRef::track $see{$key} if $key and $see{$key};
				}
			});
			#return;
			measure('call queues');
			$c2->after($reload, sb {
				measure('invoke fullstats');
				$c2->fullstats(cb => sb { $c2->event( got_stats => @_ ) })
			});
			measure('delay fullstats');
		});
		$c2->event( got_stats => () );return;
		#$c2->fullstats(cb => $statscb);return;
		my $sv = $queues[0];
		my $neww = sub {
			$watchers{$sv} = $c2->watcher(
				src      => $sv,
				prefetch => $rate,
				rate     => $rate,
				job => sub {
					#shift->requeue(job => shift);
					my ($w,$job) = @_;
					warn "$w ++$job->{id}";
					after {
						$w->requeue(job => $job, cb => sub {
							#warn "
							
						});
					} 0.1;
					#%watchers;
				},
				nomore => sub {
					#%watchers;
				},
			);
		};
		after {
			warn "Starting watcher";
			$neww->();
		} 0.1;
		my $p;$p = periodic {
			$p;
			weaken( my $w = $watchers{$sv} );
			warn "Cleaning watcher ".refcount($w);
			delete $watchers{$sv};
			after {
				if ($w) {
					warn "Not cleaned watcher ".refcount($w);
					print findref $w, 20;
				} else {
					warn "Cleaned";
					$neww->();
				}
			} 1;
			
		} 3;
		return;
		after {
			weaken( my $w = $watchers{$sv} );
			warn "Cleaning watcher ".refcount($w);
			delete $watchers{$sv};
			after {
				if ($w) {
					warn "Not cleaned watcher ".refcount($w);
					print findref $w, 20;
				} else {
					warn "Cleaned";
				}
			} 1;
		} 0.5;
	});
	$cv->recv;
	undef $c2;
	%watchers = ();
	#warn Dump \%watchers,\%see, $c2;
	if ($see) {
		print findref $see;
	}
	exit 0;
}
$0 = "test-pmq - terminal";
$SIG{INT} = sub { kill INT => $server,$loader,$client };

waitpid($server,0) if $server;
waitpid($loader,0) if $loader;
waitpid($client,0);

END {
	kill TERM => -$$ if $server and $client and $loader;
}

__END__


my $start = time;
my ($ins,$upd,$del) = (0)x3;
$| = 1;

$c1->connect(cb {
	warn "Connected: @_";
	#my $w;$w = AnyEvent->timer(interval => $inverval, cb => sub {
	my $w;$w = AnyEvent->timer(after => $inverval, cb => sub {
		undef $w;
		for my $dst (@queues) {
			for my $id (@range) {
				$c1->put(
					dst => $dst,
					id  => $id,
					data => { x => 'x'x10 },
					cb => sub {
						#push @{$history{$id}},"insert: $! ".( @_ > 1 ? "$_[1]" :'').( $taken{$id} ? ' +taken' : '' );
						shift or $!{EEXIST} or warn "create $dst.$id failed: @_";
					},
				);
			}
		}
		return;

		for my $dst (@queues) {
			my $id = $range[ int rand $#range ];
			#warn "Timer $dst $id";
			#$history{$id} ||= [];
			$c1->peek( $dst, $id, cb {
				if (my $j = shift) {
					#warn "Peeked ". Dump $j;
					if (rand > 0.8) {
					#if (rand > 0.2) {
						$del++;
						$c1->delete(
							cb {
								#push @{$history{$id}},"delete: $! ".( @_ > 1 ? "$_[1]" :'').( $taken{$id} ? ' +taken' : '' );
								shift or $!{ENOENT} or warn "Delete failed: @_";
							}
							job => $j,
						);
					} else {
						$upd++;
						$c1->update(
							cb {
								#push @{$history{$id}},"update: $! ".( @_ > 1 ? "$_[1]" :'').( $taken{$id} ? ' +taken' : '' );
								shift or $!{ENOENT} or warn "Update failed: @_ / $!";
							}
							job => $j,
							pri => int rand 100,
							data => { y => 'y'x100 },
						);
					}
				} else {
					$ins++;
					#warn "No job, create";
					$c1->put(
						dst => $dst,
						id  => $id,
						data => { x => 'x'x100 },
						cb => sub {
							#push @{$history{$id}},"insert: $! ".( @_ > 1 ? "$_[1]" :'').( $taken{$id} ? ' +taken' : '' );
							shift or $!{EEXIST} or warn "create $dst.$id failed: @_";
						},
					);
				}
				my $int = time - $start;
				#printf "\r".(" "x40)."\rins: %0.2f/s, upd: %0.2f/s del %0.1f/s  ", $ins/$int, $upd/$int, $del/$int;
			} );
		}
		return;
		#$rc or $self->log->error("Can't update job $exists_job->{src}.$exists_job->{id} in queue: $e");
		#	my ($rc,$e) = 
		#$rc or warn("Can't create job in queue: $e");
	});
});

$cv->recv;
warn Dump \%seen;
