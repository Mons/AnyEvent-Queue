package AnyEvent::Queue::Watcher;

use strict;
use base 'Object::Event';
use Data::Dumper;
use Carp;
use Time::HiRes qw(time);
use Scalar::Util qw(weaken isweak);

use constant::def DEBUG => 0;
use Devel::Leak::Cb;
use Dash::Leak;

BEGIN { eval { require Devel::Refcount; *refcount = \&Devel::Refcount::refcount; 1 } or *refcount = sub { -1 }; }

sub after (&$)    { my $cb = shift; my $t;$t = AnyEvent->timer( after=> $_[0], cb => sub { undef $t; goto &$cb });return; }

our %TAKEN;

sub new {
	my $pk = shift;
	my %args = @_;
	my %cb;
	for (keys %args) {
		$cb{$_} = delete $args{$_} if UNIVERSAL::isa( $args{$_}, 'CODE' );
	}
	my $self = bless {
		prefetch => 1, # take N tasks in parallel
		delay    => 0, # fetch using delay between takes
		rate     => 0, # fetch using rate limit
		%args,
	}, $pk;
	$self->reg_cb(%cb);
	%args = %cb = ();
	$self;
}

sub reset {
	my $self = shift;
	if ($self->{rate}) {
		$self->{rps}{_} = [];
		$self->{rps}{max} = 20/$self->{rate};
		$self->{rps}{wait} =                    # last wait passed
		$self->{rps}{delay} = 1/$self->{rate};  # calculated delay for requested rate
	}
	$self->{curfetch} = 1;#$self->{prefetch};
	$self->{taken}{count} = 0;
	$self->{taking} = 0; # currently in progress
	$self->{nomore} = 0;
}


sub _loop {
	weaken(my $self = shift);
	#warn "watcher loop $self->{taking} + $self->{taken}{count} cmp $self->{curfetch}";
	leaksz 'watcher loop in';
	#weaken(my $x = shift);
	#$self->_run
	#weaken($self) unless isweak($self);
	#warn "run $self | ".refcount($self);
	if ($self->{stopping}) {
		if (!$self->{taking} and !$self->{taken}{count}) {
			$self->{stopped} = 1;
			my $cb = delete $self->{stopping};
			$cb->();
		} else {
			#warn "stopping, but have $self->{taking}+$self->{taken}{count}";
		}
		return;
	}
	return if $self->{taking} + $self->{taken}{count} > $self->{curfetch};
	my $delay = 0;
	# Distribute "parralel" takers evenly
	if ($self->{rate}) {
		(undef,$delay) = $self->rps(1); # concurrency here = 1, we need minimal delay
	} else {
		#$delay = 0.0001;
	}
	for (($self->{taken}{count} + $self->{taking} + 1)..$self->{curfetch}) {
		my $cdelay = $delay * ($_ - $self->{taken}{count} - 1);
		if ($cdelay) {
			#warn "take after $cdelay";
			$self->{client}->after( $cdelay, cb {
				$self or return;
				return if $self->{taking} + $self->{taken}{count} >= $self->{curfetch};
				$self->{taking}++;
				leaksz 'watcher loop after in 1';
				$self->take;
			});
		} else {
			#warn "non-delayed take $self ".refcount($self);
			$self->{taking}++;
			leaksz 'watcher loop after in 2';
			$self->take;
			return;
			#$self->{client}->after( 0.0000001, cb {
			#	$self or warn("No self"),return;
			#	$self->{taking}++;
			#	::measure('watcher loop after in 3') if ::MEMTEST;
			#	$self->take;
			#});
		}
	}
	leaksz 'watcher loop out';
	return;
	
}

sub _next {
	weaken(my $self = shift);
	#my $self = shift;
	unless ($self) {
		warn "Destroyed during next";
		return;
	}
	return $self->_loop if $self->{stopping};
	if ($self->{rate}) {
		my ($rps,$wait) = $self->rps($self->{curfetch});
		#warn "rated $self->{rate} take: wait $wait";
		$self->{taking}++;
		if ($wait) {
			$self->{client}->after( $wait, cb {
				$self and $self->maybe_take;
			});
		} else {
			$self->take;
		}
	}
	elsif ($self->{delay}) {
		warn "release with self.delay $self->{delay}";
		$self->{taking}++;
		$self->{client}->after( $self->{delay}, cb {
			$self and $self->maybe_take;
		});
	}
	else {
		#warn "seq take";
		$self->{taking}++;
		# TODO: WTF?
		warn "taking while stopping" if $self->{stopping};
		#$self->take;
		$self->maybe_take;
	}
	return;
}

sub take {
	#warn "taking...";
	weaken( my $self = shift );
	weaken( my $client = $self->{client} );
	#my $self = shift;
	leaksz 'watcher take';
	$client->take(
		src => $self->{source},
		cb => sub {
			my ($j,$e) = @_;
			leaksz 'watcher taken';
			#refcount($self) == 1 and warn "Refcnt = 1 object ready to destroy";
			#warn "_take(@_)";
			$self->{taking}--;
			if (my $job = shift) {
				$client or warn("No client"),return;
				unless($self) {
					warn("Taken job without self, releasing $j->{id}");
					$client->release(job => $j);
					return;
				};
				$self->{nomore} = 0;
				$self->{taken}{count}++;
				$self->{taken}{ $job->src }{ $job->id } = $job;
				push @{ $self->{rps}{_} },time if $self->{rps};
				$self->event( job => $job ) or warn "!!! job cb not handled";
				if ( $self->{curfetch} < $self->{prefetch} ) {
					#warn "Raise curfetch $self->{curfetch}+1";
					$self->{curfetch}++;
					$self->_loop;
				}
				return;
			}
			elsif (my $err = shift) {
				#warn "Fail $err";
				if ($err eq 'NO JOBS') {
					$self->{nomore}++ or $self->event( nomore => () ) or warn "nomore not handled";
					$self->{curfetch} = 1 if $self->{curfetch} > 1;
					unless ($self->{stopping}) {
						#warn "(".int($self)." : ".refcount($self).") not stopping, want more after delay ($self->{taking}/$self->{curfetch}).";
						#warn "no more, delay loop";
						$self->{client}->after( 0.3, cb {
							$self or return;
							#warn "delayed loop";
							$self->_loop;
						});
					} else {
						# check for termination
						$self->{client}->after( 0.0000001, cb {
							$self->_loop;
						});
						#$self->_loop;
					}
					return;
				} else {
					warn "Take failed: $err";
					$self->{client}->after(0.1, cb {
						$self->{taking}++;
						$self->maybe_take;
					});
				}
			}
			else {
				die "Take return nothing: @_";
				#$self->event(_taken => ());
			}
			return;
			#warn "Taken @_";
			#$client or warn("No client"),return;
			#$self or warn("Taken job without self"),
			#return $client->release(job => $_[0]);
			#$self and $self->event(_taken => @_);
			#undef $self;
		},
	);
	return;
}

sub maybe_take {
	weaken( my $self = shift );
	return $self->{taking}--, $self->_loop if $self->{stopping};
	return $self->{taking}-- if $self->{taking} + $self->{taken}{count} > $self->{curfetch};
	return $self->take;
	# below also works
}

sub stop {
	my $self = shift;
	my %args = @_;
	$self->{stopping} = $args{cb} or croak "cb required";
	%args = ();
	return;
}

sub rps {
	my $self = shift;
	return wantarray ? (0,0) : 0 unless $self->{rps};
	my $concurrency = shift || 1;
	#warn "rps ($concurrency)";
	my $now = time;
	my $max = $self->{rps}{max};  # keep 20 last requests
	my $see = $now - $max;
	shift @{ $self->{rps}{_} }
		while @{$self->{rps}{_}}
		and $self->{rps}{_}[0] < $see;
	my $n = @{ $self->{rps}{_} };
	my ($rps,$wait);
	if (!$n or $self->{rps}{_}[-1] - $self->{rps}{_}[0] < $self->{rps}{delay}/2) {
		($rps,$wait) = ($self->{rate},$self->{rps}{delay});
	} else {
		# calculate wasted time as actual average time from last requests - last passed wait
		my $lastreq = $self->{rps}{_}[-1] - $self->{rps}{_}[-2];
		# alt: take 2 last requests
		#my $lastreq = ( $self->{rps}{_}[-1] - $self->{rps}{_}[$n > 2 ? -3 : -2] ) / ( $n > 2 ? 2 : 1 );
		my $wasted = $lastreq - $self->{rps}{wait};
		# if no HIRES, waste could be negative
		$wasted = 0 if $wasted < 0;
		
		# total runtime of last N requests:

		# forecast next request time as lastreq
		my $time = $now - $self->{rps}{_}[0] + $lastreq;
		$rps = $n / ( $time );
		my $req = ( $time ) / $n if DEBUG; # only for debug
		
		# alt: take actual rutime. (n-1 to take req wait into account)
		# my $time = $now - $self->{rps}{_}[0];
		# $rps = ($n - 1) / ( $time );
		# my $req = ( $time ) / ( $n - 1 );
		
		my $shft = $lastreq - $req if DEBUG; # only for debug
		$wait = $self->{rps}{delay} - $wasted; # if time wasted somewhere, use smaller delay
		$wait = 0 if $wait < 0;    # if waste is greater, than rdelay, then we can't reach required => no wait
		warn sprintf "n=%2d/max=%0.2f, time=%d, rps=%0.2f, wait=%0.2f (%0.2f), wasted=%0.2f, req=%0.2f, lreq=%0.2f, shft=%+0.4f",
			$n, $max, $time, $rps, $wait, $self->{rps}{wait}, $wasted, $req, $lastreq, $shft
			if DEBUG;
	}
	$wait *= $concurrency;
	$self->{rps}{wait} = $wait;
	#warn "rps ($concurrency) = $rps,$wait";
	return wantarray ? ($rps,$wait) : $rps;
}

sub run {
	my $self = shift;
	$self->handles('job') or croak "job cb not handled";
	weaken($self);
	if ($self->{stopped}) {
		$self->{stopped} = 0;
	}
	elsif ($self->{stopping}) {
		warn "Run invoked during stopping. Stopping aborted";
		$self->{stopped} = 0;
		undef $self->{stopping};
	}
	$self->reset;
	$self->_loop;
	return;
}

sub _back {
	weaken( my $self = shift );
	my $op = shift;
	my %args = @_;
	my $job = $args{job};
	$self->{client} or return $args{cb}(undef,"No client"),%args = ();
	my $taken = $self->{taken};
	weaken( $self->{waitingcb}{int $args{cb}} = $args{cb} ) if $args{cb};
	$self->{client}->$op(
		@_,
		cb => cb {
			local *__ANON__ = $op.'.cb';
			$taken->{count}--;
			delete $taken->{$job->{src}}{$job->{id}};
			undef $taken;
			$_[0] and @_ > 1 and warn "$op $job->{src}.$job->{id} failed: @_[1..$#_]";
			if ($self) {
				delete $self->{waitingcb}{int $args{cb}} if $args{cb};
				eval{ delete($args{cb})->(@_) if $args{cb}; 1 } or carp "$op cb failed: $@";
				%args = ();
				$self->_next;
			} else {
				warn "requeued with destroyed";
				eval{ delete($args{cb})->(@_) if $args{cb}; 1 } or carp "$op cb failed: $@";
				%args = ();
			}
			#undef $self;
		},
	);
}
sub ack     { shift->_back( ack => @_ ) }
sub requeue { shift->_back( requeue => @_ ) }
sub release { shift->_back( release => @_ ) }
sub bury    { shift->_back( bury => @_ ) }

sub taken_keys {
	my $self = shift;
	return join('; ', map { ref $self->{taken}{$_} ? "$_:[".join(',', keys %{$self->{taken}{$_}}).']' : () } keys %{$self->{taken}} );
}

sub destroy {
	my ($self) = @_;
	$self->DESTROY;
	bless $self, "AnyEvent::Queue::Watcher::destroyed";
}
sub AnyEvent::Queue::Watcher::destroyed::AUTOLOAD {}
sub DESTROY {
	my $self = shift;
	warn "(".int($self).") Destroying watcher $self->{source}";
	for my $k (keys %{ $self->{waitingcb} || {} }) {
		if ($self->{waitingcb}{$k}) {
			delete($self->{waitingcb}{$k})->(undef, "Watcher destroyed");
		} else {
			delete $self->{waitingcb}{$k};
		}
	}
	if ($self->{stopping}) {
		eval{ delete($self->{stopping})->(); };warn if $@;
	}
	$self->{client} and $self->{client}->destroy;
	weaken( my $w = $self->{client} );
	%$self = ();
	if ($w) {
		warn "Not cleaned watcher client ".refcount($w);
	}
	return;
}

1;
