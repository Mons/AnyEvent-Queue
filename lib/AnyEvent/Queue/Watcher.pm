package AnyEvent::Queue::Watcher;

use base 'Object::Event';
use Data::Dumper;
use Carp;

use Time::HiRes qw(time);

use constant::def DEBUG => 0;

sub new {
	my $pk = shift;
	#warn Dumper \@_;
	my %args = @_;
	my %cb;
	for (keys %args) {
		$cb{$_} = delete $args{$_} if ref $args{$_} eq 'CODE';
	}
	my $self = bless {
		prefetch => 1, # take N tasks in parallel
		delay    => 0, # fetch using delay between takes
		rate     => 0, # fetch using rate limit
		%args,
	}, $pk;
	if ($self->{rate}) {
		$self->{rps}{_} = [];
		$self->{rps}{max} = 20/$self->{rate};
		$self->{rps}{wait} =                    # last wait passed
		$self->{rps}{delay} = 1/$self->{rate};  # calculated delay for requested rate
	}
	$self->{curfetch} = $self->{prefetch};
	$self->{taken}{count} = 0;
	$self->{taking} = 0; # currently in progress
	$self->{nomore} = 0;
	$self->reg_cb(%cb);
	$self;
}

sub stop {
	my $self = shift;
	my %args = @_;
	$self->{stopping} = $args{cb} or croak "cb required";
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
#	my $rdelay = $self->{rps}{delay};
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

sub taken_cb {
	my $self = shift;
	my $job = shift;
	$self->{taking}--;
	#warn "taken: cf = $self->{curfetch}, taking = $self->{taking}, taken = $self->{taken}{count}";
	if ($job) {
		$self->{nomore} = 0;
		push @{ $self->{rps}{_} },time if $self->{rps};
		$self->eventif( job => $job ) or warn "job not handled";
		$self->{taken}{count}++;
		if ( $self->{curfetch} < $self->{prefetch} ) {
			$self->{curfetch}++;
			$self->_run;
		};
	} else {
		$self->{nomore}++ or $self->eventif( nomore => () ) or warn "nomore not handled";
		if ($self->{curfetch} > 1) {
			warn "curfetch $self->{curfetch} => 1";
			$self->{curfetch} = 1;
		}
		unless ($self->{stopping}) {
			my $timer;
			#warn "want more";
			$timer = AnyEvent->timer( after => 0.3 , cb => sub {
				undef $timer;
				$self->_run;
				#$self->{client}->take( src => $self->{source}, cb => sub { $self->taken_cb(@_) } );
			});
		} else {
			$self->_run; # check for termination
		}
	}
}

sub _run {
	my $self = shift;
	#warn "_run ($self->{taken}{count})";
	if ($self->{stopping}) {
		if (!$self->{taking} and !$self->{taken}{count}) {
			$self->{stopped} = 1;
			my $cb = delete $self->{stopping};
			$cb->();
		}
		return;
	}
	return if $self->{taking} + $self->{taken}{count} > $self->{curfetch};
		my $delay = 0;
		# Distribute "parralel" takers evenly
		if ($self->{rate}) {
			(undef,$delay) = $self->rps(1); # concurrency here = 1, we need minimal delay
		}
		#warn " $self->{curfetch} ($self->{taking}) :  $self->{taken}{count} + $self->{taking} .. $self->{curfetch}";
		for (($self->{taken}{count} + $self->{taking} + 1)..$self->{curfetch}) {
			my $cdelay = $delay * ($_ - $self->{taken}{count} - 1);
			#warn "_run with cdelay = $cdelay ($delay x $self->{curfetch})";
			if ($cdelay) {
				my $timer;
				$timer = AnyEvent->timer( after => $cdelay , cb => sub {
					undef $timer;
					#warn "<<take";
					return if $self->{taking} + $self->{taken}{count} >= $self->{curfetch};
					$self->{taking}++;
					$self->{client}->take( src => $self->{source}, cb => sub { $self->taken_cb(@_) } );
				});
			} else {
				#warn "<<take";
				$self->{taking}++;
				$self->{client}->take( src => $self->{source}, cb => sub { $self->taken_cb(@_) } );
			}
		}
}

sub run {
	my $self = shift;
	if ($self->{stopped}) {
		$self->{stopped} = 0;
	}
	elsif ($self->{stopping}) {
		warn "Run invoked during stopping. Stopping aborted";
		$self->{stopped} = 0;
		undef $self->{stopping};
	}
	$self->_run;
}

sub maybe_take {
	my $self = shift;
	#warn "maybe take ($self->{taking} + $self->{taken}{count} > $self->{curfetch})";
	return $self->{taking}--, $self->_run if $self->{stopping};
	return $self->{taking}-- if $self->{taking} + $self->{taken}{count} > $self->{curfetch};
	#warn "maybe take ($self->{taking} + $self->{taken}{count} > $self->{curfetch}) do";
	$self->{client}->take( src => $self->{source}, cb => sub { $self->taken_cb(@_) } );
}

sub next_take {
	my $self = shift;
	$self->{taken}{count}--;
	#warn "next take ($self->{taken}{count})";
	return $self->_run if $self->{stopping};
	if ($self->{rate}) {
		# TODO: rate limit
		my ($rps,$wait) = $self->rps($self->{curfetch});
		#warn "Rate: $self->{rate}, rps = $rps, wait = $wait";
		my $timer;
		$self->{taking}++;
		$timer = AnyEvent->timer( after => $wait , cb => sub {
			undef $timer;
			$self->maybe_take;
		});
	}
	elsif ($self->{delay}) {
		my $timer;
		$self->{taking}++;
		$timer = AnyEvent->timer( after => $self->{delay} , cb => sub {
			undef $timer;
			$self->maybe_take;
		});
		
	} else {
		$self->{taking}++;
		$self->{client}->take( src => $self->{source}, cb => sub { $self->taken_cb(@_) } );
	}
	
}

sub release {
	my $self = shift;
	my %args = @_;
	$self->{client} or return $args{cb}->(undef,"No client");
	$self->{client}->release(
		%args,
		cb => sub {
			local *__ANON__ = 'release.cb';
			eval{ $args{cb}->(@_) if $args{cb}; 1 } or carp "cb failed: $@";
			$self->next_take();
		}
	);
}

sub requeue {
	my $self = shift;
	my %args = @_;
	$self->{client} or return $args{cb}->(undef,"No client");
	$self->{client}->requeue(
		%args,
		cb => sub {
			local *__ANON__ = 'requeue.cb';
			eval{ $args{cb}->(@_) if $args{cb}; 1 } or carp "cb failed: $@";
			$self->next_take();
		}
	);
}

sub ack {
	my $self = shift;
	my %args = @_;
	$self->{client} or return $args{cb}->(undef,"No client");
	$self->{client}->ack( 
		%args,
		cb => sub {
			local *__ANON__ = 'ack.cb';
			eval{ $args{cb}->(@_) if $args{cb}; 1 } or carp "cb failed: $@";
			$self->next_take();
		}
	);
}

sub bury {
	my $self = shift;
	my %args = @_;
	$self->{client} or return $args{cb}->(undef,"No client");
	$self->{client}->bury( 
		%args,
		cb => sub {
			local *__ANON__ = 'bury.cb';
			eval{ $args{cb}->(@_) if $args{cb}; 1 } or carp "cb failed: $@";
			$self->next_take();
		}
	);
}

sub eventif {
	#my ($self,$name) = @_;
	my $self = shift;my $name = shift;
	return 0 unless $self->eventcan($name);
	$self->event($name => @_);
	return 1;
	#goto &{ $self->can('event') };
	
}

sub eventcan {
	my $self = shift;
	my $name = shift;
	return undef unless exists $self->{__oe_events}{$name};
	return scalar @{ $self->{__oe_events}{$name} };
}

sub handle {
	my ($self,$con, $cmd, @args ) = @_;
	$self->eventif( $cmd => $con, @args )
		or do {
			$con->reply("NOT SUPPORTED");
			warn "$cmd event not handled";
			0;
		};
}

1;
