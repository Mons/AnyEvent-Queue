package AnyEvent::Queue::Client::Beanstalk;

use strict;
use Carp;
use Data::Dumper;
use base 'AnyEvent::Queue::Client';
use Scalar::Util qw(weaken);

use AnyEvent::Queue::Encoder::YAML;
use bytes ();
our $yaml;
BEGIN { $yaml = AnyEvent::Queue::Encoder::YAML->new; }


=head1 REFERENCE

	+ put
	+ push

	+ take
	+ requeue
	+ release
	+ ack
	~ bury
	
	+ peek
	+ delete
	
	+ stats
	~ queues

=cut

use constant DEFPRI => 2048;
our $NL = "\015\012";

sub new {
	my $self = shift->next::method(@_);
	weaken(my $me = $self);
	$me->reg_cb(connected => sub {
		$me or return;
		#warn "Reset BT state";
		$me->{state}{watching} = 1;
		$me->{state}{watch} = { default => 1 };
		$me->{state}{use} = 'default';
	});
	$self;
}

sub _put  { shift->_add('put', @_) }
sub _push { shift->_add('push',@_) }

sub _ping { my $self = shift; my %args = @_; $self->__ping($args{cb}) }

sub _add {
	my $self = shift;
	my $cmd  = shift;
	
	my %args = @_;@_ = ();
	$args{data} or croak "No data for $cmd";
	$args{data} = $self->{encoder}->encode($args{data}) if ref $args{data};
	$args{cb} or return $self->event( error => "no cb for put at @{[ (caller)[1,2] ]}" );
	
	( $args{dst} ||= 'default' ) =~ s/\s+/-/sg;

	$args{delay} ||= 0;
	$args{pri} = 0 if $cmd eq 'push';

	my $put;$put = sub {
		undef $put;
		if ( ( my $sz = bytes::length($args{data}) ) > $self->{max_job_size} ) {
			$args{cb}(undef,"Job too big ($sz), max allowed: $self->{max_job_size}");
			%args = ();
			return;
		}
		$self->__use( $args{dst}, sub {
			shift or return do{ %args = ();$args{cb}->(undef,@_) };
			$self->__put($args{data},$args{pri},$args{delay},$args{ttr},sub {
				my $action = shift or return do{ %args = ();$args{cb}->(undef,@_) };
				warn "Job was buried" if $action =~ /buried/;
				my $id = shift;
				my $job = $self->job({
					id  => $id,
					pri => $args{pri},
					src => $args{dst},
					data => $args{data},
				});
				$args{cb}->($job);
				%args = ();
			});
		});
	};
	unless (defined $self->{max_job_size}) {
		$self->_stats(cb => sub {
			if (my $stats = shift) {
				$self->{max_job_size} = $stats->{'max-job-size'};
				$put->();
			} else {
				undef $put;
				$args{cb}->(undef,@_);
				%args = ();
			}
		});
	} else {
		$put->();
	}
}

sub _recv_job {
	my $self = shift;
	my $command = shift;
	my $src = shift;
	my %args = @_;

	$args{cb} or return $self->event( error => "no cb for take at @{[ (caller)[1,2] ]}" );
	$self->{con}->command($command, cb => sub {
		defined( local $_ = shift ) or return $args{cb}(undef,@_);
		if (/^(?:TAKEN|PEEKED)\s+(\d+)\s+(\d+)\s+(\d+)\s*$/) {
			my ($id,$len,$pri) = ($1,$2,$3);
			$self->{con}->recv( $len, cb => sub {
				my $data = shift;
				my $job = 
					$self->job({
						id  => $id,
						pri => $pri,
						src => $src,
						data => $data,
					});
				$args{cb}->($job);
			});
		} else {
			warn "$_";
			local $@ = $_;
			$args{cb}->(undef, $_);
		}
	});
}



sub _take {
	my $self = shift;
	my %args = @_;
	my $src = $args{src} || $args{dst} || 'default';
	::measure('bt take begin');
	$self->__watch_only($src, sub {
		::measure('bt watch ok');
		if (shift) {
			$self->__reserve(sub {
				::measure('bt reserve end');
				$args{cb}->(@_);
			});
		} else {
			$args{cb}->(undef,shift);
		}
	});
}

sub _peek {
	my $self = shift;
	my $src = shift;
	my $id = shift;
	$src =~ s/\s+/_/sg;
	$self->_recv_job("peek $src $id", $src, @_);
}

sub _extract_id_cb {
	my $self = shift;
	my %args = @_;
	$args{cb} or return $self->event( error => "no cb at @{[ (caller)[1,2] ]}" );
	my ($id,$pri);
	if ($args{job}) {
		$id = $args{job}{id};
		$pri = $args{job}{pri};
	} else {
		$id = $args{id};
		$pri = $args{pri};
	}
	$id or confess "No id";
	return ($id,$args{cb},$pri);
}

sub _give_back {
	my $self = shift;
	my $cmd = shift;
	my %args = @_;
	
	$args{cb} or return $self->event( error => "no cb for $cmd at @{[ (caller)[1,2] ]}" );

	my ($id,$dst);
	if ($args{job}) {
		$id = $args{job}{id};
		$dst = $args{job}{dst} || $args{job}{src};
	} else {
		$id = $args{id};
		$dst = $args{dst} || $args{src} || 'default';
	}

	$id or confess "No id for $cmd";

	#warn "Sending request | cb = $args{cb}";
	$self->{con}->command("$cmd $id$args{add}", cb => sub {
		defined( local $_ = shift ) or return $args{cb}(undef,@_);
		if (/^OK\s*$/) {
			$args{cb}->(1);
		} else {
			warn ">> $cmd $id$args{add}\n<< $_ ";
			local $@ = $_;
			$args{cb}->(undef, $_);
		}
	});
}

sub _requeue { croak "Not implemented" }
sub _release {
	my $self = shift;
	my %args = @_;
	
	$args{cb} or return $self->event( error => "no cb for release at @{[ (caller)[1,2] ]}" );

	my ($id,$dst,$pri,$delay);
	$delay = $args{delay} || 0;
	if ($args{job}) {
		$id = $args{job}{id};
		$dst = $args{job}{dst} || $args{job}{src};
		$pri = $args{job}{pri};
	} else {
		$id = $args{id};
		$dst = $args{dst} || $args{src} || 'default';
		$pri = $args{pri} || DEFPRI;
	}

	$id or confess "No id for release";
	$self->__release( $id, $pri, $args{delay}, $args{cb});
}
sub _ack     { shift->_delete(@_) }

sub _delete  {
	my $self = shift;
	my ($id,$cb) = $self->_extract_id_cb(@_);
	$self->__delete($id,$cb);
}

sub _bury  {
	my $self = shift;
	my ($id,$cb,$pri) = $self->_extract_id_cb(@_);
	$self->__bury($id,$pri,$cb);
}

sub _stats {
	my $self = shift;
	@_%2 and confess "Wrong arguments";
	my %args = @_;
	my $cmd = ($args{queue} ? "stats-tube $args{queue}" : 'stats');
	$args{cb} or return $self->event( error => "no cb for stats at @{[ (caller)[1,2] ]}" );
	$self->{con} or return $args{cb}->(undef, "Not connected");
	$self->{con}->command($cmd, cb => sub {
		defined( local $_ = shift ) or return $args{cb}(undef,@_);
		if (/^OK\s+(\d+)\s*$/) {
			my ($len) = ($1);
			$self->{con}->recv( $len, cb => sub {
				my $data = shift;
				my $stats = eval { $yaml->decode($data) };
				if ($@) {
					$self->event( error => undef, "Failed to decode data: $@", $data );
					$args{cb}->(undef, "Failed to decode data: $@",$data);
				} else {
					#warn "Received stats for ".($q ? $q : '<all>').":". Dumper($deco);
					if ($args{queue}) {
=for rem
      "total-jobs" => 2,
      "current-jobs-delayed" => 0,
      "current-jobs-reserved" => 0,
      "name" => "s-twitter-blog",
      "current-watching" => 0,
      "current-jobs-ready" => 1,
      "current-jobs-urgent" => 0,
      "current-jobs-buried" => 0,
      "current-waiting" => 0,
      "current-using" => 0

=cut
						my %stats = (
							urgent  => $stats->{'current-jobs-urgent'},
							
							ready   => $stats->{'current-jobs-ready'},
							delayed => $stats->{'current-jobs-delayed'},
							buried  => $stats->{'current-jobs-buried'},
							taken   => $stats->{'current-jobs-reserved'},
							total   => $stats->{'total-jobs'},
							active  => 0,
						);
						$stats{active} += $stats{$_} for qw(ready delayed taken);
						#$stats{total} = $stats{active} + $stats{buried};
						$args{cb}->(\%stats);
					} else {
						$args{cb}->($stats);
					}
				}
			});
		} else {
			local $@ = $_;
			$args{cb}->(undef, $_);
		}
	});
}

sub _fullstats {
	my $self = shift;
	@_%2 and confess "Wrong arguments";
	my %args = @_;
	$args{cb} or return $self->event( error => "no cb for queues at @{[ (caller)[1,2] ]}" );
	$self->{con} or return $args{cb}->(undef, "Not connected");
	$self->_queues(cb => sub {
		my $qlist = shift;
		my $cv = AnyEvent->condvar;
		my %stats = (
			queues => $qlist,
		);
		$cv->begin(sub {
			$args{cb}->(\%stats);
		});
		$cv->begin;
		$self->_stats( cb => sub {
			my $stats = shift;
			my %cmd;
			for ( keys %{ $stats } ) {
				next unless s{^cmd-}{};
				$cmd{$_} = $stats->{"cmd-$_"};
			}
			$stats{cmd} = \%cmd;
			$cv->end;
		});
		for my $q ( @$qlist ) {
			$cv->begin;
			$self->_stats(queue => $q, cb => sub {
				$stats{queue}{$q} = shift;

				$cv->end;
			});
		}
		$cv->end;
	});
}

sub _queues {
	my $self = shift;
	@_%2 and confess "Wrong arguments";
	my %args = @_;
	$args{cb} or return $self->event( error => "no cb for queues at @{[ (caller)[1,2] ]}" );
	$self->{con} or return $args{cb}->(undef, "Not connected");
	$self->{con}->command("list-tubes", cb => sub {
		defined( local $_ = shift ) or return $args{cb}(undef,@_);
		if (/^OK\s+(\d+)\s*$/) {
			my ($len) = ($1);
			$self->{con}->recv( $len, cb => sub {
				my $data = shift;
				my $deco = eval { $yaml->decode($data) };
				if ($@) {
					$self->event( error => undef, "Failed to decode data: $@", $data );
					$args{cb}->(undef, "Failed to decode data: $@",$data);
				} else {
					#warn "Received stats for ".($q ? $q : '<all>').":". Dumper($deco);
					$args{cb}->($deco);
				}
			});
		} else {
			local $@ = $_;
			$args{cb}->(undef, $_);
		}
	});
}

# Low level proto implementation

sub __e {
	my $self = shift;
	my $cb = shift;
	if (/OUT_OF_MEMORY/) {
		$self->event( emem => () );
		$cb->(undef, "Server runs out of memory");
	}
	elsif (/INTERNAL_ERROR/) {
		$self->event( eerr => () );
		$cb->(undef, "Server got internal error");
	}
	elsif (/DRAINING/) {
		$self->event( erdonly => () );
		$cb->(undef, "Server doesn't receive more tasks");
	}
	elsif (/BAD_FORMAT/) {
		$self->event( eformat => () );
		$cb->(undef, "Not well-formed message");
	}
	elsif (/UNKNOWN_COMMAND/) {
		$self->event( ecommand => () );
		$cb->(undef, "Unsupported command");
	}
}

sub __use {
	my $self = shift;
	my $dst = shift;
	my $cb = shift;
	return $cb->(1) if $self->{state}{use} eq $dst;
	$self->{con}->command("use $dst", cb => sub {
		defined( local $_ = shift ) or return $cb->(undef,@_);
		if (/USING \Q$dst\E/) {
			$self->{state}{use} = $dst;
			$cb->(1);
		}
		else {
			$self->__e($cb);
		}
	});
}

# put <pri> <delay> <ttr> <bytes>\r\n
# <data>\r\n
sub __put {
	my $self = shift;
	my $cb = pop;
	my ($data,$pri,$delay,$ttr) = @_;
	$pri = DEFPRI unless defined $pri;
	$pri = ( $pri ) % (2**32);
	# warn "put $data,$pri,$delay,$ttr";
	$delay = 0 unless defined $delay;
	$ttr ||= 300;
	utf8::encode $data if utf8::is_utf8 $data;
	my $length = bytes::length($data);
	#warn ">> put $pri $delay $ttr $length  ";

	$self->{con}->command("put $pri $delay $ttr $length$NL$data", cb => sub {
		defined( local $_ = shift ) or return $cb->(undef,@_);
		#warn "<< $_  ";
		if (/INSERTED (\d+)/) {
			my $id = $1;
			$cb->( inserted => $id );
		}
		elsif (/BURIED(?: (\d+)|)/) {
			my $id = $1;
			$cb->( buried => $id );
		}
		elsif(/JOB_TOO_BIG/) {
			$cb->(undef, 'Job size is too big');
		}
		elsif(/EXPECTED_CRLF/) {
			$cb->(undef, 'Newline required after body');
		}
		else {
			#warn ">> put $pri $delay $ttr $length".Dumper("$NL$data")."\n<< $_";
			$self->__e($cb);
		}
	} );
}

sub __reserve {
	my $self = shift;
	my $cb = shift;
	::measure('bt __reserve begin');
	$self->{con}->command("reserve-with-timeout 1", cb => sub {
		defined( local $_ = shift ) or return $cb->(undef,@_);
		if (/RESERVED (\d+) (\d+)/) {
			my ($job,$size) = ($1,$2);
			::measure('bt __reserve ok, reading');
			$self->{con}->recv($size+2, cb => sub { # +2 means with \r\n
				my $data = shift;
				::measure('bt __reserve got data');
				substr($data,$size) = ''; # truncate trailing garbage
				#diag "<+ reserved: job $job, data (".length($_[1])."): $_[1]";
				my $j = {
					id => $job,
					data => $data,
					pri => DEFPRI,
				};
				if ( $self->{state}{watching} == 1 ) {
					($j->{src}) = keys %{ $self->{state}{watch} };
				} 
				$cb->( $self->job($j) );
				undef $cb;
			}); # read_chunk
		}
		elsif (/TIMED_OUT|DEADLINE_SOON/) {
			$cb->(undef,'NO JOBS');
			undef $cb;
		}
		else {
			warn "reserve failed: $_ / @_";
			$cb->(undef, $_);
			undef $cb;
		}
	});
}

sub __delete {
	my $self = shift;
	my $id = shift;
	my $cb = shift;
	$self->{con}->command("delete $id", cb => sub {
		defined( local $_ = shift ) or return $cb->(undef,@_);
		if (/DELETED/) {
			$cb->(1);
		}
		elsif(/NOT_FOUND/) {
			$cb->(0);
		}
		else {
			$self->__e($cb);
		}
		undef $cb;
	});
}

# release <id> <pri> <delay>\r\n

sub __release {
	my $self = shift;
	my $cb = pop;
	my ($id,$pri,$delay) = @_;
	$delay ||= 0;
	$pri ||= DEFPRI;
	$self->{con}->command("release $id $pri $delay", cb => sub {
		defined( local $_ = shift ) or return $cb->(undef,@_);
		if (/^RELEASED\s*$/) {
			$cb->('released');
		}
		elsif (/^BURIED\s*$/) {
			$cb->('buried');
		}
		elsif(/NOT_FOUND/) {
			$cb->(0);
		}
		else {
			$self->__e($cb);
		}
	});
}

# bury <id> <pri>\r\n
sub __bury {
	my $self = shift;
	my $cb = pop;
	my ($id,$pri) = @_;
	$pri ||= DEFPRI;
	$self->{con}->command("bury $id $pri", cb => sub {
		defined( local $_ = shift ) or return $cb->(undef,@_);
		if (/^BURIED\s*$/) {
			$cb->(1);
		}
		elsif(/^NOT_FOUND\s*$/) {
			$cb->(0);
		}
		else {
			$self->__e($cb);
		}
	});
	
}

sub __watch {
	my $self = shift;
	my $dst = shift;
	my $cb = shift;
	return $cb->(1) if $self->{state}{watch}{$dst};
	$self->{con}->command("watch $dst", cb => sub {
		defined( local $_ = shift ) or return $cb->(undef,@_);
		#diag "<< watch $queue: $_";
		if (/WATCHING (\d+)/) {
			$self->{state}{watching} = $1;
			$self->{state}{watch}{$dst}++;
			$cb->($self->{state}{watching});
		} else {
			$cb->(undef,$_);
			return;
		}
	});
}

sub __ping {
	my $self = shift;
	my $cb = shift;
	$self->{con}->command("ping", cb => sub {
		defined( local $_ = shift ) or return $cb->(undef,@_);
		defined or return $cb->(undef,@_);
		#diag "<< ping: $_";
		if (/UNKNOWN_COMMAND/) {
			$cb->('OK');
		} else {
			$cb->(undef,$_);
			return;
		}
	});
}


sub __ignore {
	my $self = shift;
	my $dst = shift;
	my $cb = shift;
	$self->{con}->command("ignore $dst", cb => sub {
		defined( local $_ = shift ) or return $cb->(undef,@_);
		if (/NOT_IGNORED/) {
			$cb->(0) if $cb;
		}
		elsif (/WATCHING (\d+)/) {
			$self->{state}{watching} = $1;
			delete $self->{state}{watch}{$dst};
			$cb->($1) if $cb;
		}
		else {
			$self->__e($cb);
		}
	});
}

sub __peek { croak "Not implemented" }
sub __kick { croak "Not implemented" }

sub __watch_only {
	my $self = shift;
	my $dst = shift;
	my $cb = shift;
	$self->__watch($dst,sub {
		if(shift) {
			if ($self->{state}{watching} > 1) {
				my $cv = AnyEvent->condvar;
				my %res;
				$cv->begin(sub {
					if (%res) {
						$cb->(undef, values %res);
					} else {
						$cb->(1);
					}
				});
				for my $iqueue ( keys %{ $self->{state}{watch} } ) {
					next if $iqueue eq $dst;
					$cv->begin;
					$self->__ignore($iqueue, sub {
						unless ( defined shift ) {
							carp "ignore $iqueue failed: @_";
							$res{$iqueue} = shift;
						}
						$cv->end;
					});
				}
				$cv->end;
			} elsif( $self->{state}{watching} == 1 ) {
				# no need to ignore something
				$cb->(1);
			} else {
				$cb->(undef, "Watch got wrong state");
			}
		} else {
			$cb->(undef,@_)
		}
	});
}

1;

