package AnyEvent::Queue::Client;

=head1 EVENTS

    connected
    error
    disconnect

=cut

use strict;
use Class::C3;
use Carp;
use base 'Object::Event';

use AnyEvent::Handle;
use AnyEvent::Socket;

use AnyEvent::Queue::Conn;
use AnyEvent::Queue::Watcher;
use AnyEvent::Queue::Job;

use Scalar::Util qw(weaken);
use Data::Dumper;

=head1 INTERFACE

=cut

sub ready          { defined(shift->{con}) ? 1 : 0 } # is queue connected?

sub ping           { shift->_ping( @_ ) };

sub create         { shift->_create( @_ ) } # register queue
sub drop           { shift->_drop( @_ ) } # drop queue (use with caution)

sub create         { shift->any_method( _create  => @_ ) } # register queue
sub drop           { shift->any_method( _drop  => @_ ) } # drop queue (use with caution)

sub put            { shift->_put( @_ ) };  # normal add
sub push:method    { shift->_push( @_ ) };  # out of order add

sub take           { shift->_take( @_ )};
sub requeue        { shift->_requeue ( @_ ) };
sub release        { shift->_release ( @_ ) };
sub ack            { shift->_ack ( @_ ) };
sub bury           { shift->_bury ( @_ ) };

sub peek           { shift->_peek ( @_ ) };
sub delete:method  { shift->_delete ( @_ ) };
sub update         { shift->_update ( @_ ) };

sub stats          { shift->_stats ( @_ ) };
sub fullstats      { shift->_fullstats ( @_ ) };

sub queues         { shift->_queues ( @_ ) };

sub connect:method { shift->_connect( @_ ); }


=head1 FEATURES

=cut

sub cv { AnyEvent->condvar }

use AnyEvent::Queue::Encoder::YAML;
our $YML = AnyEvent::Queue::Encoder::YAML->new;

sub job {
	my $self = shift;
	#warn "New job".Dumper(\@_);
	my $args = shift;
	eval {
		my $data = $self->{encoder}->decode($args->{data});
		$args->{data} = $data;
	};
	if (my $e = $@) {
		eval {
			my $data = $YML->decode($args->{data});
			$args->{data} = $data;
			#warn "default encoder $self->{encoder} failed ($e) but YML succeded";
		};
	}
	$self->job_class->new( $args );
}

sub job_class { shift->{job_class} }

sub new {
	my $pkg = shift;
	my %args = @_ == 1 && ref $_[0] ? %{$_[0]} : @_;
	my %cb;
	for (keys %args) {
		$cb{$_} = delete $args{$_} if ref $args{$_} eq 'CODE';
	}
	my $self = bless {
		job_class => 'AnyEvent::Queue::Job',
		encoder   => 'AnyEvent::Queue::Encoder::YAML',
		current_server => 0,
		%args,
	}, $pkg;
	
	unless ( $self->job_class->can('new') ) {
		my $file = join '/', split '::', $self->job_class.'.pm';
		require $file;
	}
	
	unless ( ref $self->{encoder} ) {
		my $file = join '/', split '::', $self->{encoder}.'.pm';
		require $file;
		$self->{encoder} = $self->{encoder}->new;
	}
	
	$self->reg_cb(%cb);
	
	$self;
}

sub _connect {
	my ($self,%args) = @_;
	
	#my $cv;$cv = AnyEvent->condvar if $self->{sync};
	
	#$self->reg_cb( connected => sub { $cv->send; } ) if $cv;
	$self->reg_cb( connected => $args{cb} ) if exists $args{cb};
	$self->eventcan('connected') or croak "connected not handled";
	$self->{servers} = [$self->{servers}] unless ref $self->{servers};
	$self->{current_server} = 0 if $self->{current_server} >= @{ $self->{servers} };
	my ($host,$port) = $self->{servers}[$self->{current_server}++] =~ /([^:]+)(?::(\d+)|)/;
	$host ||= 'localhost';
	$port ||= 11212;
	carp "Connecting to $host:$port" if $self->{debug};
	my $g;$g = tcp_connect $host, $port, sub {
		undef $g;
		my $fh = shift or do {
			my $e = "$host:$port connect failed: $!";
			$self->event( error => undef, $e);
			#warn $e;
			my @notify;
			push @notify, $args{cb} if exists $args{cb};
			push @notify, $self->{current_cb} if defined $self->{current_cb};
			my %uniq;
			$_->(undef,$e) for grep {!$uniq{$_}++} @notify;
			@notify = ();
			if ($self->{reconnect}) {
				my $t;$t = AnyEvent->timer( after => 0.1, cb => sub {
					undef $t;
					$self->_connect(%args);
				} );
			}
			return;
		};
		my ($h,$p) = @_;
		warn "connected to $h:$p" if $self->{debug};
		$self->{con} = my $con = AnyEvent::Queue::Conn->new(
			fh => $fh,
			debug => defined $self->{debug_proto} ? $self->{debug_proto} : $self->{debug},
		);
		$con->reg_cb(
			error      => sub {
				delete $self->{con};
				defined $self->{current_cb} and  $self->{current_cb}->(undef,@_);
				$self->event( error => @_ );
				$self->_connect(%args) if $self->{reconnect};
			},
			disconnect => sub {
				#warn "got dis from con";
				#$SIG{ALRM} = sub { confess "Fuck" };
				#alarm 2;
				delete $self->{con};
				defined $self->{current_cb} and  $self->{current_cb}->(undef,@_);
				$self->event( disconnect => @_ );
				$self->_connect(%args) if $self->{reconnect};
			},
		);
		$self->event( connected => $con, "$host:$port" );
	}, sub { $self->{timeout} };
	#$cv->recv if $cv;
}

sub disconnect {
	my $self = shift;
	$self->{con} or return;
	$self->{con}->close;
}

sub watcher {
	my $self = shift;
	$self->{sync} and croak "Watcher may not be used in sync mode";
	my %args = @_;
	my $src = $args{src} || $args{dst} || $args{source};
	my $impl = $args{impl} ? delete $args{impl} : 'AnyEvent::Queue::Watcher';
	my $file = join '/', split '::', $impl.'.pm';
	require $file unless $impl->can('new');
	my $watcher = $impl->new(
		source => $src,
		client => $self,
		%args,
	);
	weaken($watcher->{client});
	$self->{watcher}{$src} = $watcher;
	$watcher->run;
	$watcher;
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

#our @WAIT;

sub any_method { # either sync or async
	my $self = shift;
	my $method = shift;
	my $cb;
	my $sync;
	my @args;
	for (@_) {
		if ($_ and !ref and $_ eq 'cb') {
			$cb = 1;
			next;
		} elsif ($cb and !ref $cb) {
			$cb = $_;
			next;
		} elsif ($_ and !ref and $_ eq 'sync') {
			$sync = 1;
			next;
		} elsif ($sync and !ref $sync) {
			$sync = \$_;
			next;
		} else {
			push @args, $_;
		}
	}
	
	$sync = $sync && ref $sync ? $$sync : $self->{sync};

	my $cv;
	$cv = AnyEvent->condvar() if $sync;
	if ($cv) {
		#push @WAIT, $method;
		my $old = $cb;
		$cb = sub {
			local *__ANON__ = "$method:sync";
			$cv->send(@_);
			$old and $old->(@_);
			undef $cv;
			@args = ();
		};
	}
	push @args, cb => $cb if $cb;
	
	local $self->{current_cb} = $cb if $sync;
	
	#warn "Making syncroniuos call to $method" if $cv;

	$cv or $cb or return $self->event( error => undef, "no cb for $method at @{[ (caller)[1,2] ]} [$cb / $cv]" );
	
	if ($sync and $self->{con} and $self->{con}{h}{_in_drain}) {
		warn "Connection handle is in draining. Nested sync calls will lock. Disabling it";
		$self->{con}{h}{_in_drain} = 0;
	}
	$self->$method(@args);

	if ($cv) {
		my @r = $cv->recv;
		#pop @WAIT if $WAIT[-1] eq $method;
		return wantarray ? @r : $r[0] if defined wantarray ;
		return;
	}
}

1;
