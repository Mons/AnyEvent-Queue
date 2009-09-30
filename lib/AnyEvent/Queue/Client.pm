package AnyEvent::Queue::Client;

=head1 EVENTS

    connected
    error
    disconnect

=cut

use strict;
use MRO::Compat;
use Carp;
use base 'AnyEvent::Connection';

use AnyEvent::Handle;
use AnyEvent::Socket;

use AnyEvent::Queue::Watcher;
use AnyEvent::Queue::Job;
use Sub::Name;

use Scalar::Util qw(weaken);
use Data::Dumper;

use R::Dump;
use AnyEvent::cb;

=head1 INTERFACE

=cut

sub ready          { $_[0]->{connected} && $_[0]->{con}{h} ? 1 : 0 } # is queue connected?

sub ping           { shift->_ping( @_ ) };

sub create         { shift->_create( @_ ) } # register queue
sub drop           { shift->_drop( @_ ) } # drop queue (use with caution)

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

=head1 FEATURES

=cut

use AnyEvent::Queue::Encoder::YAML;
our $YML = AnyEvent::Queue::Encoder::YAML->new;

sub job {
	my $self = shift;
	#warn "New job".Dumper(\@_);
	my $args = shift;
	::measure('new job');
	eval {
		my $data = $self->{encoder}->decode($args->{data});
		#warn "data = $args->{data} ".Dump($data);
		$args->{data} = $data;
	};
	::measure('new job eval 1');
	#warn if $@;
	if (my $e = $@) {
		eval {
			my $data = $YML->decode($args->{data});
			$args->{data} = $data;
			#warn "default encoder $self->{encoder} failed ($e) but YML succeded";
		};
		::measure('new job eval 2');
		#warn if $@;
	}
	::measure('new job eval end');
	$self->job_class->new( $args );
}

sub job_class { shift->{job_class} }

sub new {
	my $pkg = shift;
	my %args = @_ == 1 && ref $_[0] ? %{$_[0]} : @_;
	my %cb;
	for (keys %args) {
		$cb{$_} = delete $args{$_} if UNIVERSAL::isa( $args{$_}, 'CODE' );
	}
	$args{sync} and croak "sync methods was deprecated";
	my $self = $pkg->next::method(
		job_class => 'AnyEvent::Queue::Job',
		encoder   => 'AnyEvent::Queue::Encoder::YAML',
		%args,
	);
	$self->reg_cb(%cb);
	%args = %cb = ();
	$self;
}

sub init {
	my $self = shift;
	$self->next::method();
	$self->{servers} = [$self->{servers}] unless ref $self->{servers};
	$self->{current_server} = 0;

	unless ( $self->job_class->can('new') ) {
		my $file = join '/', split '::', $self->job_class.'.pm';
		require $file;
	}
	
	unless ( ref $self->{encoder} ) {
		my $file = join '/', split '::', $self->{encoder}.'.pm';
		require $file;
		$self->{encoder} = $self->{encoder}->new;
	}
}

sub next_server {
	my $self = shift;
	$self->{current_server} = 0 if $self->{current_server} >= @{ $self->{servers} };
	my ($host,$port) = $self->{servers}[$self->{current_server}++] =~ /([^:]+)(?::(\d+)|)/;
	$self->{host} = $host || 'localhost';
	$self->{port} = $port || 11212;
}

sub connect {
	my ($self,%args) = @_;
	if ($args{cb}) {
		$self->reg_cb( connected => sb {
			shift->unreg_me;
			delete($args{cb})->(@_);
		} );
	}
	$self->next_server();
	carp "Connecting to $self->{host}:$self->{port}" if $self->{debug};
	$self->next::method();
}

sub destroy {
	my $self = shift;
	$self->disconnect;
	for ( keys %{ $self->{watchers} || {} } ) {
		if ($self->{watchers}{$_}) {
			$self->{watchers}{$_}->destroy;
		}
	}
	%$self = ();
	undef $self;
	return;
}

sub disconnect {
	my $self = shift;
	$self->{con} or return;
	$self->{con}->close;
}

sub watcher {
	my $self = shift;
	if (@_ == 1) {
		return unless exists $self->{watchers}{$_[0]};
		return $self->{watchers}{$_[0]};
	}
	my %args = @_;
	my $src = $args{src} || $args{dst} || $args{source};
	my $use_guard = $args{guard} || 0;
	my $impl = $args{impl} ? delete $args{impl} : 'AnyEvent::Queue::Watcher';
	my $file = join '/', split '::', $impl.'.pm';
	require $file unless $impl->can('new');
	
	my $personal = (ref $self)->new(
		(map { $_ => $self->{$_} } qw( servers timeout encoder job_class )),
		reconnect => 1,
		debug => 0,
	);
	my $watcher = $impl->new(
		source => $src,
		client => $personal,
		%args,
	);
	%args = ();
	my $key = $src;
	#my $key = int $watcher;
	if (exists $self->{watchers}{$key}) {
		croak "Already have installed watcher for $key";
	}
	$self->{watchers}{$key} = $watcher;
	undef $personal;
	weaken( my $w = $watcher );
	$watcher->{client}->reg_cb(
		connected => sb {
			#shift->unreg_me;
			$w or return;
			#warn '('.int($w->{client}).") Personal connected @_";
			$w->run;
		},
	);
	$watcher->{client}->connect();
	if (defined wantarray) {
		if ($use_guard) {
			# Alternative way of retval
			undef $watcher;
			return AnyEvent::Util::guard(sub {
				$self->{watchers}{$key}->destroy
					if $self->{watchers}{$key};
				delete $self->{watchers}{$key};
			});
		}
		else {
			weaken( $self->{watchers}{$key} );
			return $watcher;
		}
	} else {
		undef $watcher;
		return;
	}
}
=for rem
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
=cut
1;
