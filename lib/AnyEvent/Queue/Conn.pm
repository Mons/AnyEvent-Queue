package AnyEvent::Queue::Conn;

use strict;
use base 'Object::Event';
use AnyEvent::Handle;
use Data::Dumper;

our $NL = "\015\012";
our $QRNL = qr<\015?\012>;

=head2 EVENTS
	disconnect
	error
=cut

sub new {
	my $pkg = shift;
	my $self = bless { @_ }, $pkg;
	$self->{h} = AnyEvent::Handle->new(
		fh => $self->{fh},
		on_eof => sub {
			local *__ANON__ = 'conn.on_eof';
			warn "eof on handle";
			delete $self->{h};
			$self->event('disconnect');
		},
		on_error => sub {
			local *__ANON__ = 'conn.on_error';
			warn "error on handle: $!";
			delete $self->{h};
			$self->event( disconnect => "Error: $!" );
		},
	);
	$self;
}

sub close {
	my $self = shift;
	undef $self->{fh};
	$self->{h}->destroy;
	undef $self->{h};
	return;
}

=item command CMD, cb => CB

	sends command ARG to peer, receive one line response and invoke CB

=cut

sub command {
	my $self = shift;
	my $write = shift;
	my %args = @_;
	$args{cb} or return $self->event( error => "no cb for command at @{[ (caller)[1,2] ]}" );
	$self->{h} or return $args{cb}->(undef,"Not connected");
	#my $i if 0;
	#my $c = ++$i;
	warn ">> $write  " if $self->{debug};
	$self->{h}->push_write("$write$NL");
	#$self->{h}->timeout( $self->{select_timeout} );
	warn "<? read  " if $self->{debug};
	$self->{h}->push_read( regex => $QRNL, sub {
		local *__ANON__ = 'conn.command.read';
		shift;
		for (@_) {
			chomp;
			substr($_,-1,1) = '' if substr($_, -1,1) eq "\015";
		}
		warn "<< @_  " if $self->{debug};
		$args{cb}->(@_)
	} );
	#sub {
		#$self->{state}{handle}->timeout( 0 ) if $self->_qsize < 1;
		#diag "<< $c. $write: $_[1] (".$self->_qsize."), timeout ".($self->{state}{handle}->timeout ? 'enabled' : 'disabled');
		#$cb->(@_);
	#});
}

sub want_command {
	my $self = shift;
	$self->{h} or return warn "Not connected";
	$self->{h}->push_read( regex => $QRNL, sub {
		local *__ANON__ = 'conn.want_command.read';
		shift;
		for (@_) {
			chomp;
			substr($_,-1,1) = '' if substr($_, -1,1) eq "\015";
		}
		$self->event(command => @_);
		$self->want_command;
	});
}

sub recv {
	my ($self,$bytes,%args) = @_;
	$args{cb} or return $self->event( error => "no cb for command at @{[ (caller)[1,2] ]}" );
	$self->{h} or return $args{cb}->(undef,"Not connected");
	warn "<+ read $bytes " if $self->{debug};
	$self->{h}->unshift_read( chunk => $bytes, sub {
		local *__ANON__ = 'conn.recv.read';
		# Also eat CRLF or LF from read buffer
		substr( $self->{h}{rbuf}, 0, 1 ) = '' if substr( $self->{h}{rbuf}, 0, 1 ) eq "\015";
		substr( $self->{h}{rbuf}, 0, 1 ) = '' if substr( $self->{h}{rbuf}, 0, 1 ) eq "\012";
		shift; $args{cb}->(@_);
	} );
}

sub reply {
	my $self = shift;
	$self->{h} or return warn "Not connected";
	$self->{h}->push_write("@_$NL");
	warn ">> @_  " if $self->{debug};
}

1;
