package AnyEvent::Queue::Conn;

use strict;
use base 'Object::Event';
use AnyEvent::Handle;
use Scalar::Util qw(weaken);
use Data::Dumper;

our $NL = "\015\012";
our $QRNL = qr<\015?\012>;

=head2 EVENTS
	disconnect
	error

=head2 METHODS

=over 4

=cut

sub new {
	my $pkg = shift;
	my $self = bless { @_ }, $pkg;
	$self->{h} = AnyEvent::Handle->new(
		fh => $self->{fh},
		autocork => 1,
		on_eof => sub {
			local *__ANON__ = 'conn.on_eof';
			warn "[\U$self->{side}\E] Eof on handle";
			delete $self->{h};
			for my $k (keys %{ $self->{waitingcb} }) {
				if ($self->{waitingcb}{$k}) {
					$self->{waitingcb}{$k}->(undef, "eof from client");
				}
				delete $self->{waitingcb}{$k};
			}
			$self->event('disconnect');
		},
		on_error => sub {
			local *__ANON__ = 'conn.on_error';
			my $e = "$!";
			if ( $self->{destroying} ) {
				$e = "Connection closed";
			} else {
				#warn "[\U$self->{side}\E] Error on handle: $e"; # TODO: uncomment
			}
			delete $self->{h};
			for my $k (keys %{ $self->{waitingcb} }) {
				if ($self->{waitingcb}{$k}) {
					$self->{waitingcb}{$k}->(undef, "$e");
				}
				delete $self->{waitingcb}{$k};
			}
			$self->event( disconnect => "Error: $e" );
		},
	);
	$self;
}

sub close {
	my $self = shift;
	undef $self->{fh};
	$self->{h} and $self->{h}->destroy;
	undef $self->{h};
	$self->{destroying} = 1;
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
	weaken( $self->{waitingcb}{int $args{cb}} = $args{cb} );
	
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
		delete $self->{waitingcb}{int $args{cb}};
		delete($args{cb})->(@_);
		%args = ();
		undef $self;
	} );
	#sub {
		#$self->{state}{handle}->timeout( 0 ) if $self->_qsize < 1;
		#diag "<< $c. $write: $_[1] (".$self->_qsize."), timeout ".($self->{state}{handle}->timeout ? 'enabled' : 'disabled');
		#$cb->(@_);
	#});
}

sub commandx {
	my ($debug,%args);
	$args{cf} = sub {
		local *__ANON__ = 'conn.command.read';
		shift;
		for (@_) {
			chomp;
			substr($_,-1,1) = '' if substr($_, -1,1) eq "\015";
		}
		warn "<< @_  " if $debug;
		#delete $self->{waitingcb}{int $args{cb}};
		$args{cb}(@_);
		%args = ();
	};
	{
		my $self = shift;
		my $write = shift;
		%args = (@_, cf => $args{cf});
		$args{write} = $write;
		$args{cb} or return %args = (),$self->event( error => "no cb for command at @{[ (caller)[1,2] ]}" );
		$self->{h} or return $args{cb}(undef,"Not connected"),%args=();
		weaken($self->{waitingcb}{int $args{cb}} = $args{cb});
		$debug = $self->{debug};
		warn ">> $write  " if $self->{debug};
		$self->{h}->push_write("$args{write}$NL");
		warn "<? read  " if $debug;
		$self->{h}->push_read( regex => $QRNL, $args{cf} );
	}
	#$self->{h}->timeout( $self->{select_timeout} );
	#sub {
		#$self->{state}{handle}->timeout( 0 ) if $self->_qsize < 1;
		#diag "<< $c. $write: $_[1] (".$self->_qsize."), timeout ".($self->{state}{handle}->timeout ? 'enabled' : 'disabled');
		#$cb->(@_);
	#});
}

sub want_command {
	my $self = shift;
	$self->{h} or return warn "[\U$self->{side}\E] Not connected";
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
	$self->{waitingcb}{int $args{cb}} = $args{cb};
	$self->{h}->unshift_read( chunk => $bytes, sub {
		local *__ANON__ = 'conn.recv.read';
		# Also eat CRLF or LF from read buffer
		substr( $self->{h}{rbuf}, 0, 1 ) = '' if substr( $self->{h}{rbuf}, 0, 1 ) eq "\015";
		substr( $self->{h}{rbuf}, 0, 1 ) = '' if substr( $self->{h}{rbuf}, 0, 1 ) eq "\012";
		delete $self->{waitingcb}{int $args{cb}};
		shift; $args{cb}->(@_);
	} );
}

sub reply {
	my $self = shift;
	$self->{h} or return warn "[\U$self->{side}\E] Not connected";
	$self->{h}->push_write("@_$NL");
	warn ">> @_  " if $self->{debug};
}

sub DESTROY {
	my $self = shift;
	warn "(".int($self).") Destroying conn";
	$self->close;
	#$self->destroy;
}

=back

=cut

1;
