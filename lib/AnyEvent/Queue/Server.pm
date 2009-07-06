package AnyEvent::Queue::Server;

use strict;
use Class::C3;
use base 'Object::Event';
use AnyEvent::Handle;
use AnyEvent::Socket;

use AnyEvent::Queue::Conn;

use Data::Dumper;
use Carp;

sub root_package { __PACKAGE__ }

sub new {
	my $pkg = shift;
	my $self = bless { @_ }, $pkg;
	$self->set_exception_cb( sub {
		my ($e, $event, @args) = @_;
		my $con;
		{
			local $::self = $self;
			local $::con;
			local $::event = $event;
			{
				package DB;
				my $i = 0;
				while (my @c = caller(++$i)) {
					#warn "$i. [@DB::args]";
					next if @DB::args < 1;
					last if $DB::args[0] == $::self and $DB::args[1] eq $::event;
				}
				$::con = $DB::args[2];
			}
			$con = $::con;
		}
		if ($con) {
			my $msg = "INTERNAL ERROR";
			if ($self->{devel}) {
				$e =~ s{(?:\r?\n)+}{ }sg;
				$e =~ s{\s+$}{}s;
				$msg .= ": ".$e;
			}
			$con->reply($msg);
		}
		warn "exception during $event : $e";
	} );
	
	if ($self->{proto}) {
		my $proto = substr($self->{proto},0,2) eq '::' ? $self->root_package.'::Proto'.$self->{proto} : $self->{proto};
		eval qq{ use $proto ;1} or croak $@;
		$proto->register( $self );
	}
	
	if ($self->{implementation}) {
		my $impl = substr($self->{implementation},0,2) eq '::' ? $self->root_package.'::Impl'.$self->{implementation} : $self->{implementation};
		eval qq{ use $impl ;1} or croak $@;
		$impl->register( $self );
	}

	$self;
}

sub start {
	my $self = shift;
	$self->eventcan('command') or croak "Server implementation $self doesn't parses commands";
	$self->{engine} or croak "Server implementation $self doesn't have queue engine";
	tcp_server undef, $self->{port}, sub {
		my $fh = shift;
		unless ($fh) {
			$self->event( error => "couldn't accept client: $!" );
			return;
		}
		$self->accept_connection($fh);
	};
	print "Server started on port $self->{port}\n";
}

sub accept_connection {
	my ($self,$fh,@args) = @_;
	print "Client connected @args\n";
	my $con = AnyEvent::Queue::Conn->new( fh => $fh );
	$self->{c}{int $con} = $con;
	$con->reg_cb(
		disconnect => sub {
			delete $self->{c}{int $_[0]};
			$self->event( disconnect => $_[0], $_[1] );
		},
		command => sub {
			$self->event( command => @_ )
		},
	);
	$self->event( connect => $con );
	$con->want_command;
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
