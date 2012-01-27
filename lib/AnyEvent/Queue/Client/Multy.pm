package AnyEvent::Queue::Client::Multy;

use strict;
use Carp;
use Sys::Hostname 'hostname';
our $hostname = hostname();

sub new {
	my $pkg = shift;
	my $class = shift;
	my $self = bless {}, $pkg;
	my %args = @_;
	my @servers = @{ delete $args{servers} };
	$self->{queue} = {
		ready => [],
		pool => {},
	};
	my $cb = delete $args{cb};
	for (@servers) {
		my ($host,$port) = split ':',$_;
		$host = 'localhost' if $host eq $hostname;
		$host .= ':'.$port;
		
		my $c = $class->new(
			%args,
			server          => $host,
			servers         => [$host],
		);
		$self->{queue}{pool}{$host} = $c;
		$c->reg_cb(
			connected => sub {
				my ($c,$con,$host) = @_;
				warn("out connected $host ($c->{server})");
				push @{ $self->{queue}{ready} }, $c;
				exists $cb->{connected} and $cb->{connected}( @_ );
				if ($self->{watchers}) { $self->rewatch }
				return;
			},
			disconnected => sub {
				my $c = shift;
				warn("out disconnected @_");
				
				@{ $self->{queue}{ready} } = grep {
					$_ != $c;
				} @{ $self->{queue}{ready} };
				exists $cb->{disconnected} and $cb->{disconnected}( @_ );
				if ($self->{watchers}) { $self->rewatch }
				return;
			},
		);
	}
	return $self;
}

sub connect {
	my $self = shift;
	for my $c ( values %{ $self->{queue}{pool} } ){
		$c->connect;
	}
}

sub active {
	my $self = shift;
	return 0+@{ $self->{queue}{ready} };
}

sub watchers {
	my $self = shift;
	my %args = @_;
	my $src = $args{src} || 'default';
	if (exists $self->{watchers}) {
		croak("already watched");
	}
	$self->{watchers} = \%args;
	$self->rewatch;
}

sub rewatch {
	my $self = shift;
	my %active;
	for my $c ( @{ $self->{queue}{ready} } ){
		$active{ $c->{server} }++;
		if (exists $self->{watched}{ $c->{server} }) { next; }
		$self->{watched}{ $c->{server} } = $c->watch( %{ $self->{watchers} } );
	}
	for my $c ( values %{ $self->{queue}{pool} } ) {
		next if $active{ $c->{server} };
		if (exists $self->{watched}{ $c->{server} }) {
			warn "throw out stalled watcher for $c->{server}";
			delete $self->{watched}{ $c->{server} };
		}
	}
}

sub put {
	my $self = shift;
	my %args = @_;
	my ($out,$servers);
	if (exists $args{servers}) {
		$servers = delete $args{servers};
		$out = shift @$servers;
	} else {
		my @servers = @{ $self->{queue}{ready} };
		if (!@servers) {
			return $args{cb}(undef, "Not connected to any server");
		}
		push @{ $self->{queue}{ready} },shift @{ $self->{queue}{ready} };
		$out = shift @servers;
		$servers = \@servers;
	}
	$out or return $args{cb}(undef, "No active output queue");
	warn "Selected $out->{server} for put and @$servers in reserve\n";
	my $cb = delete $args{cb};
	$out->put(%args, cb => sub {
		if (my $job = shift) {
			$cb->($job);
		} else {
			if (@$servers) {
				return $self->put( %args, cb => $cb, servers => $servers );
			} else {
				$cb->(undef,@_);
			}
		}
	});
	return;
}

1;
