package AnyEvent::Queue::Client::PMQ;

use strict;
use Carp;
use Data::Dumper;
use base 'AnyEvent::Queue::Client';

use constant DEFPRI => 16;
use AnyEvent::Queue::Encoder::JSON;
our $json;
BEGIN { $json = AnyEvent::Queue::Encoder::JSON->new; }

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

sub new {
	my $pk = shift;
	my $self = $pk->next::method( encoder => 'AnyEvent::Queue::Encoder::YAML', @_ );
	
	$self;
}

sub _put  { shift->_add('put', @_) }
sub _push : method { shift->_add('push',@_) }

sub _add {
	my $self = shift;
	my $cmd  = shift;
	
	my %args = @_;
	my $data = $args{data} or croak "No data for $cmd";
	$data = $self->{encoder}->encode($data) if ref $data;
	
	$args{cb} or return $self->event( error => "no cb for put at @{[ (caller)[1,2] ]}" );
	my $dst = $args{dst} || 'default';
	$dst =~ s/\s+/_/sg;
	my $pri = ( $args{pri} || 16 ) % 256;
	utf8::encode $data if utf8::is_utf8 $data;
	my $length = length($data);
	my $delay = 0;
	my $id = exists $args{id} ? $args{id} : '';
	$self->{con} or return $args{cb}->(undef, "Not connected");
	$self->{con}->command("$cmd $dst $pri $length $delay $id\r\n$data", cb => sub {
		local $_ = shift;
		if (/^INSERTED\s+(\d+)$/) {
			my $job = $self->job({
				id  => $1,
				pri => $pri,
				src => $dst,
				data => $data,
			});
			carp "Id mismatch: requested: $id, got: $job->{id}" if length $id and $id != $job->{id};
			
			$args{cb}->($job);
		} else {
			local $@ = $_;
			$args{cb}->(undef, $_);
		}
	});
}

sub _recv_job {
	my $self = shift;
	my $command = shift;
	my $src = shift;
	my %args = @_;

	$args{cb} or return $self->event( error => "no cb for take at @{[ (caller)[1,2] ]}" );
	$self->{con} or return $args{cb}->(undef, "Not connected");
	$self->{con}->command($command, cb => sub {
		local $_ = shift;
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
			local $@ = $_;
			$args{cb}->(undef, $_);
		}
	});
}

sub _take {
	my $self = shift;
	my %args = @_;
	my $src = $args{src} || 'default';
	$src =~ s/\s+/_/sg;
	$self->_recv_job("take $src", $src, @_);
}

sub _peek {
	my $self = shift;
	my $src = shift;
	my $id = shift;
	$src =~ s/\s+/_/sg;
	$self->_recv_job("peek $src $id", $src, @_);
}

sub _give_back {
	my $self = shift;
	my $cmd = shift;
	my %args = @_;
	
	$args{cb} or return $self->event( error => "no cb for $cmd at @{[ (caller)[1,2] ]}" );

	my ($id,$dst,$pri,$delay);
	if ($args{job}) {
		$id  = $args{job}{id};
		$dst = $args{job}{src} || $args{job}{dst};
		$pri = $args{job}{pri};
		$delay = $args{job}{delay};
	} else {
		$id  = $args{id};
		$dst = $args{dst} || $args{src} || 'default';
		$pri = $args{pri} || DEFPRI;
	}

	$id or confess "No id for $cmd";
	$delay = $args{delay} if exists $args{delay}; # argument have priority over job

	$self->{con} or return $args{cb}->(undef, "Not connected");
	$self->{con}->command("$cmd $dst $id $delay", cb => sub {
		local $_ = shift;
		if (/^OK\s*$/) {
			$args{cb}->(1);
		} else {
			local $@ = $_;
			$args{cb}->(undef, $_);
		}
	});
	
}

sub _requeue { shift->_give_back('requeue', @_) }
sub _release { shift->_give_back('release', @_) }
sub _ack     { shift->_give_back('ack',     @_) }
sub _bury    { shift->_give_back('bury',    @_) }

sub _delete  { shift->_give_back('delete',   @_) }

sub _stats {
	my $self = shift;
	@_%2 and confess "Wrong arguments";
	my %args = @_;
	my $q = exists $args{queue} ? $args{queue} : exists $args{src} ? $args{src} : exists $args{dst} ? $args{dst} : undef;
	my $cmd = "stats".(defined $q ? " $q" : '');
	$args{cb} or return $self->event( error => "no cb for stats at @{[ (caller)[1,2] ]}" );
	$self->{con} or return $args{cb}->(undef, "Not connected");
	$self->{con}->command($cmd, cb => sub {
		local $_ = shift;
		if (/^STATS\s+(\d+)\s*$/) {
			my ($len) = ($1);
			$self->{con}->recv( $len, cb => sub {
				my $data = shift;
				#my $deco = eval { $json->decode($data) };
				my $deco = eval { $self->{encoder}->decode($data) };
				if ($@) {
					$self->event( error => undef, "Failed to decode data: $@", $data );
					$args{cb}->(undef, "Failed to decode data: $@",$data);
				} else {
					#warn "Received stats for ".($q ? $q : '<all>').":". Dumper($deco);
					if (!defined $deco->{total}) {
						$deco->{total} = 0;
						$deco->{total} += $deco->{$_} for qw( active buried );
					}
					$args{cb}->($deco);
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
	my $cmd = "fullstats";
	$args{cb} or return $self->event( error => "no cb for $cmd at @{[ (caller)[1,2] ]}" );
	$self->{con} or return $args{cb}->(undef, "Not connected");
	$self->{con}->command($cmd, cb => sub {
		local $_ = shift;
		if (/^FULLSTATS\s+(\d+)\s*$/) {
			my ($len) = ($1);
			$self->{con}->recv( $len, cb => sub {
				my $data = shift;
				#my $deco = eval { $json->decode($data) };
				my $deco = eval { $self->{encoder}->decode($data) };
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

sub _update {
	my $self = shift;
	@_%2 and confess "Wrong arguments";
	my %args = @_;
	
	$args{cb} or return $self->event( error => "no cb for update at @{[ (caller)[1,2] ]}" );

	my ($id,$dst,$pri,$delay);
	if ($args{job}) {
		$id  = $args{job}{id};
		$dst = $args{job}{src} || $args{job}{dst};
		$pri = $args{job}{pri};
		$delay = $args{job}{delay};
	} else {
		$id  = $args{id};
		$dst = $args{dst} || $args{src} || 'default';
		$pri = $args{pri} || DEFPRI;
	}

	defined $id or confess "No id for update";
	exists $args{data} or croak "No data for update";

	$delay = $args{delay} if exists $args{delay}; # argument have priority over job

	my $data = $args{data};
	$data = $self->{encoder}->encode($data) if ref $data;

	utf8::encode $data if utf8::is_utf8 $data;
	my $length = length($data);

	$self->{con} or return $args{cb}->(undef, "Not connected");
	$self->{con}->command("update $dst $id $pri $length $delay\r\n$data", cb => sub {
		local $_ = shift;
		if (/^OK\s*$/) {
			$args{cb}->(1);
		} else {
			local $@ = $_;
			$args{cb}->(undef, $_);
		}
	});
	
}

sub _create {
	my $self = shift;
	@_%2 and confess "Wrong arguments";
	my %args = @_;
	my $dst = exists $args{queue} ? $args{queue} : exists $args{src} ? $args{src} : exists $args{dst} ? $args{dst} : undef;
	defined $dst or confess "Required queue argument";
	$self->{con} or return $args{cb}->(undef, "Not connected");
	$self->{con}->command("create $dst", cb => sub {
		local $_ = shift;
		if (/^OK\s+(\S+)\s*$/) {
			$args{cb}->(1,lc $1);
		} else {
			local $@ = $_;
			$args{cb}->(undef, $_);
		}
	});
}

sub _drop {
	my $self = shift;
	@_%2 and confess "Wrong arguments";
	my %args = @_;
	my $dst = exists $args{queue} ? $args{queue} : exists $args{src} ? $args{src} : exists $args{dst} ? $args{dst} : undef;
	defined $dst or confess "Required queue argument";
	$self->{con} or return $args{cb}->(undef, "Not connected");
	$self->{con}->command("drop $dst", cb => sub {
		local $_ = shift;
		if (/^OK\s+(\d+)\s*$/) {
			my $count = $1 || '0E0';
			$args{cb}->($count);
		} else {
			local $@ = $_;
			$args{cb}->(undef, $_);
		}
	});
}

sub _queues {
	my $self = shift;
	my %args = @_;
	$args{cb} or return $self->event( error => "no cb for queues at @{[ (caller)[1,2] ]}" );
	
	$self->_stats(cb => sub {
		my $stats = shift or return $args{cb}->(undef,@_);
		$args{cb}->($stats->{queues});
	});
}

1;

