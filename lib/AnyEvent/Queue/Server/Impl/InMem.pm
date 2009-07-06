package AnyEvent::Queue::Server::Impl::InMem;

use strict;
use Queue::PQ;
use Data::Dumper;
use AnyEvent::Queue::Encoder::YAML;
our $encoder;
BEGIN { $encoder = AnyEvent::Queue::Encoder::YAML->new; }


sub register {
	my $pk = shift;
	my $server = shift;
	$server->{engine} ||= Queue::PQ->new();
	
	$server->reg_cb(
		# queue events
		reset => sub {
			my ($srv, $con) = @_;
			$server->{engine}->reset(int $con);
		},
		
		add => sub {
			my ($srv, $con, $type, @args) = @_;
			if( my $id = $server->{engine}->$type(int $con, @args) ) {
				$con->reply("INSERTED $id");
			} else {
				$con->reply($@ // "SHIT HAPPENS");
			}
		},

		take => sub {
			my ($srv, $con, @args) = @_;
			if( my $job = $server->{engine}->take(int $con, @args) ) {
				my $data = $job->{data};
				my $length = bytes::length($data);
				$con->reply("TAKEN $job->{id} $length $job->{pri}\n$data");
			} else {
				$con->reply("NO JOBS");
			}
		},
		
		given => sub { # task given back
			my ($srv, $con, $cmd, @args) = @_;
			if (my $r = $server->{engine}->$cmd(int $con, @args)) {
				$con->reply($r);
			} else {
				$con->reply($@);
			}
		},
		
		taskop => sub {
			my ($srv, $con, $cmd, @args) = @_;
			return $srv->handle($con, peek => @args) if $cmd eq 'peek';
			if (my $r = $server->{engine}->$cmd(int $con, @args)) {
				$con->reply($r);
			} else {
				$con->reply($@);
			}
		},

		queueop => sub {
			my ($srv, $con, $cmd, @args) = @_;
			if (my $r = $server->{engine}->$cmd(int $con, @args)) {
				$con->reply($r);
			} else {
				$con->reply($@);
			}
		},
		
		peek => sub {
			my ($srv, $con, @args) = @_;
			if (my $job = $server->{engine}->peek(int $con, @args)) {
				my $data = $job->{data};
				my $length = bytes::length($data);
				$con->reply("PEEKED $job->{id} $length $job->{pri}\n$data");
			} else {
				$con->reply($@);
			}
		
		},
		
		stats => sub {
=head1 STATS KEYS

	?q prios
	
	urgent
	ready
	taken
	delayed
	buried
	
	+!q cmd
	+!q queues

=cut
			my ($srv, $con, @args) = @_;
			my $stats = $server->{engine}->stats(int $con, @args);
			#use JSON::XS;
			#my $tstats = JSON::XS->new()->utf8(1)->pretty(1)->encode($stats);
			my $tstats = $encoder->encode($stats);
			my $length = bytes::length($tstats);
			$con->reply("STATS $length\n$tstats");
		},
		fullstats => sub {
			my ($srv, $con, @args) = @_;
			my $stats = $server->{engine}->fullstats(int $con, @args);
			#use JSON::XS;
			#my $tstats = JSON::XS->new()->utf8(1)->pretty(1)->encode($stats);
			my $tstats = $encoder->encode($stats);
			my $length = bytes::length($tstats);
			$con->reply("FULLSTATS $length\n$tstats");
		},
	);

	$server;

}
1;
