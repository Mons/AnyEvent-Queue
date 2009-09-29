package AnyEvent::Queue::Server::Proto::PMQ;

use strict;

sub register {
	my $pk = shift;
	my $server = shift;
	
	my $debug = $server->{debug} // $server->{debug_proto} // 0;
	
	$server->reg_cb(
		connect => sub {
			my ($srv,$con) = @_;
			#$con->reply("Hello");
		},
		disconnect => sub {
			my ($srv,$con) = @_;
			warn "client disconnected";
			$server->event( reset => $con )
				or warn "reset event not handled";
		},
		error => sub {
			warn "err @_";
		},
		command => sub {
			my ($srv,$con,$cmd) = @_;
			for ( $cmd ) {
				# sample: helo listener:back02
				print STDERR "command $cmd\n" if $debug;
				if ( m{^
						(helo|ehlo)           # command, required
						\s+(\S+)              # name, required
						(?:\s+(\S+)|)         # role, optional (r/w/rw)
						\s*
				$}x ) {
					my ($cmd,$name,$role) = ($1,$2,$3);
					$server->handle( $con,  helo => $name,$role );
				}
				elsif ( m{^ help \s* $}x ) {
					$server->handle( $con,  'help' );
				}
				elsif ( m{^
						(put|push)            # command, required
						\s+(\S+)              # destination, required
						\s+(\d+)              # priority, required
						\s+(\d+)              # bytes, required
						(?:\s+(\d+)           # delay, optional
							(?:\s+(\d+)|)       # id, optional (beware of duplicates)
						|)
						\s*
				$}x ) {
					my ($cmd,$dst,$pri,$bytes,$delay,$id) = ($1,$2,$3,$4,$5,$6);
					$con->recv($bytes, cb => sub {
						my $data = substr($_[0],0,$bytes);
						$server->handle($con, add => $cmd, $dst,$pri,$data,$delay,$id );
					});
				}
				elsif ( m{^
						(?:take)      # command, required
						\s+(\S+)      # destination, required
						(?:\s+(\S+)|) # wait, optional
						\s*
				$}x ) {
					my ($cmd,$dst,$wait) = ('take',$1,$2);
					$server->handle($con, $cmd => $dst,$wait);
				}
				elsif ( m{^
						(ack|release|requeue|bury)  # command, required
						\s+(\S+)       # destination, required
						\s+(\S+)       # id, required
						(?:\s+(\S+)|)  # delay, optional
						\s*
				$}x ) {
					my ($cmd,$dst,$id,$delay) = ($1,$2,$3,$4);
					$server->handle($con, given => $cmd, $dst, $id, $delay );
					
				}
				elsif ( m{^
						(bury|peek|delete)  # command, required
						\s+(\S+)     # destination, required
						\s+(\S+)     # id, required
						\s*
				$}x ) {
					my ($cmd,$dst,$id) = ($1,$2,$3);
					$server->handle($con, taskop => $cmd,$dst,$id );
					
				}
				elsif ( m{^
						(?:update)            # command, required
						\s+(\S+)              # destination, required
						\s+(\d+)              # id, required
						\s+(\d+)              # priority, required
						\s+(\d+)              # bytes, required
						(?:\s+(\d+)|)          # delay, optional
						\s*
				$}x ) {
					my ($dst, $id,$pri,$bytes,$delay) = ($1,$2,$3,$4,$5);
					$con->recv($bytes, cb => sub {
						my $data = substr($_[0],0,$bytes);
						$server->handle($con, taskop => update => $dst,$id,$pri,$data,$delay )
							or warn "taskop.$cmd event not handled";
					});
				}
				elsif ( m{^
						(stats|fullstats) # command, required
						(?:\s+(\S+)|)     # destination, optional
						\s*
				$}x ) {
					my ($cmd,$dst) = ($1,$2);
					$server->handle($con, $cmd => defined $dst ? $dst : () )
						or warn "$cmd event not handled";
				}
				elsif ( m{^
						(create|drop)     # command, required
						\s+(\S+)          # destination, required
						\s*
				$}x ) {
					my ($cmd,$dst) = ($1,$2);
					$server->handle($con, queueop => $cmd => $dst )
						or warn "queueop.$cmd event not handled";
				}
				#elsif ( m{^ (?:debug) $}x ) {
					#warn Dumper \%Q;
					#$con->reply("DEBUG\n".Dumper \%Q);
				#}
				elsif ( m{^(?:put|push|unshift|take|ack|release|requeue|bury|peek|delete|update|create|drop|stats)\b.*}sx) {
					$con->reply("BAD FORMAT");
				}
				else {
					$con->reply("BAD COMMAND");
				}
				#$0 = "$appname: $qsize : $tsize";
			}
		},
		
	);
}

1;
