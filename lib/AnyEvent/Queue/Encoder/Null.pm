package AnyEvent::Queue::Encoder::Null;

sub new { return bless \(do {my $o}), shift }
sub format { 'raw' }
sub encode { shift; shift }
sub decode { shift; shift }

1;
