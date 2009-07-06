package AnyEvent::Queue::Encoder::Storable;

use Storable;

sub format { 'storable' }
sub new { return bless \(do {my $o}), shift }
sub encode { shift; Storable::nfreeze(shift) }
sub decode { shift; Storable::thaw(shift) }

1;
