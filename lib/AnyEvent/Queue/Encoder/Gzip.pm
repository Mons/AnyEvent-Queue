package AnyEvent::Queue::Encoder::Gzip;

use Storable;
use Compress::Zlib;

sub new {
	return bless \(do{ my $o; }), shift
}

sub format { 'gzip' }
sub encode { shift; Compress::Zlib::memGzip(Storable::nfreeze(shift)) }
sub decode { shift; Storable::thaw(Compress::Zlib::memGunzip(shift)) }

1;
