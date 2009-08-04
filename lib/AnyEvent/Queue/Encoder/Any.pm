package AnyEvent::Queue::Encoder::Any;

use Storable;
use Compress::Zlib;
use AnyEvent::Queue::Encoder::YAML;
use AnyEvent::Queue::Encoder::JSON;
use AnyEvent::Queue::Encoder::Storable;

sub new {
	my $pkg = shift;
	return bless \(do{ my $o = [
		AnyEvent::Queue::Encoder::Storable->new(),
		AnyEvent::Queue::Encoder::JSON->new(),
		AnyEvent::Queue::Encoder::YAML->new(),
	]; }), $pkg;
}

sub format { 'gzip' }

sub encode {
	my $self = shift;
	Compress::Zlib::memGzip( ${$$self}[0]->encode(shift) )
}

sub decode {
	my $self = shift;
	my $data = shift;
	my $u;$u = 1,
		utf8::encode($data) if utf8::is_utf8($data);
	$data = Compress::Zlib::memGunzip($data)
		// do {
			#warn "not gzip";
			$data
		};
	my $dec;
	$u and utf8::decode($data);
	for (@{ $$self }) {
		$dec = eval{ $_->decode($data) };
		defined $dec and last;
		#warn $@ if $@;
	}
	$dec // $data;
}

1;
