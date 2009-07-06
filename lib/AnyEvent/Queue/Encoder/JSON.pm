package AnyEvent::Queue::Encoder::JSON;

use JSON::XS;

sub new {
	return bless \(do{ my $o = JSON::XS->new()->utf8(1); }), shift
}

sub format { 'json' }
sub encode { ${ shift() }->encode(shift) }
sub decode { ${ shift() }->decode(shift) }

1;
