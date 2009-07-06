package AnyEvent::Queue::Encoder::YAML;

use Carp;
BEGIN {
	if( eval{ require YAML::Syck; 1 } ) {
		YAML::Syck->import(qw(Dump Load));
	}
	elsif ( eval{ require YAML::XS; 1 } ) {
		YAML::XS->import(qw(Dump Load));
	}
	elsif ( eval{ require YAML; 1 } ) {
		carp "YAML is slow. It's better to install YAML::Syck or YAML::XS";
		YAML->import(qw(Dump Load));
	}
	else {
		croak "Neither YAML::Syck, nor  YAML::XS nor YAML found";
	}
}

sub new { return bless \(do{ my $o; }), shift }
sub format { 'yaml' }
sub encode { shift; Dump(shift) }
sub decode { shift; Load(shift) }

1;
