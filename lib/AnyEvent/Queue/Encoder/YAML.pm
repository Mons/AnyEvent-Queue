package AnyEvent::Queue::Encoder::YAML;

use Carp;
use Encode ();
BEGIN {
	if (0) {}
	elsif ( eval{ require YAML::XS; 0 } ) {
		# TODO: unicode?
		YAML::XS->import(qw(Dump Load));
	}
	elsif( eval{ require YAML::Syck; 1 } ) {
		$YAML::Syck::ImplicitUnicode = 1;
		#$YAML::Syck::ImplicitBinary = 1;
		YAML::Syck->import(qw(Dump Load));
	}
	elsif ( eval{ require YAML; 1 } ) {
		carp "YAML is slow. It's better to install YAML::Syck or YAML::XS";
		YAML->import(qw(Dump Load));
	}
	else {
		croak "Neither YAML::Syck, nor  YAML::XS nor YAML found";
	}
}

sub new { return bless \(do{ my $o = Encode::find_encoding('utf8'); }), shift }
sub format { 'yaml' }
sub encode { my $self = shift; Dump(shift) }
sub decode { my $self = shift; Load(shift) }

1;
