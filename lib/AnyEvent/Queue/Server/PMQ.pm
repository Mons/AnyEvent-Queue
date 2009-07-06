package AnyEvent::Queue::Server::PMQ;

use strict;
use base 'AnyEvent::Queue::Server';
use AnyEvent::Queue::Server::Proto::PMQ;
use AnyEvent::Queue::Server::Impl::InMem;

sub new {
	my $pk = shift;
	my %args = (
		proto => '::PMQ',
		implementation => '::InMem',
		@_,
	);
	my $self = $pk->next::method(%args);
	$self;
}

1;
