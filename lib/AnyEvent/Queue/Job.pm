package AnyEvent::Queue::Job;

use strict;
use Carp;
our @F;
BEGIN{
	@F = qw(id src pri delay state ttr data);
	no strict 'refs';
	for my $f (@F) {
		*$f = sub { shift->{$f} };
	}
	sub priority { shift->pri }
	sub source   { shift->src }
}

sub new {
	my $self = bless {},shift;
	@_ == 0 or @_ == 1 or croak "Bad arguments to job->new()";
	my $args = @_ ? shift : {};
	exists $args->{$_} and $self->{$_} = delete $args->{$_} for @F;
	%$args and carp "unknown fields for $self: [@{[ keys %$args ]}]";
	$self;
}

1;
