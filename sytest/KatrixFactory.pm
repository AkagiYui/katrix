# SyTest homeserver factory for Katrix.
#
# Ported from sytest's lib/SyTest/HomeserverFactory/Dendrite.pm. run-tests.pl
# selects an implementation via `-I <name>`, which it matches against the
# `->name()` of every class under SyTest::HomeserverFactory:: (the base
# `name()` strips that prefix, so this package registers as "Katrix::Monolith").
# The factory then instantiates the matching SyTest::Homeserver::Katrix::*
# class. Without this layer run-tests.pl reports
# "Unrecognised server implementation Katrix::Monolith".

use strict;
use warnings;

require SyTest::Homeserver::Katrix;

package SyTest::HomeserverFactory::Katrix;
use base qw( SyTest::HomeserverFactory );

sub _init
{
   my $self = shift;

   $self->{args} = {
      bindir => "/tmp/bin",
   };

   $self->SUPER::_init( @_ );
}

sub implementation_name
{
   return "katrix";
}

sub get_options
{
   my $self = shift;

   return (
      'd|katrix-binary-directory=s' => \$self->{args}{bindir},
      $self->SUPER::get_options(),
   );
}

sub print_usage
{
   print STDERR <<EOF
   -d, --katrix-binary-directory DIR  - path to the directory containing the
                                        katrix binary
EOF
}

sub create_server
{
   die 'polylith Katrix not yet implemented';
}

package SyTest::HomeserverFactory::Katrix::Monolith;
use base qw( SyTest::HomeserverFactory::Katrix );

sub _init
{
   my $self = shift;
   $self->{impl} = "SyTest::Homeserver::Katrix::Monolith";

   $self->SUPER::_init( @_ );
}

sub create_server
{
   my $self = shift;
   my %params = ( @_, %{ $self->{args}} );

   return $self->{impl}->new( %params );
}

1;