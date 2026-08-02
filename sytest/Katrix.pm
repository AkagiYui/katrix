# SyTest homeserver module for Katrix.
#
# Ported from sytest's lib/SyTest/Homeserver/Dendrite.pm, adapted to katrix's
# CLI/config contract:
#   - katrix has no generate-config / generate-keys subcommands. It is
#     config-file (or env) driven, so this module writes a per-instance
#     katrix.yaml directly instead of shelling out to generate-config.
#   - the signing key is auto-created by `katrix serve` on first run, so we do
#     not pre-generate it (Dendrite's generate-keys step is dropped).
#   - federation TLS cert is self-signed via openssl (katrix's `gencert`
#     requires a CA, which sytest does not mount; this mirrors Dendrite's
#     generate-keys self-signed cert).
#   - `katrix serve --config <yaml>` starts the monolith; the client API is
#     served on listen.client (HTTP) and the same handler is also served on
#     listen.federation (HTTPS), so sytest reaches the client API over HTTPS
#     on the secure port, exactly as it does for Dendrite's monolith.

use strict;
use warnings;

use Future;

package SyTest::Homeserver::Katrix::Base;
use base qw( SyTest::Homeserver );
use YAML::XS ();

use Carp;

sub _init
{
   my $self = shift;
   my ( $args ) = @_;

   $self->{$_} = delete $args->{$_} for qw(
       bindir pg_db pg_user pg_pass
   );

   defined $self->{bindir} or croak "Need a bindir";

   $self->{paths} = {};

   $self->SUPER::_init( $args );
}

sub start
{
   my $self = shift;

   my $hs_dir = $self->{hs_dir};

   $self->{paths}{tls_cert}  = "$hs_dir/server.crt";
   $self->{paths}{tls_key}   = "$hs_dir/server.key";
   $self->{paths}{matrix_key} = "$hs_dir/signing.key";

   my $config = $self->_get_config;

   $self->{paths}{config} = $self->write_yaml_file( "katrix.yaml" => $config );

   return $self->_generate_keyfiles;
}

sub _check_db_config
{
   my $self = shift;
   my ( %config ) = @_;

   # katrix only runs against postgres in CI
   return $self->SUPER::_check_db_config( @_ );
}

sub federation_host
{
   my $self = shift;
   return $self->{bind_host};
}

# Build the katrix config hash directly (katrix has no generate-config).
sub _get_config
{
   my $self = shift;

   my %db_config = $self->_get_dbconfig(
      type => 'pg',
      args => {},
   );

   my $db_args = $db_config{args};
   # password may be undef (sytest leaves it blank for local trust auth)
   my $db_uri = sprintf(
      'postgresql://%s:%s@%s/%s?sslmode=%s',
      $db_args->{user} // 'postgres',
      $db_args->{password} // '',
      $db_args->{host} // 'localhost',
      $db_args->{dbname},
      $db_args->{sslmode} // 'disable',
   );

   local $YAML::XS::Boolean = "JSON::PP";

   my $config = {
      server_name     => $self->server_name,
      public_base_url => $self->public_baseurl,

      listen => {
         client     => $self->{bind_host} . ':' . $self->unsecure_port,
         federation => $self->{bind_host} . ':' . $self->secure_port,
      },

      database => {
         dsn => $db_uri,
      },

      signing_key_path => $self->{paths}{matrix_key},

      registration => {
         enabled      => $JSON::true,
         require_token => $JSON::false,
         allow_guest  => $JSON::true,
      },

      media => {
         store_path => $self->{hs_dir} . '/media',
      },

      federation_enabled => $JSON::true,

      # The sytest mock identity server presents a self-signed certificate
      # (keys/tls-selfsigned.crt), so skip TLS verification for identity-server
      # requests (3PID invites).
      identity_server_insecure => $JSON::true,

      federation_tls => {
         cert_path => $self->{paths}{tls_cert},
         key_path  => $self->{paths}{tls_key},
      },
   };

   return $config;
}

# Generate the federation TLS cert/key (self-signed). The signing key is left
# for `katrix serve` to auto-create on first run.
sub _generate_keyfiles
{
   my $self = shift;

   if ( -f $self->{paths}{tls_cert} && -f $self->{paths}{tls_key} ) {
      return Future->done;
   }

   $self->{output}->diag( "Generating self-signed TLS cert for katrix" );

   my $server_name = $self->server_name;
   my @command = (
      'openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
      '-keyout', $self->{paths}{tls_key},
      '-out',    $self->{paths}{tls_cert},
      '-subj',   "/CN=$server_name",
      '-days',   '365',
      '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1',
   );

   return $self->_run_command(
      command => [ @command ],
   )->on_done( sub {
      $self->{output}->diag( "Generated TLS cert for katrix" );
   });
}


package SyTest::Homeserver::Katrix::Monolith;
use base qw( SyTest::Homeserver::Katrix::Base );

use Carp;

sub _init
{
   my $self = shift;
   my ( $args ) = @_;

   $self->SUPER::_init( $args );

   my $idx = $self->{hs_index};
   $self->{ports} = {
      monolith          => main::alloc_port( "monolith[$idx]" ),
      monolith_unsecure => main::alloc_port( "monolith[$idx].unsecure" ),
   };
}

sub server_name
{
   my $self = shift;
   return $self->{bind_host} . ":" . $self->secure_port;
}

sub federation_port
{
   my $self = shift;
   return $self->secure_port;
}

sub secure_port
{
   my $self = shift;
   return $self->{ports}{monolith};
}

sub unsecure_port
{
   my $self = shift;
   return $self->{ports}{monolith_unsecure};
}

sub public_baseurl
{
   my $self = shift;
   return "https://$self->{bind_host}:" . $self->secure_port();
}

sub start
{
   my $self = shift;

   return $self->SUPER::start->then(
      $self->_capture_weakself( '_start_monolith' )
   );
}

# Start the katrix monolith and await the secure (federation/TLS) port.
sub _start_monolith
{
   my $self = shift;

   my $output = $self->{output};
   my $loop = $self->loop;
   my $idx = $self->{hs_index};

   $output->diag( "Starting katrix monolith server" );
   my @command = (
      $self->{bindir} . '/katrix',
      'serve',
      '--config', $self->{paths}{config},
   );

   $output->diag( "Starting katrix with: @command" );

   return $self->_start_process_and_await_connectable(
      command => [ @command ],
      connect_host => $self->{bind_host},
      connect_port => $self->secure_port,
      name => "katrix-$idx",
   )->else( sub {
      die "Unable to start katrix monolith: $_[0]\n";
   })->on_done( sub {
      $output->diag( "Started katrix monolith server" );
   });
}

1;