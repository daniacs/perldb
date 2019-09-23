# Conexao com PostgreSQL.
#
# conexaoPG({
#   base=>'BDNAME', 
#   usuario=>'USERNAME',
#   senha=>'PASSWORD',
#   host=>'SERVIDOR',
#   porta=>'PORTA'})

package Postgres;
use DBD::Pg;

# Tipos especificos do PostgreSQL
sub dataType {
  {
    0  =>  "POSTGRES TYPE",  # oid, money
    1  =>  "CHAR",           # char
    3  =>  "NUMERIC",        # Com ou sem tamanho especificado
    4  =>  "INTEGER",        # int4
    12 =>  "VARCHAR",
    11 =>  "TIMESTAMP WITHOUT TIME ZONE",
    91 =>  "DATE",
    92 =>  "TIME WITHOUT TIME ZONE",
    -1 =>  "TEXT",
    -3 =>  "BYTEA",          # similar ao blob
    -5 =>  "BIGINT"          #int8
  }->{shift()};
};

sub conexaoPG {
  my $parametros  = shift;
  my $server = $parametros->{'host'};
  my $base  = $parametros->{'base'};
  my $user  = $parametros->{'usuario'};
  my $password  = $parametros->{'senha'};
  my $port  = defined($parametros->{'porta'}) ? $parametros->{'porta'} : 5432;
  my $dbh_pg = DBI->connect( "dbi:Pg:dbname=$base;host=$server",
    $user, $password ) || die( $DBI::errstr . "\n" );

  $dbh_pg->{AutoCommit}    = 0;
  $dbh_pg->{RaiseError}    = 1;
  $dbh_pg->{PrintError}    = 1;
  $dbh_pg->{pg_enable_utf8} = 0; # Se default ou 1, retorna ISO8859-1!
  
  # Encoding
  my $sql = "SELECT name,setting 
    FROM pg_catalog.pg_settings 
    WHERE name IN ('server_encoding', 'client_encoding')";
  my $temp_sth = $dbh_pg->prepare($sql);
  my $res = $temp_sth->execute();
  $res = $temp_sth->fetchall_hashref('name');
  $parametros->{client_encoding} = $res->{client_encoding}->{setting};
  $parametros->{server_encoding} = $res->{server_encoding}->{setting};
  return $dbh_pg;
};

1;
