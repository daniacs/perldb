# Conexao com Oracle. Pode ser feita com ou sem o uso do TSNAMES.ORA
#
# Sem TNSNAMES: conexaoOracle({
#   usuario=>'USERNAME',
#   senha=>'PASSWORD',
#   host=>'SERVIDOR',
#   base=>'BDNAME', 
#   porta=>'PORTA'})
#
# Com TNSNAMES: conexaoOracle({
#   base=>'BDNAME', 
#   usuario=>'USERNAME',
#   senha=>'PASSWORD'})

package Oracle;
use DBD::Oracle;

# Tipos especificos do Oracle
sub dataType {
  {
    1  =>  "CHAR",
    3  =>  "NUMBER",         # Tamanho especificado, sem decimal
    8  =>  "NUMBER",         # Tamanho nao especificado
    12 =>  "VARCHAR2",
    30 =>  "BLOB",
    40 =>  "CLOB",
    93 =>  "DATE/TIMESTAMP", # O tipo timestamp nao tem SQL_DATA_TYPE
    -1 =>  "LONG",
    -3 =>  "RAW",
    -4 =>  "LONG RAW",
    -9104  =>  "ORACLE ROWID",
    undef => "ORACLE TYPE"
  }->{shift()};
};

sub conexaoOracle {
  my $parametros  = shift;
  my $base  = $parametros->{'base'};
  my $user  = $parametros->{'usuario'};
  my $password  = $parametros->{'senha'};
  my $host  = $parametros->{'host'};
  my $port  = defined($parametros->{'porta'}) ? $parametros->{'porta'} : 1521;
  my $dbh_oracle = undef;

  # Define o cliente Oracle como sendo UTF-8
  #$ENV{'NLS_LANG'}  = 'AMERICAN_AMERICA.AL32UTF8';
  $ENV{'NLS_LANG'}  = 'BRAZILIAN PORTUGUESE_BRAZIL.AL32UTF8';
  #$ENV{'NLS_NCHAR'} = 'AL32UTF8';

  # Conexoes feitas sem o tnsnames.ora
  if ((defined($host)) && (defined($base))) {
    #$dbh_oracle = DBI->connect( "dbi:Oracle:host=$host;SID=$sid;port=$port", "$user/$password")
    $dbh_oracle = DBI->connect("dbi:Oracle://$host:$port/$base", "$user", "$password")
      or die( $DBI::errstr . "\n" );
  }
  else {
    $dbh_oracle = DBI->connect("dbi:Oracle:$base", $user, $password )
      or die( $DBI::errstr . "\n" );
  }

  $dbh_oracle->{AutoCommit}    = 0;
  $dbh_oracle->{RaiseError}    = 1;
  $dbh_oracle->{PrintError}    = 1;
  $dbh_oracle->{ora_check_sql} = 0;
  #$dbh_oracle->{RowCacheSize}  = 16;
  $dbh_oracle->do("alter session set NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'");
  $dbh_oracle->do("alter session set NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF6'");
  $dbh_oracle->do("alter session set NLS_NUMERIC_CHARACTERS = '. '");
  $parametros->{client_encoding} = undef;
  $parametros->{server_encoding} = $dbh_oracle->ora_nls_parameters()->{NLS_CHARACTERSET};

  return $dbh_oracle;
};


1;
