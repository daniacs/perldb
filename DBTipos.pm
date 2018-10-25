package DBTipos;
use constant ORA_DRIVER => "Oracle";
use constant PG_DRIVER => "Pg";
use DBD::Oracle qw(:ora_types);
use constant ORACLE_BLOB => ORA_BLOB;

sub ora2pg {
  {
    "CHAR"            => "char",
    "NCHAR"           => "char",
    "VARCHAR"         => "varchar",
    "NVARCHAR"        => "varchar",
    "VARCHAR2"        => "varchar",
    "NVARCHAR2"       => "varchar",
    "STRING"          => "varchar",
    "DATE"            => "timestamp",
    "LONG"            => "text",
    "LONG RAW"        => "bytea",
    "CLOB"            => "text",
    "NCLOB"           => "text",
    "BLOB"            => "bytea",
    "BFILE"           => "bytea",
    "RAW"             => "bytea",
    "ROWID"           => "oid",
    "FLOAT"           => "double precision",
    "INT"             => "numeric",
    "NUMBER"          => "numeric",
    "BINARY_INTEGER"  => "integer",
    "PLS_INTEGER"     => "integer",
    "REAL"            => "real",
    "SMALLINT"        => "smallint",
    "BINARY_FLOAT"    => "double precision",
    "BINARY_DOUBLE"   => "double precision",
    "TIMESTAMP"       => "timestamp",
    "BOOLEAN"         => "boolean",
    "INTERVAL"        => "interval",
    "XMLTYPE"         => "xml",
    "TIMESTAMP WITH TIME ZONE"        => "timestamp with time zone",
    "TIMESTAMP WITH LOCAL TIME ZONE"  => "timestamp with time zone",
    "SDO_GEOMETRY"    => "geometry"
  }->{shift()};
};


sub pg2ora{
  {
    "char"                  =>    "CHAR",
    "character"             =>    "CHAR",
    "varchar"               =>    "VARCHAR2",
    "character varying"     =>    "VARCHAR2",
    "timestamp"             =>    "DATE",
    "text"                  =>    "CLOB",
    "bytea"                 =>    "BLOB",
    "oid"                   =>    "BLOB",
    "double precision"      =>    "FLOAT",
    "numeric"               =>    "NUMBER",
    "integer"               =>    "INTEGER",
    "bigint"                =>    "NUMBER(19)",
    "real"                  =>    "REAL", 
    "smallint"              =>    "SMALLINT",
    "boolean"               =>    "BOOLEAN",
    "interval"              =>    "INTERVAL",
    "xml"                   =>    "XMLTYPE",
    "timestamp with time zone"      => "TIMESTAMP WITH TIME ZONE",
    "timestamp without time zone"   => "TIMESTAMP",
    "geometry"              => "SDO_GEOMETRY"
  }->{shift()};
};

sub identidade {
  return shift();
};

# def_name(nome_coluna, Driver banco)
sub def_name {
  my $col = lc(shift);
  my $driver = shift;

  if ($driver eq PG_DRIVER) {
    if ($col =~ /\W/) {
      return "\"$col\"";
    }
    return $col;
  }
  elsif ($driver eq ORA_DRIVER) {
    return uc($col);
  }
  else {
    print "Driver especificado nao implementado\n";
    return $col;
  }
};


sub tipoAtributo {
  {
    1   => "CHAR",
    3   => "NUMERIC",
    4   => "PG INTEGER",
    5   => undef,
    6   => undef,
    7   => undef,
    8   => "ORA NUMBER",
    11  => "PG TIMESTAMP WITHOUT TIME ZONE",
    12  => "VARCHAR",
    30  => "ORACLE BLOB",
    40  => "ORACLE CLOB",
    91  => "PG DATE",
    92  => "PG TIME WITHOUT TIME ZONE",
    93  => "DATE/TIMESTAMP",
    -1  => "PG TEXT / ORA LONG",
    -3  => "PG BYTEA / ORA RAW",
    -4  => "ORA LONG RAW",
    -5  => "PG BIGINT",
    -9104 => "ORA ROWID",
  }->{shift()};
};

sub tipoLong {
  return {
    "LONG" => 1,
    "RAW" => 1,
    "LONG RAW" => 1,
    "CLOB" => 1,
    "BLOB" => 1,
    "BYTEA" => 1,
    "OID" => 1
  }->{uc(shift)};
};

use constant LONGS => {
  "LONG" => 1,
  "RAW" => 1,
  "LONG RAW" => 1,
  "CLOB" => 1,
  "BLOB" => 1,
  "bytea" => 1,
  "oid" => 1
};

# TODO: Fazer isso com as constantes ORA_DRIVER e PG_DRIVER?
use constant ROWID => {
  "Oracle" => "ROWID",
  "Pg"  => "oid"
};

# Adiciona a clausula LIMIT, de acordo com os parametros obrigatorios
# sql: query
# driver: dbh->{Driver}{Name}
# limit: inteiro positivo
sub addSQLLimit {
  my $sql = shift;
  my $driver = shift;
  my $limit = shift;

  if ($limit > 0) {
    if ($driver eq ORA_DRIVER) {
      $sql .= " WHERE ROWNUM <= $limit";
    }
    elsif ($driver eq PG_DRIVER) {
      $sql .= " LIMIT $limit";
    }
  }
  return $sql;
};


1;
