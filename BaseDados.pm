package BaseDados;
use Data::Dumper;
use DBTipos;
use constant ATIVA_DEBUG => 0;
use strict;
use Array::Transpose;

# Descobre o nome dos atributos retornados para uma consulta ($sth)  
# que nao eh conhecida a SQL e deseja-se usar um dos atributos como
# parametro do fetchall_arrayref.
sub print_sth_fields {
  my $params = shift;
  my $sth = $params->{sth};
  my $all = $sth->fetchrow_arrayref();
  print Dumper($sth->{NAME});
};


# https://metacpan.org/pod/DBI#foreign_key_info
sub fk_rule {
  return (
    "CASCADE",
    "RESTRICT",
    "SET NULL",
    "NO ACTION", 
    "SET DEFAULT"
  )[shift()];
};


# Executar um comando de alteracao de estrutura
sub executaDDL {
  my $params = shift;
  my $dbh = $params->{"dbh"};
  my $sql = shift;

  ATIVA_DEBUG and print "DEBUG: executando DDL\n$sql\n";
  eval {
    $dbh->do($sql);
    $dbh->commit;
  };
  return ($@) ? 0 : 1;
}

sub getSchema {
  my $params = shift;
  my $driver = $params->{dbh}->{Driver}{Name};

  if (!defined($params->{schema})) {
    if ($driver eq DBTipos::ORA_DRIVER) {
      return uc($params->{dbh}->{Username});
    }
    elsif ($driver eq DBTipos::PG_DRIVER) {
      return undef;
    }
    else {
      print "getPKs: Driver nao implementado\n";
      return undef;
    }
  }
  else { # Totalmente desnecessario chamar se ja houver definido o schema
    return $params->{schema}
  }
};
  

# $boolean = getTable({dbh=>$dbh, tabela=>$tabela[, schema=>$schema]});
sub getTable {
  my $params = shift;
  my $dbh = $params->{"dbh"};
  my $tabela;
  my $schema = defined($params->{"schema"}) ? 
    $params->{"schema"} : getSchema($params);
  $tabela = DBTipos::def_name($params->{tabela}, $dbh->{Driver}{Name});

  my $sth = $dbh->table_info(undef, $schema, $tabela);
  my $tabs = $sth->fetchall_hashref('TABLE_NAME');
  my $num_tabs = keys(%$tabs);
  return ($num_tabs > 0 ) ? 1 : 0;
};


# $hashref = getColumns({dbh=>$dbh, tabela=>$tabela[, schema=>$schema]});
# Retorna um array $ret onde:
# $ret[0] = Driver do banco
# $ret[1..n] = Hashes com informacoes das colunas
# XXX: O column_info nao busca colunas de tabela particionada no PostgreSQL :(
sub getColumns {
  my $params = shift;
  my $dbh = $params->{"dbh"};
  my $tabela = $params->{"tabela"};
  my $schema = defined($params->{"schema"}) ? 
    $params->{"schema"} : getSchema($params);

  my $sth = $dbh->column_info(undef, $schema, $tabela, undef);
  my $attrs = $sth->fetchall_hashref('COLUMN_NAME');
  my $num_cols = keys(%$attrs);
  my @atributos = ();

  $atributos[0] = $dbh->{Driver}{Name};

  if ($num_cols > 0) {
    foreach my $attr (keys %$attrs) {
      my %atributo;
      $atributo{COLUMN_NAME} =  $attr;
      $atributo{DATA_TYPE} =    $attrs->{$attr}->{DATA_TYPE};
      $atributo{TYPE_NAME} =    $attrs->{$attr}->{TYPE_NAME};
      $atributo{COLUMN_SIZE} =  $attrs->{$attr}->{COLUMN_SIZE};
      $atributo{NULLABLE} =     $attrs->{$attr}->{NULLABLE};
      $atributo{POSITION} =     $attrs->{$attr}->{ORDINAL_POSITION};
      $atributos[$attrs->{$attr}->{ORDINAL_POSITION}] = \%atributo;
    }
    return \@atributos;
  }
  else {
    ATIVA_DEBUG and 
      print "ERRO: Numero de colunas = 0 ou tabela $tabela nao existe\n";
    return 0;
  }
};

# $hashref = getPKs({dbh=>$dbh, tabela=>$tabela[, schema=>$schema]});
sub getPKs {
  my $params = shift;
  my $dbh = $params->{"dbh"};
  my $tabela = $params->{"tabela"};
  my $schema = defined($params->{"schema"}) ? 
    $params->{"schema"} : getSchema($params);
  my $driver = $dbh->{Driver}{Name};

  my $sth = $dbh->primary_key_info(undef, $schema, $tabela);
  my $attrs = $sth->fetchall_hashref('COLUMN_NAME');
  my @chaves;
  my $pkname;

  foreach my $chave (keys %$attrs) {
    my %atributo;
    $atributo{COLUMN_NAME} =  $chave;
    $atributo{KEY_SEQ} =  $attrs->{$chave}->{KEY_SEQ};
    $atributo{COLUMN_NAME} =  $chave;
    $chaves[$attrs->{$chave}->{KEY_SEQ}] = \%atributo;
    $pkname = $attrs->{$chave}->{PK_NAME};
  }
  $chaves[0]->{PK_NAME} = $pkname;
  return \@chaves;
};

# $hashref = getFKs({dbh=>$dbh, tabela=>$tabela[, schema=>$schema], deps=0});
# Se deps != 0, mostra as FKs das tabelas que dependem da tabela
sub getFKs {
  my $params = shift;
  my $dbh = $params->{"dbh"};
  my $tabela = $params->{"tabela"};
  my $schema = defined($params->{"schema"}) ? 
    $params->{"schema"} : getSchema($params);
  my $sth;
  my $dependencias = defined($params->{deps}) ? $params->{deps} : 0;

  if ($dependencias eq 0) {
    $sth = $dbh->foreign_key_info("", "", "", "", "$schema", "$tabela");
  }
  else {
    $sth = $dbh->foreign_key_info("", "$schema", "$tabela", "", "", "");
  }
  my $chaves = $sth->fetchall_hashref(['FK_NAME', 'FK_COLUMN_NAME']);
  my @fks;
  my $c = 0; #indice da FK

  # fk("fk_name=>NAME, fk_cols=>[....]")
  while (my ($fk_key, $fk_val) = each(%$chaves)) {
    my @fk_cols;
    $fk_cols[0] = $fk_key;
    while (my ($fk_col_key, $fk_col_val) = each(%$fk_val)) {
      my $fk_col;
      $fk_col->{COLNAME} = $fk_col_val->{FK_COLUMN_NAME};  # Coluna
      $fk_col->{RCOLNAME} = $fk_col_val->{UK_COLUMN_NAME}; # Coluna referenciada
      $fk_col->{RTABLE} = $fk_col_val->{UK_TABLE_NAME};    # Tabela referenciada
      $fk_col->{RKEY} = $fk_col_val->{UK_NAME};    # Chave referenciada
      $fk_col->{DELETE} = fk_rule($fk_col_val->{DELETE_RULE});
      $fk_col->{UPDATE} = fk_rule($fk_col_val->{UPDATE_RULE});
      $fk_cols[$fk_col_val->{ORDINAL_POSITION}] = $fk_col;
    }
    #print Dumper @fk_cols;
    $fks[$c] = \@fk_cols;
    $c++;
  }
  #print Dumper @fks;
  return @fks;
};


# Gera a SQL para criacao de tabela na base de dados $driver_dest
# a partir de atributos lidos da base de dados $driver_orig
# TODO: melhorar a precisao numerica. Por enquanto so ta transferindo 
# a precisao do VARCHAR (DATA_TYPE == 12 e DATA_TYPE == 1)
# XXX: Modifica o parametro de entrada, criando o $params->{atributos_dest}
# contendo a estrutura da tabela que sera gerada
sub geraCreateSQL {
  my $params = shift;
  my $tabela = $params->{tabela};
  my $schema = $params->{schema};
  my $atributos_orig = $params->{atributos};
  my $pks_orig = $params->{pks};
  my $fks_orig = $params->{fks};
  my $indices_orig = $params->{indices};
  my $driver_dest = $params->{dbh}->{Driver}{Name};

  my $driver_orig = $atributos_orig->[0];
  my $num_atribs_orig = @$atributos_orig;
  my $map_type;
  my $map_name;
  my @sql_dest;
  my $atributos_dest;

  ATIVA_DEBUG and print "DEBUG: $driver_orig ---> $driver_dest\n";

  # Mapear as funcoes que "traduzem" 
  # - os tipos dos atributos origem para o destino ($map_type)
  # - os nomes dos atributos ($map_name)
  if ($driver_orig eq $driver_dest) {
    $map_type = \&DBTipos::identidade;
    $map_name = \&DBTipos::identidade;
  }
  else {
    $map_name = \&DBTipos::def_name;
    if ($driver_orig eq DBTipos::PG_DRIVER) {
      if ($driver_dest eq DBTipos::ORA_DRIVER) {
        $map_type = \&DBTipos::pg2ora;
      }
    }
    elsif ($driver_orig eq DBTipos::ORA_DRIVER) {
      if ($driver_dest eq DBTipos::PG_DRIVER) {
        $map_type = \&DBTipos::ora2pg;
      }
    }
    else {
      die("Conversao nao implementada: $driver_orig/$driver_dest");
    }
  }

  $atributos_dest->[0] = $driver_dest;
  my $attr_seq = 0;
  my $sql = "CREATE TABLE $tabela ( \n";

  for (my $i = 1; $i <@$atributos_orig; $i++) {
    my $attr = $atributos_orig->[$i];
    if (defined($attr)) {
      my %attr_dest;
      $attr_dest{COLUMN_NAME} = $map_name->($attr->{COLUMN_NAME}, 
        $driver_dest);
      $attr_dest{TYPE_NAME} = $map_type->($attr->{TYPE_NAME});
      $attr_dest{COLUMN_SIZE} = $attr->{COLUMN_SIZE};
      $attr_dest{NULLABLE} = $attr->{NULLABLE};
      $attr_dest{POSITION} = $attr->{POSITION};
      $atributos_dest->[$attr_seq+1] = \%attr_dest;

      # Geracao da SQL
      $sql_dest[$attr_seq] = $attr_dest{COLUMN_NAME}." ".$attr_dest{TYPE_NAME};
      $sql_dest[$attr_seq] .= "(".$attr->{COLUMN_SIZE}.")" if 
        (($attr->{DATA_TYPE} == 12) || ($attr->{DATA_TYPE} == 1));
      $sql_dest[$attr_seq] .= " NOT NULL" if ($attr->{NULLABLE} == 0);
      $attr_seq++;
    }
  }

  # Adiciona o atributo "atributos_dest"
  $params->{atributos_dest} = $atributos_dest;

  $sql .= join(",\n", @sql_dest);
  $sql .= ")";
  return $sql;
};

# Verifica se a tabela ja existe no destino.
# Se ja existir e tiver o parametro overwrite, faz um drop/create
# Se nao existir, simplesmente cria
# TODO: Verificar schema na hora de dar o DROP
sub criaTabela {
  my $params = shift;
  my $schema = defined($params->{"schema"}) ? 
    $params->{"schema"} : getSchema($params);
  my $overwrite = defined($params->{overwrite}) ? $params->{overwrite} : 0;
  if (getTable($params)) {
    if ($overwrite != 1) {
      print "Tabela ja existe e overwrite nao especificado. Abortando!\n";
      return 0;
    }
    else {
      my $tabela = $params->{tabela};
      if (!executaDDL($params, "DROP TABLE $tabela")) {
        print "Nao foi possivel remover a tabela $tabela\n";
        return 0;
      }
    }
  }
  else {
    print "Tabela ".$params->{tabela}." nao existe. Criar.\n";
  }
  my $sql = geraCreateSQL($params);
  return executaDDL($params, $sql);
};


# Copia a tabela em um arquivo [CSV? SQL?] e, caso a coluna seja do
# tipo LONG / LONG RAW / CLOB / BLOB / BYTEA / OID, 
# cria um arquivo com o conteudo do binario, com o nome da PK (se houver)
# ou do identificador de linha (ROWID / oid)
# TODO: terminar!
sub dumpTabela {
  my $params = shift;
  my $dbh = $params->{dbh};
  my $tabela = $params->{tabela};
  my $dir_dest = $params->{dir_dest};
  my $limit = $params->{limit};
  my $driver = $dbh->{Driver}{Name};
  my @pks_attr;
  my $estrutura = getColumns($params);
  my $pks = getPKs($params);
  shift @$estrutura;
  shift @$pks;
  my $unique_select;

  if ( -d $dir_dest ) {
    print "Existe\n";
    while ($_ = glob("$dir_dest/*")) {
      next if -d $_;
      unlink($_) and print "$_ removido\n" or die("Nao pode remover $_");
    }
  }
  else {
    print "Nao existe\n";
    if (!mkdir($dir_dest)) {
      die ("Nao foi possivel criar o diretorio de dump $dir_dest");
    }
  }

  # Se existir chave primaria, busca os atributos
  if (@$pks > 0) {
    @pks_attr = map { $_->{COLUMN_NAME} } 
      sort { $b->{KEY_SEQ} <=> $a->{KEY_SEQ} } @$pks;
    $unique_select = join("||'_'||", @pks_attr);
  }
  else {
    $unique_select = DBTipos::ROWID->{$driver};
  }

  # Se o tipo for binario, long, long raw, bytea, etc, o select eh feito
  # em conjunto com a chave primaria ou com o identificador de coluna
  my @sql_fields = map { 
    (DBTipos::LONGS->{$_->{TYPE_NAME}}) ? 
      "'".$_->{COLUMN_NAME}."_'||".$unique_select : 
      $_->{COLUMN_NAME} 
  } @$estrutura;

  # Filtrar os campos que sao do tipo binario
  my @bin_fields = map { $_->{COLUMN_NAME} } 
    grep { DBTipos::LONGS->{$_->{TYPE_NAME}} == 1 } @$estrutura;

  my $sql = "SELECT ".join(",",@sql_fields)." FROM $tabela ";
  $sql = DBTipos::addSQLLimit($sql, $driver, $limit) if (defined($limit));

  # Executar o SELECT de $sql e gravar no arquivo
  my $sth = $dbh->prepare($sql);
  $sth->execute();
  while (my $res = $sth->fetchrow_arrayref()) {
    print join(",", @$res)."\n";
  }

  # Gravar os campos binario em arquivos com o nome do campo e id unico
  if (@bin_fields > 0) {
    $dbh->{LongReadLen} = 100*1024*1024; #TODO: Definir via parametro? 
    $dbh->{LongTruncOk} = 0; # Se tamanho do dado > $dbh->{LongReadLen}, die!
    foreach my $bin_field (@bin_fields) {
      $sql = "SELECT $bin_field, '$bin_field'||$unique_select FROM $tabela";
      $sql = DBTipos::addSQLLimit($sql, $driver, $limit) if (defined($limit));
      $sth = $dbh->prepare($sql);
      $sth->execute();
      while (my $res = $sth->fetchrow_arrayref()) {
        open(my $fh, '>:raw', "$dir_dest/".$res->[1]);
        print($fh $res->[0]);
      }
    }
  }
}

# BaseDados::transfereTabela({
#   dbh_src => $dbh_origem,
#   dbh_dst => $dbh_destino,
#   [schema_src => "schema_origem",]
#   [schema_dst => "schema_destino",]
#   tabela_src => "tabela_origem"
#   tabela_src => "tabela_destino"
#   overwrite => (0: nao faz drop se a tabela existir. 1: drop if existis)
#   dados => (0: nao transfere dados; 1: transfere)
#})
sub transfereTabela {
  my $params = shift;
  my $dbh_src = $params->{dbh_src};
  my $dbh_dst = $params->{dbh_dst};
  my $tabela_src = $params->{tabela_src};
  my $tabela_dst = $params->{tabela_dst};
  my $schema_src = $params->{schema_src};
  my $schema_dst = $params->{schema_dst};
  my $driver_src = $dbh_src->{Driver}{Name};
  my $driver_dst = $dbh_dst->{Driver}{Name};
  my $overwrite = defined($params->{overwrite}) ? $params->{overwrite} : 0;
  my $migra_dados = defined($params->{dados}) ? $params->{dados} : 0;

  # Busca estrutura da tabela que sera transferida
  my $estrutura = getColumns({
      dbh => $dbh_src,
      tabela => $tabela_src,
      schema => $schema_src});

  if (!$estrutura) {
    print "Nao foi possivel buscar os atributos da tabela $tabela_src.\n";
    return 0;
  }

  # Create table
  my $params_cria = {dbh=>$dbh_dst, tabela=>$tabela_dst, schema=>$schema_dst,
    overwrite=>$overwrite, atributos=>$estrutura};
  if (!criaTabela($params_cria)) {
    print "Nao foi possivel [re]criar a tabela $tabela_dst.\n";
    return 0;
  }

  # XXX: BLOBS: http://www.perlguru.com/gforum.cgi?post=79689
  if ($migra_dados) {
    #($driver_dst eq DBTipos::ORA_DRIVER) and $dbh_dst->{LongReadLen} = 100*1024*1024;
    # No caso do transfere, tabela destino === tabela origem
    # Nao eh necessario fazer comparacao estrutural
    my $sql = "SELECT * FROM $tabela_src";
    my $sth = $dbh_src->prepare($sql);
    my $res = $sth->execute();
    my @tipos = @{$params_cria->{atributos_dest}};
    shift(@tipos);

    print Dumper(@tipos);

    # Verificar como que deve ser feita a transferencia dos dados
    # 1: Execucao comum => SELECT INTO ARRAY -> INSERT ARRAY INTO TABLE
    # 2: Execucao por array => SELECT, transpose, bind_param_array, execute
    my $tipo_execucao = 1;

    # XXX: Se tiver qualquer atributo do tipo LOB (BLOB, CLOB), as operacoes 
    # nao podem ser feitas por array: "LOBs not supported for array operation."
    # TODO: Criar execucao por array com timestamp no Oracle
    # Corrigir erro ORA-01843

    # $pars[$tipo[$i]->{TYPE_NAME}] = 1 para todo $tipo[$i].
    # Cria um hash com valor 1 pra todos os valores de $tipo[$i]->{TYPE_NAME}
    my %pars = map {$_->{TYPE_NAME} => 1} @tipos;
    if ($pars{BLOB}) {
      $tipo_execucao = 1; # Execucao linear
    }
    else {
      $tipo_execucao = 2; # Execucao por bind_array
    }

    # TODO: Definir a quantidade de linhas a partir da qual a consulta deve
    # ser particionada
    if ($res > 0) {
      if ($tipo_execucao == 1) {
        my $rows = $sth->fetchall_arrayref();
        my $num_cols = @{$rows->[0]};
        my $insert_sql = "INSERT INTO $tabela_dst VALUES (";
        $insert_sql .= ("?, " x ($num_cols-1))."?)";
        my $ins_sth = $dbh_dst->prepare_cached($insert_sql);
        foreach my $item (@$rows) {
          $ins_sth->execute(@$item);
        }
      }
      elsif ($tipo_execucao == 2) {
        if ($res < 5e7) {
          my $rows = $sth->fetchall_arrayref();
#          print Dumper $rows;
          my $num_cols = @{$rows->[0]};
          my $insert_sql = "INSERT INTO $tabela_dst VALUES (";
          $insert_sql .= ("?, " x ($num_cols-1))."?)";
          my $ins_sth = $dbh_dst->prepare_cached($insert_sql);
          my $rows_transp = transpose($rows);
#          print Dumper($rows_transp);
          for (my $i = 0; $i < @{$rows_transp}; $i++) {
            if ($tipos[$i]->{TYPE_NAME} eq "BLOB") {
              #print DBTipos::ORA_BLOB."\n";
              $ins_sth->bind_param_array($i+1, $rows_transp->[$i], {ora_type=>DBTipos::ORA_BLOB});
            }
            else {
              $ins_sth->bind_param_array($i+1, $rows_transp->[$i]);
            }
          }
          my %attr;
          $ins_sth->execute_array(\%attr);
          $ins_sth->finish;
        }
        else {
          print "Implementar transferencia de tabela grande\n";
          return 0;
        }
      }
    }
    else {
      print "Nao houve resultados da tabela $tabela_src\n";
      return 1;
    }
  }
  else {
    # Nao migra os dados
    undef;
  }
};

1;
