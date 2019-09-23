package BaseDados;
use Data::Dumper;
use DBTipos;
use strict;
use Array::Transpose;
use Time::HiRes qw(time);

use constant ATIVA_DEBUG => 0;
# Tipos de execucao
use constant EXEC_LINEAR => 1;
use constant EXEC_PARALLEL => 2;
use constant EXEC_BLOB => 3;
use constant EXEC_DESC => {
  1 => "execucao linear",
  2 => "execucao paralela",
  3 => "execucao com binarios/blob"
};

# Quando ha transferencia de dados binarios, define o tamanho
# maximo que esses dados podem ter (para cada registro).
use constant MAX_BIN_SIZE => 100*1024*1024;

# TODO: quando o numero de registros for maior que ROW_LIMIT_FOR_SPLIT_INSERT,
# optar por dividir a execucao (multiplos commits) a cada SPLIT_ROWS
# (ou permitir a definicao do split via parametro).
use constant ROW_LIMIT_FOR_SPLIT_INSERT => 500000;
use constant SPLIT_ROWS => 50000;


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
  #print Dumper $attrs;
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
      $atributo{COLUMN_DEF} =   $attrs->{$attr}->{COLUMN_DEF};
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
  my $attrs;
  if (defined($sth)) {
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
  }
  else {
    return undef;
  }
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
  my $map_default;
  my @sql_dest;
  my $atributos_dest;

  ATIVA_DEBUG and print "DEBUG: $driver_orig ---> $driver_dest\n";

  # Mapear as funcoes que "traduzem" 
  # - os tipos dos atributos origem para o destino ($map_type)
  # - os nomes dos atributos ($map_name)
  if ($driver_orig eq $driver_dest) {
    $map_type = \&DBTipos::identidade;
    $map_name = \&DBTipos::identidade;
    $map_default = \&DBTipos::identidade;
  }
  else {
    $map_name = \&DBTipos::def_name;
    if ($driver_orig eq DBTipos::PG_DRIVER) {
      if ($driver_dest eq DBTipos::ORA_DRIVER) {
        $map_type = \&DBTipos::pg2ora;
        $map_default = \&DBTipos::def_value;
      }
    }
    elsif ($driver_orig eq DBTipos::ORA_DRIVER) {
      if ($driver_dest eq DBTipos::PG_DRIVER) {
        $map_type = \&DBTipos::ora2pg;
        $map_default = \&DBTipos::def_value;
      }
    }
    else {
      die("Conversao nao implementada: $driver_orig/$driver_dest");
    }
  }

  $atributos_dest->[0] = $driver_dest;
  my $attr_seq = 0;
  my $sql = "CREATE TABLE $tabela ( \n";

  # CREATE TABLE .... [DEFAULT ...] [NOT NULL]
  for (my $i = 1; $i <@$atributos_orig; $i++) {
    my $attr = $atributos_orig->[$i];
    if (defined($attr)) {
      my %attr_dest;
      $attr_dest{COLUMN_NAME} = $map_name->($attr->{COLUMN_NAME}, 
        $driver_dest);
      $attr_dest{TYPE_NAME} = $map_type->($attr->{TYPE_NAME});
      $attr_dest{COLUMN_DEF} = $map_default->($attr->{COLUMN_DEF}, $attr->{TYPE_NAME});
      $attr_dest{COLUMN_SIZE} = $attr->{COLUMN_SIZE};
      $attr_dest{NULLABLE} = $attr->{NULLABLE};
      $attr_dest{POSITION} = $attr->{POSITION};
      $atributos_dest->[$attr_seq+1] = \%attr_dest;

      # Geracao da SQL
      $sql_dest[$attr_seq] = $attr_dest{COLUMN_NAME}." ".$attr_dest{TYPE_NAME};
      $sql_dest[$attr_seq] .= "(".$attr->{COLUMN_SIZE}.")" if 
        (($attr->{DATA_TYPE} == 12) || ($attr->{DATA_TYPE} == 1));
      $sql_dest[$attr_seq] .= " DEFAULT ".$attr_dest{COLUMN_DEF} if defined ($attr->{COLUMN_DEF});
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
  my $sql = geraCreateSQL($params);
  return executaDDL($params, $sql);
};

# force = 1: remove a tabela mesmo se houver dados.
sub removeTabela {
  my $params = shift;
  my $dbh = $params->{dbh};
  my $schema = $params->{schema};
  my $driver = $dbh->{Driver}{Name};
  my $tabela = DBTipos::def_name($params->{tabela}, $driver);
  my $force = $params->{force};

  if (getTable($params)) {
    my $sql = "SELECT COUNT(1) FROM $tabela";
    my $sth = $dbh->prepare($sql);
    my $res = $sth->execute;
    $res = $sth->fetchrow_arrayref();
    if (($res->[0] > 0) && ($force != 1)) {
      print "Tabela com dados e opcao 'force' nao especificada\n";
      return undef;
    }
    eval {
      $dbh->do("DROP TABLE $tabela");
    };
    if ($@) {
      print "Nao foi possivel excluir a tabela $tabela\n";
    }
  }
  else {
    print "Tabela $tabela nao existe\n";
    return undef;
  }
};

# Copia a tabela em um arquivo [CSV? SQL?] e, caso a coluna seja do
# tipo LONG / LONG RAW / CLOB / BLOB / BYTEA / OID, 
# cria um arquivo com o conteudo do binario, com o nome da PK (se houver)
# ou do identificador de linha (ROWID / oid)
sub dumpTabela {
  my $params = shift;
  my $dbh = $params->{dbh};
  my $dir_dest = $params->{dir_dest};
  my $limit = $params->{limit};
  my $driver = $dbh->{Driver}{Name};
  my $tabela = DBTipos::def_name($params->{tabela}, $driver);
  my @pks_attr;

  return undef if (!getTable($params));

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
    print "Diretorio $dir_dest nao existe\n";
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
    $dbh->{LongReadLen} = MAX_BIN_SIZE;
    $dbh->{LongTruncOk} = 0;
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
#   limit => numero de registros transferidos
#   pk => (0: nao migra chave primaria; 1: migra a chave primaria)
#   defer_pk => (0: cria pk antes dos dados; 1: cria pk depois da migracao)
#})
sub transfereTabela {
  my $params = shift;
  my $dbh_src = $params->{dbh_src};
  my $dbh_dst = $params->{dbh_dst};
  my $schema_src = $params->{schema_src};
  my $schema_dst = $params->{schema_dst};
  my $driver_src = $dbh_src->{Driver}{Name};
  my $driver_dst = $dbh_dst->{Driver}{Name};
  my $tabela_src = DBTipos::def_name($params->{tabela_src}, $driver_src);
  my $tabela_dst = DBTipos::def_name($params->{tabela_dst}, $driver_dst);
  my $overwrite = defined($params->{overwrite}) ? $params->{overwrite} : 0;
  my $migra_dados = defined($params->{dados}) ? $params->{dados} : 0;
  my $limit = $params->{limit};
  my $debug = $params->{debug};
  my $migra_pk = defined($params->{pk}) ? $params->{pk} : 0;
  my $defer_pk = defined($params->{defer_pk}) ? $params->{defer_pk} : 0;
  my $pgcopy = defined($params->{pgcopy}) ? $params->{pgcopy} : 0;
  my $modo = defined($params->{modo}) ? $params->{modo} : EXEC_LINEAR;

  my $tempo_inicio = time();
  my $delta_t = 0;
  my $checkpoint_t = 0;
  my $tipo_execucao;

  # Busca estrutura da tabela origem
  my $estrutura = getColumns({
      dbh => $dbh_src,
      tabela => $tabela_src,
      schema => $schema_src});

  if (!$estrutura) {
    print "Nao foi possivel buscar os atributos da tabela $tabela_src.\n";
    return 0;
  }

  $checkpoint_t = time();
  $delta_t = $checkpoint_t - $tempo_inicio;
  printf("getColumns:\t\t%.2f s\n", $delta_t) if ($debug == 1);

  #my $pk = getPKs({dbh => $dbh_src, tabela=>$tabela_src, schema=>$schema_src });
  #print Dumper($pk) and die;

  # Cria a tabela destino
  my $params_cria = {dbh=>$dbh_dst, tabela=>$tabela_dst, schema=>$schema_dst,
    overwrite=>$overwrite, atributos=>$estrutura};
  if (!criaTabela($params_cria)) {
    print "Nao foi possivel [re]criar a tabela $tabela_dst.\n";
    return 0;
  }

  $delta_t = time() - $checkpoint_t;
  $checkpoint_t = time();
  printf("DROP/CREATE TABLE\t%.2f s\n", $delta_t) if ($debug == 1);

  if ($migra_dados) {
    # Transfere: origem === destino (nao precisa comparar estrutura)
    my $sql = "SELECT * FROM $tabela_src";
    $sql = DBTipos::addSQLLimit($sql, $driver_src, $limit) 
      if (defined($limit));

    if ($driver_src eq DBTipos::ORA_DRIVER) {
      $dbh_src->{LongReadLen} = MAX_BIN_SIZE;
      $dbh_src->{LongTruncOk} = 0;
    }

    print "SQL:\t\t\t$sql\n" if ($debug == 1);

    my $sth = $dbh_src->prepare($sql);
    my $res = $sth->execute();
    my $all_rows = $sth->fetchall_arrayref();

    $delta_t = time() - $checkpoint_t;
    $checkpoint_t = time();
    if ($debug == 1) {
      printf("SELECT\t\t\t%.2f s\n", $delta_t);
      print "LINHAS\t\t\t".@$all_rows."\n";
    }

    my @tipos = @{$params_cria->{atributos_dest}};
    shift(@tipos);
    my @tipo_cols = map { $_->{TYPE_NAME} } @tipos;

    my %pars = map { $_->{TYPE_NAME} => 1 } @tipos;
    my @attr_bin = map { DBTipos::LONGS->{$_} or 0 } @tipo_cols;
    my $num_bins = 0;
    map { $num_bins += $_ } @attr_bin;

    if ($num_bins > 0) {
      $tipo_execucao = EXEC_BLOB;
    }
    else {
      $tipo_execucao = $modo;
    }
    print "Transferencia de dados via ".EXEC_DESC->{$modo}."\n" if ($debug == 1);

    if (@$all_rows < 1) {
      print "Nao houve resultados da tabela $tabela_src\n";
      return 0;
    }

    if ($tipo_execucao == EXEC_BLOB) {
      $dbh_dst->{LongReadLen} = MAX_BIN_SIZE;
      $dbh_dst->{LongTruncOk} = 0;
      my $rows = $all_rows;
      my $num_cols = @{$rows->[0]};
      my $insert_sql = "INSERT INTO $tabela_dst VALUES (";
      $insert_sql .= ("?, " x ($num_cols-1))."?)";
      my $ins_sth = $dbh_dst->prepare_cached($insert_sql);
      eval {
        foreach my $item (@$rows) {
          for (my $i = 0; $i < @$item; $i++) {
            $ins_sth->bind_param(
              $i+1, 
              $item->[$i], 
              DBTipos::MAP_TIPO->{$tipo_cols[$i]}
            );
          }
          $ins_sth->execute();
        }
      };
      if ($@) {
        print "Houve uma falha ao executar a copia dos dados! Rollback!\n";
        $dbh_dst->rollback();
      }
      else {
        $dbh_dst->commit();
      }
    }
    elsif ($tipo_execucao == EXEC_LINEAR) {
      my $num_cols = @{$all_rows->[0]};
      my $insert_sql = "INSERT INTO $tabela_dst VALUES (";
      $insert_sql .= ("?, " x ($num_cols-1))."?)";
      my $ins_sth = $dbh_dst->prepare_cached($insert_sql);
      eval {
        if (($driver_dst eq DBTipos::PG_DRIVER)  && ($pgcopy)) {
          $dbh_dst->do("COPY $tabela_dst FROM STDIN");
          foreach my $item (@$all_rows) {
            #shift(@$item);
            my $rowlen = @$item;
            #@$item[$rowlen-1] = '\N' if (not defined(@$item[$rowlen-1]));
            #print join("\t", @$item)."\n";
            $dbh_dst->pg_putcopydata(join("\t", @$item)."\n");
          }
          $dbh_dst->pg_putcopyend();
        }
        else {
          foreach my $item (@$all_rows) {
            #print $item->[1]."\n";
            $ins_sth->execute(@$item);
          }
        }
      };
      if ($@) {
        print "Houve uma falha ao executar a copia dos dados!\n";
        print "Executando rollback\n";
        $dbh_dst->rollback();
      }
      else {
        $dbh_dst->commit();
      }
    }
    elsif ($tipo_execucao == EXEC_PARALLEL) {
      my $parallel_t0 = time();
      my $parallel_chkpt;
      my $num_cols = @{$all_rows->[0]};
      my $insert_sql = "INSERT INTO $tabela_dst VALUES (";
      $insert_sql .= ("?, " x ($num_cols-1))."?)";
      my $ins_sth = $dbh_dst->prepare_cached($insert_sql);

      eval {
        my $rows_transp = transpose($all_rows);
        for (my $i = 0; $i < @{$rows_transp}; $i++) {
          $ins_sth->bind_param_array($i+1, $rows_transp->[$i]);
        }
        my %attr;
        $ins_sth->execute_array(\%attr);
        $ins_sth->finish;
      };
      if ($@) {
        print "Houve uma falha ao executar a copia dos dados!\n";
        print "Executando rollback\n";
        $dbh_dst->rollback();
      }
      else {
        $dbh_dst->commit();
      }
    }
    $delta_t = time() - $checkpoint_t;
    $checkpoint_t = time();
    printf("Migracao dos dados\t%.2f s\n", $delta_t) if ($debug == 1);
  }
  else {
    # Nao migra os dados
    undef;
  }
};

1;
