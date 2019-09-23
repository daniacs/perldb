#!/usr/bin/perl -CS

use strict;
use Oracle;
use Postgres;
use Senha;
use BaseDados;
use Data::Dumper;

my $orahost = "homoloracle.almg.uucp";
my $oradb   = "P10";
my $orauser = "ABD7";

my $pghost = "postgresql1h.almg.uucp";
my $pgdb   = "oradata";
my $pguser = "abdstat";

my $orapwd = Senha::getPwd({'arquivo'=>'./ora.enc'});
my $pgpwd  = Senha::getPwd({'arquivo'=>'./pg.enc'});

my $param_ora = {
  'usuario' => $orauser,
  'senha'   => $orapwd, 
  'host'    => $orahost,
  'base'    => $oradb
};

my $param_pg = {
  'usuario' => $pguser,
  'senha'   => $pgpwd,
  'host'    => $pghost,
  'base'    => $pgdb
};

my $dbh_ora = Oracle::conexaoOracle($param_ora);
my $dbh_pg = Postgres::conexaoPG ($param_pg);

#my $cols1 = BaseDados::getColumns({
#  "dbh"=>$dbh_pg, 
#  "tabela"=>"tagendamento", 
#  "schema"=>"abd7"});
#print Dumper($cols1);

#EXEC_LINEAR => 1
#EXEC_PARALLEL => 2
#EXEC_BLOB => 3
BaseDados::transfereTabela({
  dbh_src => $dbh_ora,
  dbh_dst => $dbh_pg,
  tabela_src => "testatisticas_segmentos",
  tabela_dst => "testatisticas_segmentos",
  overwrite => 1,
  dados => 1,
  debug=>1,
  pgcopy => 1,
  modo => 1
});



#BaseDados::transfereTabela({
#  dbh_src => $dbh_pg,
#  dbh_dst => $dbh_ora,
#  tabela_src => "tarquivo_anexo",
#  tabela_dst => "TARQUIVO_ANEXO",
#  overwrite => 1,
#  dados => 1,
#  limit => 1000000,
#  debug=>1
#});

#BaseDados::dumpTabela({
#  dbh => $dbh_ora,
##   dbh => $dbh_pg,
##   tabela => "tarquivo_anexo",
#  tabela => "TSOLICITACAO_INFORMATICA_ARQ",
##  tabela => "TROLE_ROLE",
##  tabela => "TESTATISTICAS_RAC",
##  tabela => "tnotificacao_usuario",
##  schema => "ABD7",
#  dir_dest => "/tmp/tsolicitacao_informatica_arq",
#  limit => 10
#});

#BaseDados::transfereTabela({
#  dbh_src => $dbh_ora,
#  dbh_dst => $dbh_pg,
#  schema_src => "ABD7",
#  schema_dst => "abd7",
#  tabela_src => "TESTATISTICAS_RAC",
#  tabela_dst => "ttemp",
#  overwrite => 1,
#  dados => 1
#});

#my $cols1 = BaseDados::getColumns({
#  "dbh"=>$dbh_ora, 
#  "tabela"=>"TESTATISTICAS_RAC", 
#  "schema"=>"ABD7"});

#my $cols1 = BaseDados::getColumns({
#  "dbh"=>$dbh_ora, 
#  "tabela"=>"TESTATISTICAS_RAC", 
#  "schema"=>"ABD7"});

#my $pks1 = BaseDados::getPKs({
#  "dbh" => $dbh_ora,
#  "tabela" => "TACRESC_PERC_INTERNO_BENEF",
#  "schema"=>"ABD7"});

#my $fks = BaseDados::getFKs({
#  "dbh" => $dbh_ora,
#  #"tabela" => "TCREDENCIADO",
#  "tabela" => "TCONSULTA",
#  #"tabela" => "TROLE_ROLE",
#  "schema"=>"ABD7",
#  "deps" => 0
#});

#BaseDados::transfereTabela({
#  dbh_src => $dbh_ora,
#  dbh_dst => $dbh_pg,
#  schema_src => "ABD7",
#  schema_dst => "abd7",
#  tabela_src => "TSOLIC_NOTA_TAQ",
#  tabela_dst => "tconsulta01",
#  overwrite => 1
#});

#BaseDados::criaTabela({
#  dbh=>$dbh_ora,
#  tabela => "TTESTE",
#  overwrite => 1, 
#  atributos => $cols2});

#BaseDados::removeTabela({
#  dbh => $dbh_ora,
#  tabela => "TSOLICITACAO_INFORMATICA_ARQ2"
#});

$dbh_ora->disconnect;
$dbh_pg->disconnect;
