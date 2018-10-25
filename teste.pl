#!/usr/bin/perl

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
my $pgdb   = "p10";
my $pguser = "abd7";

my $pwd = Senha::getPwd({'arquivo'=>'./pwd.enc'});

my $dbh_ora = Oracle::conexaoOracle({
  'usuario' => $orauser,
  'senha'   => $pwd, 
  'host'    => $orahost,
  'base'    => $oradb});

my $dbh_pg = Postgres::conexaoPG ({
  'usuario' => $pguser,
  'senha'   => $pwd,
  'host'    => $pghost,
  'base'    => $pgdb});

#my $cols1 = BaseDados::getColumns({
#  "dbh"=>$dbh_ora, 
#  "tabela"=>"TESTATISTICAS_RAC", 
#  "schema"=>"ABD7"});
#

#BaseDados::dumpTabela({
##  dbh => $dbh_ora,
#   dbh => $dbh_pg,
#   tabela => "tarquivo_anexo",
##  tabela => "TSOLICITACAO_INFORMATICA_ARQ",
##  tabela => "TROLE_ROLE",
##  tabela => "TESTATISTICAS_RAC",
##  tabela => "tnotificacao_usuario",
##  schema => "ABD7",
#  dir_dest => "/tmp/tarquivo_anexo",
#  limit => 10
#});

BaseDados::transfereTabela({
  dbh_src => $dbh_pg,
  dbh_dst => $dbh_ora,
#  schema_src => "abd7",
#  schema_dst => "ABD7",
#  tabela_src => "tespaco",
#  tabela_dst => "tespaco",
  tabela_src => "tarquivo_anexo",
  tabela_dst => "tarquivo_anexo",
  overwrite => 1,
  dados => 1
});

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
#
#my $pks1 = BaseDados::getPKs({
#  "dbh" => $dbh_ora,
#  "tabela" => "TACRESC_PERC_INTERNO_BENEF",
#  "schema"=>"ABD7"});
#
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

$dbh_ora->disconnect;
$dbh_pg->disconnect;
