#!/usr/bin/perl
# Gera o arquivo $arq com a senha criptografada
# que deve ser utilizada com a funcao Senha::getPwd(arquivo=>$arq).

use strict;
use lib '/u01/app/oracle/product/11.2.0/dbhome_1/plus/admin/estatisticas/lib';
use Senha;

my $helpmsg = "Definir arquivo com a senha que deve ser gerada!
Ex: $0 <arq_senha> [<arq_senha_criptografada>]
";

die($helpmsg) 
  if (@ARGV < 1);

my $arqIn  = $ARGV[0];
my $arqOut;
my $senha;

if (defined $ARGV[1]) {
  $arqOut = $ARGV[1];
}
else {
  $arqOut = "/tmp/pwd.enc";
}

#local $/ = undef;
open(ARQIN, "<$arqIn") or die("Nao foi possivel abrir o arquivo $arqIn");
$senha = <ARQIN>;
close(ARQIN);
chomp($senha);

Senha::makePwd({senha=>$senha, arquivo=>$arqOut});
chmod(0600, $arqOut);
print "Arquivo de senha gerado: $arqOut\n";

my $textoPlano = Senha::getPwd({arquivo=>$arqOut});
chomp($textoPlano);

if ($textoPlano eq $senha) {
  print "Senha criptografada com sucesso\n";
}
else {
  print "Houve alguma falha ao criptografar a senha\n";
}
