package Senha;

use constant CHAVE => "1234567890ABCDEF01234567890ABCDEFGHIJKLMNOPQRTUVWXYZ";

sub makePwd {
  my $params = shift;
  my $senhaTxt = $params->{'senha'};
  my $arquivo  = $params->{'arquivo'};
  blowCrypt({senha=>$senhaTxt, chave=>CHAVE, arquivo=>$arquivo});
}

sub getPwd {
  my $params = shift;
  my $senhaEnc = $params->{'senha'};
  my $arquivo  = $params->{'arquivo'};
  return blowDecrypt({senha=>$senhaEnc, chave=>CHAVE, arquivo=>$arquivo});
}


# Chamada: blowCrypt({senha=>"SENHA", chave=>"CHAVE" [, arquivo=>"ARQ"]})
# Retorna $parametros->{'senha'} encriptado pela chave $parametros->{'chave'}
# Se arquivo estiver definido, grava a senha criptografada no mesmo.
sub blowCrypt {
  use Crypt::CBC;
  use MIME::Base64;
  my $parametros = shift;
  my $senha = $parametros->{'senha'};
  my $key   = $parametros->{'chave'};
  my $file  = $parametros->{'arquivo'};

  my $cipher = Crypt::CBC->new(
    -key    => "$chave",
    -cipher => 'Blowfish'
  );
  my $ciphertext = encode_base64($cipher->encrypt($senha));

  if(defined($file) && $file ne ""){
    open my $cipherfile, ">$file";
    print $cipherfile $ciphertext;
    close $cipherfile;
  }
  return $ciphertext;
};


# Chamada: blowDecrypt({senha=>"SENHA", chave=>"CHAVE" [, arquivo=>"ARQ"]})
# Retorna $parametros->{'senhaEnc'} descriptografado pela chave 
# $parametros->{'chave'}.
# Se arquivo estiver definido, obtem a senha criptografada do mesmo.
sub blowDecrypt {
  use Crypt::CBC;
  use MIME::Base64;
  my $parametros = shift;
  my $senhaEnc = $parametros->{'senha'};
  my $key      = $parametros->{'chave'};
  my $file     = $parametros->{'arquivo'};

  my $cipher = Crypt::CBC->new(
    -key    => "$chave",
    -cipher => 'Blowfish'
  );
  my $decrypted;

  if(defined($file) && $file ne ""){
    local $/ = undef;
    open (my $ciphertext,'<', $file) 
      or die "Could not open password file: $file.\n$!\n";
    $senhaEnc = <$ciphertext>;
  }
  $decrypted = $cipher->decrypt(decode_base64($senhaEnc));
  return $decrypted;
};

1;
