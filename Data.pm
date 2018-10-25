package Data;
use Time::Local;

# Retorna a data (string "DD/MM/AAAA") de <data atual> + dias (parametro)
# data(0)  = hoje
# data(-1) = ontem
# data(1)  = amanha
sub data {
  my $dias = shift;
  my $formato = shift;
  $formato = "YYYY-MM-DD HH24:MI" unless defined $formato;
  my $segundos = $dias*86400;
  my ($aux,$aux,$aux,$dia,$mes,$ano,$aux,$aux,$aux) = localtime;
  my $lt_dia = timelocal(0, 0, 0, $dia, $mes, $ano);
  ($aux,$aux,$aux,$dia,$mes,$ano,$aux,$aux,$aux) = localtime($lt_dia+$segundos);
  $ano += 1900;
  $mes += 1;
  if ($formato eq "YYYY-MM-DD HH24:MI") {
    return sprintf("%.2d%.2d%.4d", $dia,$mes,$ano);
  }
  else {
    die("Formato especificado eh invalido");
  }
};

# Retorna o timestamp equivalente a data de "hoje" (meia-noite).
# Se worktime for definido, retorna "hoje" a partir das 7:30.
sub hoje_timestamp {
  my $worktime = shift;
  my ($aux,$aux,$aux,$dia,$mes,$ano,$aux,$aux,$aux) = localtime;
  return ($worktime) ? 
    timelocal(0, 30, 7, $dia, $mes, $ano) : 
    timelocal(0, 0, 0, $dia, $mes, $ano);
}


sub validaData {
  my $dia = shift;
  my $mes = shift;
  my $ano = shift;
  my $hora = shift;
  my $min = shift;
  my $ts = timelocal(0, $min, $hora, $dia, $mes-1, $ano);
  # Se a hora for invalida, o timelocal da um erro e nao continua
};

sub defineFormato {
  my $data = shift;
  my $formato;
  if ($data =~ /(\d{2})(\d{2})(\d{4})-(\d{2})(\d{2})/) {
    validaData($1, $2, $3, $4, $5);
    $formato = "DDMMYYYY-HH24MI";
  }
  elsif ($data =~ /(\d{2})-(\d{2})-(\d{4})/) {
    validaData($1, $2, $3);
    $formato = "DD-MM-YYYY";
  }
  elsif ($data =~ /(\d{2})(\d{2})(\d{4})/) {
    validaData($1, $2, $3);
    $formato = "DDMMYYYY";
  }
  else {
    die("Formato invalido para data. Usar DDMMAAAA[-HHmm] ou DD-MM-YYYY");
  }
  return $formato;
};


sub dataPadrao {
  my $data = shift;
  my $dataPadrao;
  if ($data =~ /(\d{2})(\d{2})(\d{4})-(\d{2})(\d{2})/) { # "DDMMYYYY-HH24MI"
    validaData($1, $2, $3, $4, $5);
    $dataPadrao = "$3-$2-$1 $4:$5";
  }
  elsif ($data =~ /(\d{2})-(\d{2})-(\d{4})/) {           # "DD-MM-YYYY"
    validaData($1, $2, $3);
    $dataPadrao = "$3-$2-$1 00:00";
  }
  elsif ($data =~ /(\d{2})(\d{2})(\d{4})/) {             # "DDMMYYYY"
    validaData($1, $2, $3);
    $dataPadrao = "$3-$2-$1 00:00";
  }
  else {
    die("Formato invalido para data. Usar DDMMAAAA[-HHmm] ou DD-MM-YYYY");
  }
  return ($dataPadrao, "YYYY-MM-DD HH24:MI");
};

1;
