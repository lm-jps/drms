eval 'exec /home/jsoc/bin/$JSOC_MACHINE/perl -S $0 "$@"'
    if 0;
#/home/production/cvs/JSOC/base/sums/scripts/t120stageall.pl
#
#Will keep 4 versions of t120_reachive.pl running.
#Will use the input file for the list of tape#s to call t120_reachive.pl
#for.
#
use Term::ReadKey;
use FindBin qw($RealBin);
use lib "$RealBin/../../../localization";
use drmsparams;

sub usage {
  print "Keep 3 versions of t120_reachive.pl running\n";
  print "Usage: t120stageall.pl tapelist_file\n";
  exit(1);
}

#Return date in form for a label e.g. 1998.01.07_14:42:04
#Also set effective_date in form 199801071442
sub labeldate {
  local($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst,$date,$sec2,$min2,$hour2,$mday2,$year2);
  ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
  $sec2 = sprintf("%02d", $sec);
  $min2 = sprintf("%02d", $min);
  $hour2 = sprintf("%02d", $hour);
  $mday2 = sprintf("%02d", $mday);
  $mon2 = sprintf("%02d", $mon+1);
  $year4 = sprintf("%04d", $year+1900);
  $date = $year4.".".$mon2.".".$mday2._.$hour2.":".$min2.":".$sec2;
  $effdate = $year4.$mon2.$mday2.$hour2.$min2;
  return($date);
}

$HOSTDB = drmsparams::SUMS_DB_HOST;      #host where DB runs
$DB =  drmsparams::DBNAME;
while ($ARGV[0] =~ /^-/) {
  $_ = shift;
  if (/^-h(.*)/) {
    $HOSTDB = $1;
  }
  if (/^-d(.*)/) {
    $DB = $1;
  }
}

if($#ARGV != 0) {
  &usage;
}
$tapeidfile = $ARGV[0];

$ldate = &labeldate();
print "t120stageall.pl for tapeidfile=$tapeidfile $ldate\n";
$hostdb = $HOSTDB;      #host where Postgres runs
$user = $ENV{'USER'};
#$ENV{'SUMSERVER'} = "d02.Stanford.EDU";
if($user ne "production") {
  print "You must be user production to run\n";
  exit;
}
print "Need hmi password to run: passwd =";
ReadMode('noecho');
$passwd = ReadLine(0);
chomp($passwd);
ReadMode('normal');
print "\n";
#if($passwd ne <passwd>) {
#  print "Invalid passwd\n";
#  exit(1);
#}
open(ID, $tapeidfile) || die "Can't open $tapeidfile: $!\n";
while(<ID>) {
  if(/^#/ || /^\n/) { #ignore any comment or blank lines
    next;
  }
  print "$_";
  chomp($_);
  push(@idlist, $_);
}
close(ID);
while(1) {
  $found = 0;
  @ps = `ps -ef | grep t120_reachive`;
  #print "@ps\n";
  while($psone = shift(@ps)) {
    #print "$psone\n";
    if($psone =~ /t120_reachive.pl/) {
      $found++;
    }
  }
  print "found $found instances running\n";
  if($found < 7) {
    if(!($tid = shift(@idlist))) {
      print "###All done with list of input tapes\n";
      exit;
    }
    chomp($tid);
    ($tapen, $filen) = split(/\s/, $tid);
    $cmd = "t120_reachive.pl $tapen $filen 1> /usr/local/logs/t120_rearchive/$tapen.log 2>&1";
    print "$cmd\n";
    if($fpid = fork) {
      #This is the parent. The child's pid is in $fpid
      print stdout "pid is $fpid\n";
      #wait;
    } elsif (defined $fpid) {     # $fpid is zero here if defined
      exec $cmd;                  # run pe
      exit;                               # never fall through
    } else {
      #fork error
      print LOG "!!! Can't fork a pe: $!\n";
    }
  }
  else {
    sleep(30);
  }
}
