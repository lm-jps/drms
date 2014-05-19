#!/usr/bin/perl
#/home/production/cvs/JSOC/scripts/sum_rm_stop dbname (e.g. jsoc_sums)
#Send a USR1 alarm to sum_rm_0, sum_rm_1, sum_rm_2.
#This is normally run when the production user wants to stop 
#the sum_rm processes.
#
$| = 1;                 #flush output as we go
if($#ARGV != 0) {
  print "Usage: $0 dbname\n";
  exit(1);
}
$DBIN = $ARGV[0];
$sumserver = "k1";
$host = `hostname -s`;
chomp($host);
if($host ne $sumserver) {
  print "This can only be run on $sumserver.\n";
  exit;
}
$sumsmanager = "production";
$user = $ENV{'USER'};
if($user ne $sumsmanager) {
  print "You must be user $sumsmanager to run\n";
  exit;
}

($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
$sec2 = sprintf("%02d", $sec);
$min2 = sprintf("%02d", $min);
$hour2 = sprintf("%02d", $hour);
$mday2 = sprintf("%02d", $mday);
$mon2 = sprintf("%02d", $mon+1);
$year2 = sprintf("%02d", $year);
$date = (1900 + $year).".".$mon2.".$mday2"._.$hour2.":$min2".":$sec2";
print "\nsum_rm_stop at $date\n\n";

  @ps_prod = `ps -ef`;
for($i = 0; $i < 3; $i++) {
  $p = "sum_rm_$i $DBIN";
  if(@line = grep(/$p/, @ps_prod)) {
      $x = shift(@line);
      ($a, $psid, $c) = split(/\s+/, $x);
      print "Killing $p with kill -USR1 $psid\n";
      `kill -USR1 $psid`;
  }
}
