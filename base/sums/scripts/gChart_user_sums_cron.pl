#!/usr/bin/perl
#gChart_user_sums_cron.pl
#Normally a cron job on k1:
#55 3 * * * /home/production/cvs/JSOC/base/sums/scripts/gChart_user_sums_cron.pl

#Return the current date in form for a label e.g. 2012.06.20
sub labeldate {
  local($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst,$date,$sec2,$min2,$hour2,$mday2,$year2);
  ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
  $sec2 = sprintf("%02d", $sec);
  $min2 = sprintf("%02d", $min);
  $hour2 = sprintf("%02d", $hour);
  $mday2 = sprintf("%02d", $mday);
  $mon2 = sprintf("%02d", $mon+1);
  $year4 = sprintf("%04d", $year+1900);
  $date = $year4.".".$mon2.".".$mday2;
  return($date);
}

$ENV{'PATH'} = "/home/production/cvs/JSOC/base/sums/scripts:/home/jsoc/bin/linux_avx:$ENV{'PATH'}";
$path = $ENV{'PATH'};
$ENV{'JSOC_MACHINE'} = "linux_avx";  #cron job run on k1

$datearg = &labeldate();
$cmd = "time_index -d in=$datearg";
$dnum = `$cmd`;
$dnum--;
$cmd = "time_index -t day=$dnum";
$yesterday = `$cmd`;
$ydate = substr($yesterday, 0, 10);
print "ydate=$ydate\n";

$cmd = "gChart_user_sums.pl $ydate";
print "$cmd\n";
@x = `$cmd`;
print "@x\n";

