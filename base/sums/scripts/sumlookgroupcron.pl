#!/usr/bin/perl
#
#Calls the sumlookgroupX.pl to make the storage statistics and
#copies it to /home/jsoc/public_html/SUM/sumlookgroup.html
#
#Normally started by production on n02:
#05 5 * * 1 /home/production/cvs/JSOC/base/sums/scripts/sumlookgroupcron.pl

#
#Return date in form 199801071442
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
  return($effdate);
}

$ENV{'JSOC_MACHINE'} = "linux_x86_64";
$ENV{'SUMPGPORT'} = 5434;
$ldate = &labeldate;
$cmd = "cp -p /home/jsoc/public_html/SUM/sumlookgroup.html /home/jsoc/public_html/SUM/sumlookgroup.$ldate.html";
`$cmd`;
$cmd = "/home/production/cvs/JSOC/base/sums/scripts/sumlookgroupX.pl -f jsoc_sums >& /tmp/look.sum";
`$cmd`;
`cp /tmp/look.sum /home/jsoc/public_html/SUM/sumlookgroup.html`;

