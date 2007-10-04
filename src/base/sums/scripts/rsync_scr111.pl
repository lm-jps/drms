#!/usr/bin/perl
#/home/jim/cvs/jsoc/scripts/rsync_scr111.pl
#
#Usage: rsync_scr111.pl [log_file]
#where log_file is an optional file to store the output log.
#      If none is given then the log goes to
#      /tmp/rsync_scr111_YYYY.MM.DD_HH:MM:SS.log
#
#rsync's dcs0:/home/jim/cvs/jsoc to xim:/scr111/dcs0_backup and to
#xim:/home/jim/dcs0_backup
#
#NOTE!! were the backup is place is determined in 
#/home/jim/cvs/jsoc/scripts/ssh_rsync.source and may not be /scr111.
#
#Typically this is run as a user jim cron job.
#

#Return date in form for a label e.g. 1998.01.07_14:42:04
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
  return($date);
}

$| = 1;			#flush output as we go
if($#ARGV == -1) {
  $label = &labeldate;
  $logfile = "/tmp/rsync_scr111_".$label;
}
else {
  $logfile = $ARGV[0];
}

#set up for ssh w/o password
#`source /home/jim/cvs/jsoc/scripts/ssh_setenv`;
$cmd = "source /home/jim/cvs/JSOC/src/base/sums/scripts/ssh_rsync.source";

if(system "$cmd 1> $logfile 2>&1") {
  print "Error on: $cmd\n";
  print "See log: $logfile\n";
  exit(1);
}

