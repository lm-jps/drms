#!/usr/bin/perl
#
#OLD: Called as a cron job on d02 (where SUMS runs).
#Calls tapearc to archive tapes to the T950.
#
#NEW: Take out of cronjob and run as script and keep on running tapearc.
#
#use DBI;

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

$abortfile = "/usr/local/logs/tapearc/TAPEARC_ABORT0";
$DB = "jsoc_sums";
$ENV{'PGPORT'} = "5434";        #jsoc_sums db uses non-default PGPORT

  if ( -e $abortfile ) {	# if file exist
   print "Found $abortfile\nImmediate abort of tape_do.pl\n";
   #`/bin/rm -f $abortfile`;
   exit;
  }

while(1) {			#keep it running for now
  $label = &labeldate;
  $logfile = "/usr/local/logs/tapearc/tape_do_0_".$label;
  $cmd = "/home/production/cvs/JSOC/bin/linux_ia64/tapearc0 -v jsoc_sums";
  $sleeptime = 60;
  print "$cmd\n";
  if(system "$cmd 1> $logfile 2>&1") {
    print "Error on: $cmd\n";
    print "See log: $logfile\n";
    print "Error is typically - no archive pending entries\n";
    $sleeptime = 300;		#sleep longer
    #exit(1);
  }
  if ( -e $abortfile ) {	# if file exist
   `/bin/rm -f $abortfile`;
   print "Found: $abortfile\n";
   last;
  }
  #sleep(3600);
  sleep($sleeptime);
  if ( -e $abortfile ) {	# if file exist
   `/bin/rm -f $abortfile`;
   print "Found: $abortfile\n";
   last;
  }
}

