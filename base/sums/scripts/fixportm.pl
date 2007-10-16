#!/usr/bin/perl
#
#Usage: fixportm.pl
#
#Probes the portmapper on the sum_svc machine and deletes any existing 
#registration for the RESPPROG which is defined below (originally in sum_rpc.h).
#
use Term::ReadKey;

$RESPPROG = 0x20000613;
$RESPDECIMALSTR = " 536872467";   #And notice leading space
$SUMSERVER = "d00.Stanford.EDU";  #defined in SUM.h

$| = 1;		#flush output as we go
$UID = $<;	#real user id
$outtmp = "/tmp/fixportm.$UID.log";
$HOST = $ENV{'HOST'};
if($HOST ne $SUMSERVER) {
  print "You must run this on the sum_svc machine = $SUMSERVER\n";
  exit;
}
$user = `whoami`;
chomp($user);
if($user ne "root") {
  print "You must be root to run\n";
  exit;
}
print "Make sure no sum_svc is running on $HOST!!\n";
print "Ok to proceed ('yes' to continue):";
$ans = ReadLine(0);
chomp($ans);
print "\n";
if($ans ne "yes") {
  print "Bye Bye\n";
  exit(1);
}
$cmd = "/usr/sbin/rpcinfo -p";
if(system "$cmd 1> $outtmp 2>&1") {
  print "Failed: $cmd\n";
  exit(1);
}
print "Portmapper assignments in $outtmp\n";
open(LOG, $outtmp) || die "Can't open $outtmp: $!\n";
while(<LOG>) {
  if(/^$RESPDECIMALSTR/) {
    #print "Found: $_";
    $version = substr($_, 10, 6);
    $cmd = "/usr/sbin/rpcinfo -d $RESPDECIMALSTR $version";
    print "$cmd\n";
    if(system($cmd)) {
      print "Failed: $cmd\n";
    }
  }
}
