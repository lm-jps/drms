#!/usr/bin/perl
#
#Do a "cvs status" from the current dir and print out only the files 
#that are not "Up-to-date" and their Repository revision.
#Recursively goes down all the subdirs.
#
$PID = getppid;
$user = $ENV{'USER'};
$logfile = "/tmp/cvs_status_".$user."_$PID.log";
`cvs status 1> $logfile 2>&1`;
open(CV, $logfile) || die "Can't open $logfile: $!\n";

while(<CV>) {
  #print "$_";		#!!!TEMP
  if(/^File:/) {
    if(/Up-to-date/) {
      next;
    }
    print "$_";
    <CV>;
    <CV>;
    $_ = <CV>;	#get Repository revision line
    s/\s+/ /g;                  #compress multiple spaces
    print "$_\n\n";
  }
}
