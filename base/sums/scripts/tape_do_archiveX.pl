eval 'exec /home/jsoc/bin/linux_ia64/perl -S $0 "$@"'
    if 0;
#/home/production/cvs/JSOC/base/sums/scripts/tape_do_archiveX.pl
#
#
use DBI;
use POSIX ":sys_wait_h";

$MAXFORKS = 4;		#max tapearcX forks (i.e. limit of drives to write)
$CURRFORKS = 0;
$nochild = 0;

sub usage {
  print "Do pending archive by group by dataset by sunum.\n";
  print "Usage: tape_do_archive.pl -x db_name (e.g. jsoc_sums)\n";
  print "       -x = execute, else just show what would do\n";
  print "       The default db_host is $HOSTDB\n";
  print "\nYou must have ENV SUMPGPORT set to the port number, e.g. 5434\n";
  exit(1);
}

#Return date in form for a label e.g. 1998.01.07_14:42:04
#Also set effective_date ($effdate) in form 199801071442
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

#Return an effective_date in form 199801071442
#Arg has how many sec to add to current time to calculate eff date
sub reteffdate {
  my($xsec) = @_;
  local($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst,$date,$sec2,$min2,$hour2,$mday2,$year2, $esec, $effdate);
  $esec = time + $xsec;
  ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($esec);
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

sub commify {
                  local $_  = shift;
                  1 while s/^([-+]?\d+)(\d{3})/$1,$2/;
                  return $_;
             }

#Get signal when a child (tapearcX) terminates.
#Start another if more manifest files to do.
#!!!NOTE: This has been depricated and is no longer used.
sub REAPCHILD {
  my $pid;

    $pid = waitpid(-1, WNOHANG);		#any pid, don't block
    if($pid == -1) {
      #ignore no child
      $nochild = 1;
    } elsif (WIFEXITED($?)) {
      $nochild = 0;
      print "Process $pid exited\n";
      $CURRFORKS--;
      if($newcmd = shift(@pendcmds)) {
          print "REAPCHILD CURRFORKS=$CURRFORKS has newcmd = $newcmd\n";
          if(!(&forkarc($newcmd))) {
            print "Fork Error!! There s/b an open drive for write.\n";
            exit(1);
          }
      }
      else {
        if($CURRFORKS <= 0) {
          print "All tapearcX done. Exiting.\n";
          exit(0);
        }
      }
    } else {
      print "False alarm on $pid\n";
    }
  $SIG{CHLD} = \&REAPCHILD;		#old unreliable signals protection
}

#Fork another tapearcX with its manifest file. Return 0 if reached max forks.
sub forkarc {
  my($fcmd) = @_;
  print "$fcmd\n";
  if($CURRFORKS >= $MAXFORKS) { return 0; } #can't fork any more
  $CURRFORKS++;
  if($execute) {
    if($fpid = fork) {
      #This is the parent. The child's pid is in $fpid
      print stdout "child pid is $fpid. CURRFORKS = $CURRFORKS\n";
      return 1;
      #wait;
    } elsif (defined $fpid) {     # $fpid is zero here if defined
      exec $fcmd;                 # run taprarcX
      exit;                       # never fall through
    } else {
      #fork error
      print "!!! Fatal: Can't fork: $!\n";
      #return 0;
      exit;
    }
  }
  else {
    print "CURRFORKS = $CURRFORKS\nSkip: $fcmd\n";
  }
}

  $host = `hostname -s`;
  chomp($host);
  if($host ne "d02") {
    print "Error: This must be run on d02\n";
    exit;
  }

#$SIG{CHLD} = \&REAPCHILD; 	#This was old unreliable method
#$ARCHGRPCFG = "/home/production/cvs/JSOC/base/sums/apps/data/arch_group.cfg";
$ARCHPROBE = "/home/production/cvs/JSOC/proj/util/scripts/archprobe.pl";
$ARCHFILEDIR = "/usr/local/logs/manifest";  #!!!TEMP noop
#$ARCHFILEDIR = "/usr/local/logs/manifest_test"; #!!TEMP for testing
$HOSTDB = "hmidb";      #host where DB runs
$FAST = 0;
while ($ARGV[0] =~ /^-/) {
  $_ = shift;
  if (/^-h(.*)/) {
    $HOSTDB = $1;
  }
  if (/^-f(.*)/) {
    $FAST = 1;
  }
  if (/^-x(.*)/) {
    $execute = 1;
  }
}

if($#ARGV != 0) {
  &usage;
}
$DB = $ARGV[0];

$ldate = &labeldate();
print "tape_do_archive.pl run on $ldate\n";
@ps = `ps -ef | grep tape_do_archive.pl`;
$name = "tape_do_archive.pl $DB";
$x = grep(/$name/, @ps);
print "Number of tape_do_archive.pl running = $x\n";
if($x > 1) {
  print "A tape_do_archive.pl $DB is already running.\n";
  print "Only one allowed. Aborting...\n";
  exit(1);
}
$hostdb = $HOSTDB;      #host where Postgres runs
$user = $ENV{'USER'};
$ENV{'SUMPGPORT'} = 5434;
if(!($PGPORT = $ENV{'SUMPGPORT'})) {
  print "You must have ENV SUMPGPORT set to the port number, e.g. 5434\n";
  exit;
}

#First connect to database
  $dbh = DBI->connect("dbi:Pg:dbname=$DB;host=$hostdb;port=$PGPORT", "$user", "");
  if ( !defined $dbh ) {
    die "Cannot do \$dbh->connect: $DBI::errstr\n";
  }
  $sql = "select * from sum_arch_group";
  print "$sql\n";
  $sth = $dbh->prepare($sql);
  if ( !defined $sth ) {
    print "Cannot prepare statement: $DBI::errstr\n";
    $dbh->disconnect();
    exit;
  }
  $sth->execute;
  $i = 0;
  while ( @row = $sth->fetchrow() ) {	#do for all groups
    $group = shift(@row);
    $waitdays = shift(@row);
    $sec1970 = shift(@row);
    $date = shift(@row);
    $mfilename = "$ARCHFILEDIR/manifest.group$group";
    if( -e $mfilename) {	#already exists
      print "Manifest file $mfilename still exists.\n";
      @ps = `ps -ef | grep tapearcX`;
      $name = "manifest.group$group\n"; #need \n to not get dups
      $x = grep(/$name/, @ps);
      if($x) {
        print "Processing for group $group still active. Skip...\n"; 
        next;
      }
      else {
        print "Going to recreate $mfilename if necessary\n";
        $cmd = "/bin/rm $mfilename";
        `$cmd`;
      }
    } 
    #determine if time to archive is past cadence
    if($waitdays == 0) { next; }	#no archive for this group
    $secnow = time;
    $deltasec = $secnow - $sec1970;
    $waitsec = $waitdays * 86400;
    if($deltasec < $waitsec) {
      next;
    }
    #Ok, archive this group. Update sec1970_start to now
    $ldate = &labeldate();
    $sql = "update sum_arch_group set sec1970_start=$secnow, date_arch_start='$ldate' where group_id=$group";
      $sth2 = $dbh->prepare($sql);
      if ( !defined $sth2 ) {
        print "Cannot prepare statement: $DBI::errstr\n";
        $dbh->disconnect();
        exit;
      }
      $sth2->execute;

  $GBtotal = 0;
  print "\ngroup = $group\n";
  @fileinfo = ();
  @dsdata = `$ARCHPROBE jsoc_sums hmidb 5434 production agg group ap ' ' $group 1`;
  print "@dsdata\n";
  while($dat = shift(@dsdata)) {
    if(/^#/ || /^\n/) { #ignore any comment or blank lines
      next;
    }
    ($datgrp, $series, $dpnow, $dplt100, $dpgt100, $ap) = split(/\s+/, $dat);
    $GBtotal += $ap;
  }
  print "Total GB for group $group to archive = $GBtotal\n";
    #now get the details by order of sunum
    $cmd = "$ARCHPROBE jsoc_sums hmidb 5434 production raw group ap ' ' $group 1";
    @datsunum = `$cmd`;
    $seriessave = "NONE";
    print "raw group $group:\n";
    print "@datsunum\n";
    #A line looks like
#group series sunum sudir AP (bytes) sumid eff_date
#6 hmi.hmiserieslev1pb45 153752265 /SUM1/D153752265 7763370790 62825881 201107041532
    while($_ = shift(@datsunum)) {
      #print "$_";
      if(/^#/ || /^\n/) { #ignore any comment or blank lines
        next;
      }
      #make an array for each seperate series
      chomp();
      ($grp,$ser,$sunum,$sudir,$bytes,$sumid,$edate) = split(/\s+/);
      if($seriessave eq "NONE") {
        $seriessave = $ser;
        $sarrayname = "S_$ser";
        $sarrayname =~ s/\.//;		#elim any '.' before eval
        push(@sarrayarray, $sarrayname);
      }
      if($seriessave eq $ser) {     #continue on same series
        $cmd = "push(\@$sarrayname, \"$_\")";
        eval($cmd);
      }
      else {			    #start a new series
          $seriessave = $ser;
        $sarrayname = "S_$ser";
        $sarrayname =~ s/\.//;          #elim any '.' before eval
        push(@sarrayarray, $sarrayname);
        $cmd = "push(\@$sarrayname, \"$_\")";
        eval($cmd);
      }
    }
  }

#Now process the arrays for each ds name and make a manifest file for
#each group. The ds name arrays are in group# order in @sarrayarray.
$savegrp = -1;
$filename = 0;			#full path manifest file name
@pendcmds = ();			#cmd to run after a child completes
@filecalled = ();		#manifest file names already called for execution
while($x = shift(@sarrayarray)) {
  print "$x\n";
  $cmd = "\$_ = shift(\@$x)";
  eval($cmd);
  while($_) {
    print "$_\n";
    ($grp,$ser,$sunum,$sudir,$bytes,$sumid,$edate) = split(/\s+/);
    if($grp != $savegrp) {
      $savegrp = $grp;
      if($filename) { 
        close(AR);
        print "Close $filename\n";
        $cmd = "/home/production/cvs/JSOC/bin/linux_ia64/tapearcX $filename";
        if(!(&forkarc($cmd))) {
          print "Exceeded max forks of $MAXFORKS. Will process later.\n";
          push(@pendcmds, $cmd);
        }
        else {
          push(@filecalled, $filename); #man file name (full path) in execution
        }
      }
      $filename = "$ARCHFILEDIR/manifest.group$grp";
      open(AR, ">$filename") || die "Can't open $filename: $!\n";
      print AR "#group=$grp Only one group# per manifest file\n";
      print AR "#group series sunum sudir AP (bytes) sumid eff_date\n";
      $btotal = 0; $ttotal = 0;
      $j = 0;
      $numsudir = 0; 
      @fileinfo = ();
    }
    else {
      if($ser ne $saveser) {
      }
    }
    #accumulate 500MB or 512 sudir for a tape file and put in manifest
    $btotal += $bytes;
    push(@fileinfo, "$_\n");
    $numsudir++;
      if(($btotal >= 536870912) || ($numsudir >= 512)) { #500MB or 512 sunum
        print AR "#File $j:\n";
        $info = sprintf("#Total bytes in file = %12s. Num sudir = $numsudir\n",
                        commify(int($btotal)));
        print AR "$info";
        if(!@fileinfo) { 
          print AR "\n";		#tapearcX requires
        } else {
          print AR " @fileinfo";
        }
        $ttotal += $btotal;
        if($ttotal >= 760000000000) { #!!TBD use cfg info
          #print AR "EOT:\n";	#NO, causes tapearcX problem
          $ttotal = 0;
        }
        $btotal = 0;
        $numsudir = 0;
        @fileinfo = ();
        $j++;
      }
    $cmd = "\$_ = shift(\@$x)";
    eval($cmd);
  }
  #have a new ds name. print out the last tape file for the prev ds name
    print AR "#File $j:\n";
    $info = sprintf("#Total bytes in file = %12s. Num sudir = $numsudir\n",
                        commify(int($btotal)));
    print AR "$info";
    if(!@fileinfo) { 
      print AR "\n";		#tapearcX requires
    } else {
      print AR " @fileinfo";
    }
    $ttotal += $btotal;
    if($ttotal >= 760000000000) { #!!TBD use cfg info
      #print AR "EOT:\n";	#NO, causes tapearcX problem
      $ttotal = 0;
    }
    $btotal = 0;
    $numsudir = 0;
    @fileinfo = ();
    $j++;
}
if($filename) { 
  close(AR);
  print "Close $filename\n";
  $cmd = "/home/production/cvs/JSOC/bin/linux_ia64/tapearcX $filename";
  if(!(&forkarc($cmd))) {
    print "Exceeded max forks of $MAXFORKS\n";
    push(@pendcmds, $cmd);
  }
  else {
    push(@filecalled, $filename); #man file name in execution
  }
}

#Old stuff below. This was unreliable to reap the $SIG{CHLD}
#while(1) { 
#  sleep 10; 
#  if($nochild) {        #sometime last child is not reaped. so check this
#      if($newcmd = shift(@pendcmds)) {
#          print "CURRFORKS=$CURRFORKS has newcmd = $newcmd\n";
#          if(!(&forkarc($newcmd))) {
#            print "Fork Error!! There s/b an open drive for write.\n";
#            exit(1);
#          }
#          next;
#      }
#    print "tape_do_archive.pl No child left. Exit\n";
#    exit(0);
#  }
#}

#NOTE: the tapearcX removes its manifest file when done
while(1) {
  if($#pendcmds == -1) { last; }          #nothing pending. just go away 
  sleep 120;
  $cmd = "/bin/ls $ARCHFILEDIR/man*";
  @lsman = `$cmd`;
  #find any entry in @filecalled not in @lsman, i.e. the file has completed
  %seen = ();
  @fconly = ();
  @falive = ();
  while($item = shift(@lsman)) {	#make lookup table
    chomp($item);
    $seen{$item} = 1;
  }
  foreach $item (@filecalled) {
    unless ($seen{$item}) {
      push(@fconly, $item);		#item not in @lsman
      $CURRFORKS--;

    }
    else {
      push(@falive, $item);		#item still executing
    }
  }
  while($item = shift(@fconly)) {	#an item completed. start new cmd
    if($cmd = pop(@pendcmds)) {
      ($a, $entry) = split(/\s+/, $cmd);
      push(@falive, $entry);
      if(!(&forkarc($cmd))) {
        print "Fatal Error: There s/b a free drive\n";
        exit;
      }
    }
  }
  @filecalled = @falive;
  print "These jobs still active:\n";
  while($x = shift(@falive)) {
    print "$x\n";
  }
}
print "Normal exit with these jobs still active:\n";
while($x = shift(@filecalled)) {
  print "$x\n";
}

