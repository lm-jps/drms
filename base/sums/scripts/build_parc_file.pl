#!/usr/bin/perl
#below has problem when run in cron job
#eval 'exec /home/jsoc/bin/$JSOC_MACHINE/perl -S $0 "$@"'
#    if 0;
#
#/home/production/cvs/JSOC/base/sums/scripts/build_parc_file.pl
#
#This make a .parc file to be sent from the pipeline system to a
#datacapture system. A .parc file tells the dc system what tlm
#data has been successfully archive on the pipeline system so that the
#dc system can put this info in its sum_main DB table as the safe tape
#info to indicate where on the pipeline system the tlm file has been
#saved to tape.
#
#This is typically run on the pipeline system as a cron job after midnight. 
#It wil query the sum_main table to find all the archived info for the
#.tlm data since the 24 hour period prior to the time of this script
#running. (e.g. if run after midnight on Wed, will find all the data 
#archived since midnight of Tues).
#
#The .parc file built looks like:
##Created by build_parc_file.pl for query past 2008-08-10 23:58:01
##owning_series_name                         tape_id  fn  date
#VC10_2008_218_01_56_51_00006d754c8_1c298_00 012936L4 305 2008-08-11 00:06:02
#VC13_2008_218_01_57_33_00006d91760_1c298_00 012936L4 312 2008-08-11 00:54:04
#VC10_2008_218_01_57_55_00006d91760_1c298_00 012936L4 313 2008-08-11 00:54:22
#
#NOTE: aia is always assumed to be on dsc0, and hmi on dcs1
#
use DBI;

sub usage {
  print "Make a .parc file of archive info to send to a datacapture host.\n";
  print "Usage: build_parc_file.pl [-h] [-thrs] \n";
  print "       -h = help\n";
#  print "       -d = datacapture host to send the parc file to\n";
  print "	-t = # of hours+2mins previous to current time to find data\n";
  print "            The default is 24hrs+2mins previous to find data\n";
#  print "       The default dc_host is $HOSTDC\n";
  exit(1);
}

#Return date in form e.g. 2008-05-29 11:38:18
#where this value is $hrsprev+120sec previous to the current time.
#NOTE: this format must match that in the jsoc_sums DB
#Also sets $currdate to current time of form 2008-05-29_11:38:18
sub labeldate {
  local($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst,$date,$sec2,$min2,$hour2,$mday2,$year2);
  $secprev = ($hrsprev*3600) + 120;	#add extra 2 mins
  ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time-$secprev);
  $sec2 = sprintf("%02d", $sec);
  $min2 = sprintf("%02d", $min);
  $hour2 = sprintf("%02d", $hour);
  $mday2 = sprintf("%02d", $mday);
  $mon2 = sprintf("%02d", $mon+1);
  $year4 = sprintf("%04d", $year+1900);
  $date = $year4."-".$mon2."-".$mday2." ".$hour2.":".$min2.":".$sec2;
  ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
  $sec2 = sprintf("%02d", $sec);
  $min2 = sprintf("%02d", $min);
  $hour2 = sprintf("%02d", $hour);
  $mday2 = sprintf("%02d", $mday);
  $mon2 = sprintf("%02d", $mon+1);
  $year4 = sprintf("%04d", $year+1900);
  $currdate = $year4."-".$mon2."-".$mday2."_".$hour2.":".$min2.":".$sec2;
  return($date);
}

$HOSTDB = "hmidb";	#db machine
$HOSTDC = "dcs0";	#default datacapture to send to
$TLMDSHMI = "hmi.tlme"; #series of .tlm files to query
$TLMDSAIA = "aia.tlme"; #series of .tlm files to query
$DB = "jsoc_sums";
$PGPORT = 5434;
$PARC_ROOT = "/usr/local/logs/parc/";
#$SSH_INFO = "/home/production/cvs/JSOC/base/sums/scripts/pgaginfo";
$SSH_INFO = "/tmp/ssh-agent.env";
$hrsprev = 24;
while ($ARGV[0] =~ /^-/) {
  $_ = shift;
  if (/^-h(.*)/) {
    &usage;
    exit(0);
  }
  if (/^-d(.*)/) {
    $HOSTDC = $1;
  }
  if (/^-t(.*)/) {
    $hrsprev = $1;
  }
}

@sshinfo = `cat $SSH_INFO`;
$map = "/tmp/build_source";
open(SR, ">$map") || die "Can't open $map: $!\n";
$x = shift(@sshinfo);
($a, $b, $ssh_auth_sock) = split(/ /, $x);
chomp($ssh_auth_sock);
print SR "SSH_AUTH_SOCK=$ssh_auth_sock export SSH_AUTH_SOCK\n";
$x = shift(@sshinfo);
($a, $b, $ssh_agent_pid) = split(/ /, $x);
chomp($ssh_agent_pid);
print SR "SSH_AGENT_PID=$ssh_agent_pid export SSH_AGENT_PID\n";

$ldate = &labeldate();
$parchmi = $PARC_ROOT."HMI_$currdate".".parc";
$parcaia = $PARC_ROOT."AIA_$currdate".".parc";
print "Going to find tlm data archived since $ldate\n";
print "NOTE: uses for ssh: $SSH_INFO\n";

$hostdb = $HOSTDB;      #host where Postgres runs
$user = $ENV{'USER'};
#$ENV{'SUMSERVER'} = "d02.Stanford.EDU";
if($user ne "production") {
  print "You must be user production to run\n";
  exit;
}

#First connect to database
  $dbh = DBI->connect("dbi:Pg:dbname=$DB;host=$hostdb;port=$PGPORT", "$user", "$password");
  if ( !defined $dbh ) {
    die "Cannot do \$dbh->connect: $DBI::errstr\n";
  }

for($i = 0; $i < 2; $i++) { 	#do first for HMI then AIA
  if($i == 0) {
    open(PARC, ">$parchmi") || die "Can't open $parchmi: $!\n";
    $resultfile = $parchmi; $instru = "HMI";
    $sql = "select online_loc, arch_tape, arch_tape_fn, arch_tape_date from sum_main where Arch_Tape_Date >= '$ldate' and Owning_Series = '$TLMDSHMI'";
  } else {
    open(PARC, ">$parcaia") || die "Can't open $parcaia: $!\n";
    $resultfile = $parcaia; $instru = "AIA";
    $sql = "select online_loc, arch_tape, arch_tape_fn, arch_tape_date from sum_main where Arch_Tape_Date >= '$ldate' and Owning_Series = '$TLMDSAIA'";
  }
    print PARC "#Created by build_parc_file.pl for query past $ldate\n";
    print PARC "#owning_series_name                         tape_id  fn  date\n";

    $sth = $dbh->prepare($sql);
    if ( !defined $sth ) {
      print "Cannot prepare statement: $DBI::errstr\n";
      $dbh->disconnect();
      exit; 
    }
    # Execute the statement at the database level
    $sth->execute;
    # Fetch the rows back from the SELECT statement
    @row = ();
    $count = 0;
    while ( @row = $sth->fetchrow() ) {
      $dir = shift(@row);
      if(!opendir(DIR, "$dir/S00000")) {
        print "Can't open $dir\n";
        next;
      }
      @dirfiles = readdir(DIR);
      closedir(DIR);
      $found = 0;
      while($file = shift(@dirfiles)) {
        if(($pos = rindex($file, ".tlm")) != -1) { 
          $found = 1;
          $count++;
          last;
        }
      }
      if(!$found) { next; }
      $pos = rindex($file, '.');
      $fftlm = substr($file, 0, $pos);
      print PARC "$fftlm @row\n";
    }
  close(PARC);
  print "\nDone: Found $count tlm files for $instru\n";
  print "Results in $resultfile\n";
  #now copy the resulting .parc file to the proper dcs machine
  if($i == 0) {		#hmi
    $cmd = "scp $parchmi dcs1:/dds/pipe2soc/hmi";
    print SR "$cmd\n";
  }
  else {		#aia
    $cmd = "scp $parcaia dcs0:/dds/pipe2soc/aia";
    print SR "$cmd\n";
  }
  print "NOTE: if this asks for password, you don't have ssh-agent set up\n";
  print "$cmd\n";
  $logfile = "/tmp/build_parc_file.log";
  $cmd = "source $map";
  if(system "$cmd 1> $logfile 2>&1") {
    print "Error on: $cmd\n";
    print "See log: $logfile\n";
  }
}
$dbh->disconnect();

