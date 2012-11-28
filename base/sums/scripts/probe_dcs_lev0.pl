#!/usr/bin/perl
#below has problem when run in cron job
#eval 'exec /home/jsoc/bin/$JSOC_MACHINE/perl -S  $0 "$@"'
#    if 0;
#This is an experimental program to find out the various things
#to probe to determine that the datacapture systems are
#performing all their functions correctly, and that the lev0
#data is being generated ok on the backend pipeline.
#
#d02 runs this script every midnight:
#/home/production/cvs/JSOC/base/sums/scripts/build_parc_file.pl
#This make a .parc file in /usr/local/logs/parc to be sent from the 
#pipeline system to a datacapture system. 
#A .parc file tells the dc system what tlm
#data has been successfully archive on the pipeline system so that the
#dc system can put this info in its sum_main DB table as the safe tape
#info to indicate where on the pipeline system the tlm file has been
#saved to tape.
#The .parc files made are sent to:
#  dcs0:/dds/pipe2soc/aia     and
#  dcs1:/dds/pipe2soc/hmi
#The .parc file are ingested by ingest_tlm running on dcs0,1.
#The datacapture system sum_main table is updated for its safe_tape info 
#from the .parc and the .parc file is deleted. The .parc file on
#d02:/usr/local/logs/parc is retained.

$RUNHOST = "d02.Stanford.EDU";
#$RUNHOST = "j0.Stanford.EDU";  #!!TEMP
$shost = "d02";
$SSHFILE = "/var/tmp/ssh-agent.env";
$sshfilesource = "/var/tmp/ssh-agent.env.source";
$PID = getppid;
$MLOGFILE = "/tmp/probe_dcs_lev0.mail.$PID";
$mailflg = 0;
$dc0 = "dcs0";
$dc1 = "dcs1";
$nodelev0 = "cl1n001";
$nodelev1 = "cl1n002";

#$host = $ENV{'HOST'};
$host = `hostname`;
chomp($host);
if($host ne $RUNHOST) {
  print "Error: This must be run on $RUNHOST (host=$host)\n";
  exit;
}
open(MLOG, ">$MLOGFILE") || die "Can't open $MLOGFILE: $!\n";
print MLOG "cronjob run log in: /tmp/probe.out\n";
$date = &labeldate;
print "\nprobe_dcs_lev0.pl run on $fulldate\n\n";

#First check that ssh-agents are set up ok. Exit on any error.
&ck_ssh_stuff("$RUNHOST", 1);	#$RUNHOST must be first
&ck_ssh_stuff("$dc0", 0);
&ck_ssh_stuff("$dc1", 0);


#Make sure we have a new .parc file in d02:/usr/local/logs/parc for today.
#File name like: HMI_2010-01-29_00:00:01.parc
print "\n";
$hmiparc = `ls -t /usr/local/logs/parc/HMI*.parc | sed -n 1p`;
$aiaparc = `ls -t /usr/local/logs/parc/AIA*.parc | sed -n 1p`;
#print "hmiparc = $hmiparc\n";
#print "aiaparc = $aiaparc\n";
if($hmiparc =~ /$date/) {
  print "$shost HMI .parc file found for $date\n";
}
else {
  print "$shost No HMI .parc file found for $date\n";
  print MLOG "$shost No HMI .parc file found for $date\n";
  $mailflg = 1;
}
if($aiaparc =~ /$date/) {
  print "$shost AIA .parc file found for $date\n";
}
else {
  print "$shost No AIA .parc file found for $date\n";
  print MLOG "$shost No AIA .parc file found for $date\n";
  $mailflg = 1;
}

#Make sure we have a new .arc file created on dcs0 and dcs1 for today.
#File name like: HMI_2010_36_00_05.arc.touch
print "\n";
$aiaarctst = `source $sshfilesource; ssh $dc0 "ls -t /usr/local/logs/arc/AIA*.touch | sed -n 1p"`;
if($aiaarctst =~ /$ddoy/) {
  print "$dc0 AIA .arc file created for $date\n";
}
else {
  print "$dc0 No AIA .arc file created for $date ******\n";
  print MLOG "$dc0 No AIA .arc file created for $date ******\n";
  $mailflg = 1;
}
$hmiarctst = `source $sshfilesource; ssh $dc1 "ls -t /usr/local/logs/arc/HMI*.touch | sed -n 1p"`;
#print "!!!TEST:  $hmiarctst\n"; #!!TEMP
if($hmiarctst =~ /$ddoy/) {
  print "$dc1 HMI .arc file created for $date\n";
}
else {
  print "$dc1 No HMI .arc file created for $date ******\n";
  print MLOG "$dc1 No HMI .arc file created for $date ******\n";
  $mailflg = 1;
}

#Find last tapearc file on this host
print "\n";
#$lastt = `ls -lt /usr/local/logs/tapearc/tape_do* | sed -n 1p`;
#chomp($lastt);
#print "Last $RUNHOST tape archive log is:\n$lastt\n"; #!!!TEMP
$tdir = "/usr/local/logs/tapearc/";
$lastt0 = `find $tdir -name "tape_do_0_*" | tail -1`;
$lastt1 = `find $tdir -name "tape_do_1_*" | tail -1`;
$lastt2 = `find $tdir -name "tape_do_2_*" | tail -1`;
print "Last $RUNHOST tape archive log 0 is:\n$dir$lastt0\n";
print "Last $RUNHOST tape archive log 1 is:\n$dir$lastt1\n";
print "Last $RUNHOST tape archive log 2 is:\n$dir$lastt2\n\n";

$lastt = `source $sshfilesource; ssh $dc0 "ls -lt /tmp/tapearc*.log"`;
chomp($lastt);
print "Last $dc0 tape archive log is:\n$lastt\n"; #!!!TEMP

$lastt = `source $sshfilesource; ssh $dc1 "ls -lt /tmp/tapearc*.log"`;
chomp($lastt);
print "Last $dc1 tape archive log is:\n$lastt\n"; #!!!TEMP

#Now check all the crontab entries
$found = 1;
@ctabclean = ();
print "\nNow check on crontab entries for dcs0:\n";
@ctab = `source $sshfilesource; ssh dcs0 crontab -l`;
open(CR,">/tmp/crontab.dcs0");
print CR "Last dcs0 'crontab -u production -l' was on $fulldate\n";
print CR "@ctab";
close(CR);
`source $sshfilesource; scp /tmp/crontab.dcs0 solarweb:/tmp/loglog`;
while($_ = shift(@ctab)) {
  if(/^#/ || /^\n/) { next; } #ignore any comment or blank lines
  push(@ctabclean, $_);
}
if(grep(/rsync_prod.pl/, @ctabclean)) {
  print "rsync_prod.pl OK\n";
}
else {
  print "rsync_prod.pl NOT FOUND*****\n";
  print MLOG "dcs0 rsync_prod.pl NOT FOUND*****\n";
  $found = 0;
}
if(grep(/tapearc_do_dc0/, @ctabclean)) {
  print "tapearc_do_dc0 OK\n";
}
else {
  print "tapearc_do_dc0 NOT FOUND*****\n";
  print MLOG "dcs0 tapearc_do_dc0 NOT FOUND*****\n";
  $found = 0;
}
if(grep(/build_arc_to_dds.pl/, @ctabclean)) {
  print "build_arc_to_dds.pl OK\n";
}
else {
  print "build_arc_to_dds.pl NOT FOUND*****\n";
  print MLOG "dcs0 build_arc_to_dds.pl NOT FOUND*****\n";
  $found = 0;
}
if(grep(/set_sum_main_offsite_ack_aia.pl/, @ctabclean)) {
  print "set_sum_main_offsite_ack_aia.pl OK\n";
}
else {
  print "set_sum_main_offsite_ack_aia.pl NOT FOUND*****\n";
  print MLOG "dcs0 set_sum_main_offsite_ack_aia.pl NOT FOUND*****\n";
  $found = 0;
}
if(grep(/restart_touch.pl/, @ctabclean)) {
  print "restart_touch.pl OK\n";
}
else {
  print "restart_touch.pl NOT FOUND*****\n";
  print MLOG "dcs0 restart_touch.pl NOT FOUND*****\n";
  $found = 0;
}
print "\nNow check on crontab entries for dcs1:\n";
@ctabclean = ();
@ctab = `source $sshfilesource; ssh dcs1 crontab -l`;
open(CR,">/tmp/crontab.dcs1");
print CR "Last dcs1 'crontab -u production -l' was on $fulldate\n";
print CR "@ctab";
close(CR);
`source $sshfilesource; scp /tmp/crontab.dcs1 solarweb:/tmp/loglog`;
while($_ = shift(@ctab)) {
  if(/^#/ || /^\n/) { next; } #ignore any comment or blank lines
  push(@ctabclean, $_);
}
if(grep(/rsync_prod_dcs1.pl/, @ctabclean)) {
  print "rsync_prod_dcs1.pl OK\n";
}
else {
  print "rsync_prod_dcs1.pl NOT FOUND*****\n";
  print MLOG "dcs1 rsync_prod_dcs1.pl NOT FOUND*****\n";
  $found = 0;
}
if(grep(/tapearc_do_dc1/, @ctabclean)) {
  print "tapearc_do_dc1 OK\n";
}
else {
  print "tapearc_do_dc1 NOT FOUND*****\n";
  print MLOG "dcs1 tapearc_do_dc1 NOT FOUND*****\n";
  $found = 0;
}
if(grep(/build_arc_to_dds.pl/, @ctabclean)) {
  print "build_arc_to_dds.pl OK\n";
}
else {
  print "build_arc_to_dds.pl NOT FOUND*****\n";
  print MLOG "dcs1 build_arc_to_dds.pl NOT FOUND*****\n";
  $found = 0;
}
if(grep(/set_sum_main_offsite_ack_hmi.pl/, @ctabclean)) {
  print "set_sum_main_offsite_ack_hmi.pl OK\n";
}
else {
  print "set_sum_main_offsite_ack_hmi.pl NOT FOUND*****\n";
  print MLOG "dcs1 set_sum_main_offsite_ack_hmi.pl NOT FOUND*****\n";
  $found = 0;
}
if(grep(/restart_touch.pl/, @ctabclean)) {
  print "restart_touch.pl OK\n";
}
else {
  print "restart_touch.pl NOT FOUND*****\n";
  print MLOG "dcs1 restart_touch.pl NOT FOUND*****\n";
  $found = 0;
}
print "\nNow check on crontab entries for j1:\n";
@ctabclean = ();
@ctab = `source $sshfilesource; ssh j1.stanford.edu crontab -l`;
open(CR,">/tmp/crontab.j1");
print CR "Last j1 'crontab -u production -l' was on $fulldate\n";
print CR "@ctab";
close(CR);
`source $sshfilesource; scp /tmp/crontab.j1 solarweb:/tmp/loglog`;
while($_ = shift(@ctab)) {
  if(/^#/ || /^\n/) { next; } #ignore any comment or blank lines
  push(@ctabclean, $_);
}
if(grep(/sum_alrm1/, @ctabclean)) {
  print "sum_alrm1 OK\n";
}
else {
  print "sum_alrm1 NOT FOUND*****\n";
  print MLOG "j1 sum_alrm1 NOT FOUND*****\n";
  $found = 0;
}
print "\nNow check on crontab entries for d02:\n";
@ctabclean = ();
@ctab = `crontab -l`;
open(CR,">/tmp/crontab.d02");
print CR "Last d02 'crontab -u production -l' was on $fulldate\n";
print CR "@ctab";
close(CR);
`source $sshfilesource; scp /tmp/crontab.d02 solarweb:/tmp/loglog`;
while($_ = shift(@ctab)) {
  if(/^#/ || /^\n/) { next; } #ignore any comment or blank lines
  push(@ctabclean, $_);
}
if(grep(/build_parc_file.pl/, @ctabclean)) {
  print "build_parc_file.pl OK\n";
}
else {
  print "build_parc_file.pl NOT FOUND*****\n";
  print MLOG "d02 build_parc_file.pl NOT FOUND*****\n";
  $found = 0;
}
print "\nNow get crontab entries for n02:\n";
@ctab = `source $sshfilesource; ssh n02.stanford.edu crontab -l`;
open(CR,">/tmp/crontab.n02");
print CR "Last n02 'crontab -u production -l' was on $fulldate\n";
print CR "@ctab";
close(CR);
`source $sshfilesource; scp /tmp/crontab.n02 solarweb:/tmp/loglog`;
#Can't ssh directly to cl1n001 (goes to j1). So skip this. Copied in by hand
#print "\nNow get crontab entries for cl1n001 jsocprod:\n";
#@ctab = `source $sshfilesource; ssh cl1n001.stanford.edu crontab -u jsocprod -l`;
#open(CR,">/tmp/crontab.cl1n001");
#print CR "Last cl1n001 'crontab -u jsocprod -l' was on $fulldate\n";
#print CR "@ctab";
#close(CR);

if(!$found) { $mailflg = 1; }

if($mailflg) {
  close(MLOG);
  $mail = "Mail -s \"Error from probe_dcs_lev0.pl\"";
        #$to = "lev0_user";
        $to = "jim";	#!!TEMP
        $cmd = "$mail $to < $MLOGFILE";
        system $cmd;

  exit;
}

close(MLOG);
exit;

##########################################################################

#Return date in form  2008-05-29
#NOTE: this format must match that in file name of .parc files.
#Also sets $fulldate to current time of form 2008-05-29_11:38:18
#Also set $ddoy to form: 2010_36.
sub labeldate {
  local($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst,$retdate);
  ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
  $sec2 = sprintf("%02d", $sec);
  $min2 = sprintf("%02d", $min);
  $hour2 = sprintf("%02d", $hour);
  $mday2 = sprintf("%02d", $mday);
  $mon2 = sprintf("%02d", $mon+1);
  $year4 = sprintf("%04d", $year+1900);
  $retdate = $year4."-".$mon2."-".$mday2;
  $fulldate = "$retdate"."_$hour2:"."$min2:"."$sec2";
  $ddoy = $year4."_".($yday+1);
  return($retdate);
}

#Check that a production (388) ssh-agent is running on the given host.
#The agent is what is set up in /var/tmp/ssh-agent.env.
#If $ckprime is set, then you are on the running machine, which must
#be called first to see if the prime ssh-agent is setup.
#Send mail and exit on error.
sub ck_ssh_stuff() {
  my($ckhost, $ckprime) = @_;
  if($ckprime) {
    @ps_agent = `ps -ef | grep ssh-agent`;
  }
  else {
    @ps_agent = `source $sshfilesource; ssh $ckhost "ps -ef | grep ssh-agent"`;
  }
  while($x = shift(@ps_agent)) {
    chomp($x);
    #print "$x\n";
    if($x =~ /^388 /) {
      ($a, $pid) = split(/\s+/, $x);
      #print "$pid\n";
      push(@pids, $pid);
    }
  }
  $ffound = 0;
  if($ckprime) {
    if(-e $SSHFILE) { $ffound = 1; }
  }
  else {
    $x = `source $sshfilesource; ssh $ckhost "ls $SSHFILE"`;
    chomp($x);
    if($x eq $SSHFILE) { $ffound = 1; }
  }
  if(!$ffound) {
    print "ssh setup file $SSHFILE on $ckhost not found.\n";
    print "Do this and try again:\n";
    print "  > ssh-agent | head -2 > /var/tmp/ssh-agent.env
    > chmod 600 /var/tmp/ssh-agent.env
    > source /var/tmp/ssh-agent.env
    > ssh-add
    (The production passphrase or password)\n";
    print MLOG "ssh setup file $SSHFILE on $ckhost not found.\n";
    print MLOG "Do this and try again:\n";
    print MLOG "  > ssh-agent | head -2 > /var/tmp/ssh-agent.env
    > chmod 600 /var/tmp/ssh-agent.env
    > source /var/tmp/ssh-agent.env
    > ssh-add
    (The production passphrase or password)\n";
    close(MLOG);
    $mail = "Mail -s \"Error from probe_dcs_lev0.pl\"";
          #$to = "lev0_user";
          $to = "jim";	#!!TEMP
          $cmd = "$mail $to < $MLOGFILE";
          system $cmd;
  
    exit;
  }
  if($ckprime) {
    open (EV,"$SSHFILE") || die "Can't Open : $SSHFILE $!\n"; 
    open (EVSRC,">$sshfilesource") || die "Can't Open : $sshfilesource $!\n";
    while(<EV>) {
      if(/^setenv/) {
        ($a, $b, $arg) = split(/ /);
        print EVSRC "$b=$arg\n";
        print EVSRC "export $b\n";
      }
    }
    close(EV);
    close(EVSRC);
    @catfile = `cat $SSHFILE`;
  } else {
    @catfile = `source $sshfilesource; ssh $ckhost "cat $SSHFILE"`;
  }
  while($x = shift(@catfile)) {
    if($x =~ /SSH_AGENT_PID/) {
      ($a, $b, $spid) = split(/\s+/, $x);
      chop($spid);		#elim trailing ';'
      last;
    }
  }

  $found = 0;
  while($x = shift(@pids)) {
    if($x == $spid) {
      #print "Good ssh pid $spid is found\n";
      $found = 1;
      last;
    }
  }
  if(!$found) {
    print "ssh setup file on $ckhost does not have active ssh-agent pid.\n";
    print "Do this and try again:\n";
    print "  > ssh-agent | head -2 > /var/tmp/ssh-agent.env
    > chmod 600 /var/tmp/ssh-agent.env
    > source /var/tmp/ssh-agent.env
    > ssh-add
    (The production passphrase or password)\n";
    print MLOG "ssh setup file on $ckhost does not have active ssh-agent pid.\n";
    print MLOG "Do this and try again:\n";
    print MLOG "  > ssh-agent | head -2 > /var/tmp/ssh-agent.env
    > chmod 600 /var/tmp/ssh-agent.env
    > source /var/tmp/ssh-agent.env
    > ssh-add
    (The production passphrase or password)\n";
    close(MLOG);
    $mail = "Mail -s \"Error from probe_dcs_lev0.pl\"";
          #$to = "lev0_user";
          $to = "jim";	#!!TEMP
          $cmd = "$mail $to < $MLOGFILE";
          system $cmd;
  
    exit;
  }
  print "Your $ckhost ssh-agent is set up ok\n";
}
