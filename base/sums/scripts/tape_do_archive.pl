eval 'exec /home/jsoc/bin/$JSOC_MACHINE/perl -d -S $0 "$@"'
    if 0;
#/home/production/cvs/JSOC/base/sums/scripts/tape_do_archive.pl
#
#
use DBI;

#!!!TBD
sub usage {
  print "Show storage distribution for each SUM group.\n";
  print "Usage: sumlookgroup.pl [-f] [-hdb_host] db_name (e.g. jsoc_sums)\n";
  print "       -f = fast mode. Ignore that data read from tape is always in group 0\n";
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

$ARCHGRPCFG = "/home/production/cvs/JSOC/base/sums/apps/data/arch_group.cfg";
$ARCHPROBE = "/home/production/cvs/JSOC/proj/util/scripts/archprobe.pl";
#$ARCHFILEDIR = "/usr/local/logs/tapearc";
$ARCHFILEDIR = "/tmp"; #!!TEMP
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
}

if($#ARGV != 0) {
  &usage;
}
$DB = $ARGV[0];		#!!!TBD fix what this arg is

$ldate = &labeldate();
print "tape_do_archive.pl run on $ldate\n";

$hostdb = $HOSTDB;      #host where Postgres runs
$user = $ENV{'USER'};
if(!($PGPORT = $ENV{'SUMPGPORT'})) {
  print "You must have ENV SUMPGPORT set to the port number, e.g. 5434\n";
  exit;
}

      $addsec = 86400 * 100;	#sec in 100 days
      $xeffdate = &reteffdate($addsec);

open (CFG, $ARCHGRPCFG) || die "Can't Open : $ARCHGRPCFG $!\n";
while(<CFG>) {
  if(/^#/ || /^\n/) { #ignore any comment or blank lines
    next;
  }
  chomp;
  ($group, $minwrtsz, $waitdays) = split(/\s+/);
  push(@groups, $group);
  push(@minwrtsz, $minwrtsz);
  push(@waitdays, $waitdays);
}
push(@groups, -1);		#terminate
print "groups=@groups\n";
print "minwrtsz=@minwrtsz\n";
print "waitdays=@waitdays\n";

$i = 0;
while(($group = shift(@groups)) != -1) {
  $GBtotal = 0;
  print "\ngroup = $group\n";
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
  if($GBtotal >= $minwrtsz[$i]) { 
    #now get the details by order of sunum
    $cmd = "$ARCHPROBE jsoc_sums hmidb 5434 production raw group ap ' ' $group 1";
    @datsunum = `$cmd`;
    $btotal = 0; $ttotal = 0;
    $j = 0;
    $filename = "$ARCHFILEDIR/archfile.group$group";
    open(AR, ">$filename") || die "Can't open $filename: $!\n";
    print AR "#group series sunum sudir AP (bytes) sumid eff_date\n";
    #print "@datsunum\n";
    #A line looks like
#group series sunum sudir AP (bytes) sumid eff_date
#6 hmi.hmiserieslev1pb45 153752265 /SUM1/D153752265 7763370790 62825881 201107041532
    while($_ = shift(@datsunum)) {
      #print "$_";
      if(/^#/ || /^\n/) { #ignore any comment or blank lines
        next;
      }
      #accumulate 500MB for a tape file
      ($grp,$ser,$sunum,$sudir,$bytes,$sumid,$edate) = split(/\s+/);
      chomp($edate);
      $btotal += $bytes;
      push(@fileinfo, $_);
      if($btotal >= 536870912) { #500MB
        print AR "#File $j:\n";
        print AR "@fileinfo";
        $ttotal += $btotal;
        if($ttotal >= 760000000000) { #!!TBD use cfg info
          print AR "EOT:\n";
          $ttotal = 0;
        }
        $btotal = 0;
        @fileinfo = ();
        $j++;
      }
    }
    close(AR);
  }
  $i++;
}
exit;

##################OLD STUFF BELOW########################################
#First connect to database
  $dbh = DBI->connect("dbi:Pg:dbname=$DB;host=$hostdb;port=$PGPORT", "$user", "");
  if ( !defined $dbh ) {
    die "Cannot do \$dbh->connect: $DBI::errstr\n";
  }

  print "	Rounded down to nearest Megabyte\n";
  print "	NOTE: DP<=100d includes DPnow\n";
  print "Query in progress, may take awhile...\n";
  printf("Group %12s %12s %12s %12s\n", "DPnow", "DP<=100d", "DPlater", "AP");
  printf("----- %12s %12s %12s %12s\n", "--------", "--------", "--------", "--------");

    @groupids = (0,1,2,3,4,5,6,7,8,9,10,11,12,102,103,104,105,310,311,400,401,10000,-1);
    while(($group = shift(@groupids)) != -1) {
      if($FAST) {
        $sql = "select sum(bytes) from sum_partn_alloc where status=2 and group_id=$group and effective_date <= '$effdate'";
      } 
      else {  #there can be data from tape that is always in group=0
        $sql = "select sum(bytes) from sum_main where storage_group=$group and ds_index in (select ds_index from sum_partn_alloc where status=2 and effective_date <= '$effdate')";
      }
      #print "$sql\n"; #!!!TEMP
      $sth = $dbh->prepare($sql);
      if ( !defined $sth ) {
        print "Cannot prepare statement: $DBI::errstr\n";
        $dbh->disconnect();
        exit; 
      }
      $sth->execute;
      while ( @row = $sth->fetchrow() ) {
        $bytes = shift(@row);
        $bytes = $bytes/1048576;
        push(@bytes, $bytes);
      }

      #now get DP<=100d
      if($FAST) {
        $sql = "select sum(bytes) from sum_partn_alloc where status=2 and group_id=$group and effective_date <= '$xeffdate'";
      }
      else {
        $sql = "select sum(bytes) from sum_main where storage_group=$group and ds_index in (select ds_index from sum_partn_alloc where status=2 and effective_date <= '$xeffdate')";
      }
      #print "$sql\n"; #!!!TEMP
      $sth = $dbh->prepare($sql);
      if ( !defined $sth ) {
        print "Cannot prepare statement: $DBI::errstr\n";
        $dbh->disconnect();
        exit; 
      }
      $sth->execute;
      while ( @row = $sth->fetchrow() ) {
        $bytes100 = shift(@row);
        $bytes100 = $bytes100/1048576;
        push(@bytes100, $bytes100);
      }
      #now get DPlater 
      if($FAST) {
        $sql = "select sum(bytes) from sum_partn_alloc where status=2 and group_id=$group and effective_date > '$xeffdate'";
      }
      else {
        $sql = "select sum(bytes) from sum_main where storage_group=$group and ds_index in (select ds_index from sum_partn_alloc where status=2 and effective_date > '$xeffdate')";
      }
      #print "$sql\n"; #!!!TEMP
      $sth = $dbh->prepare($sql);
      if ( !defined $sth ) {
        print "Cannot prepare statement: $DBI::errstr\n";
        $dbh->disconnect();
        exit; 
      }
      $sth->execute;
      while ( @row = $sth->fetchrow() ) {
        $byteso = shift(@row);
        $byteso = $byteso/1048576;
        push(@byteso, $byteso);
      }
      #now get AP 
      $sql = "select sum(bytes) from sum_partn_alloc where status=4 and archive_substatus=128 and group_id=$group";
      #print "$sql\n"; #!!!TEMP
      $sth = $dbh->prepare($sql);
      if ( !defined $sth ) {
        print "Cannot prepare statement: $DBI::errstr\n";
        $dbh->disconnect();
        exit; 
      }
      $sth->execute;
      while ( @row = $sth->fetchrow() ) {
        $bytesap = shift(@row);
        $bytesap = $bytesap/1048576;
        $totalbytesap += $bytesap;
        $bytes = shift(@bytes);
        $totalbytes += $bytes;
        $avail = shift(@avail);
        $totalavail += $avail;
        $bytes100 = shift(@bytes100);
        $totalbytes100 += $bytes100;
        $byteso = shift(@byteso);
        $totalbyteso += $byteso;
        #printf("$sum %12d %12d %12d %12d\n", $avail,$bytes,$byteso,$bytesap);
        printf("$group\t%12s %12s %12s %12s\n", 
		commify(int($bytes)), commify(int($bytes100)), 
		commify(int($byteso)), commify(int($bytesap)));
      }
    }
    printf("------------------------------------------------------------\n");
    printf("TOTAL:\t%12s %12s %12s %12s\n", 
		commify(int($totalbytes)), commify(int($totalbytes100)),
		commify(int($totalbyteso)), commify(int($totalbytesap)));

    $totalstor = $totalbytes+$totalbytes100+$totalbyteso+$totalbytesap;

    #Get all storage that is read back from tape  
    $sql = "select sum(bytes) from sum_partn_alloc where group_id=0 and wd like '/%/D%/D%'";
    #print "$sql\n"; #!!!TEMP
    $sth = $dbh->prepare($sql);
    if ( !defined $sth ) {
      print "Cannot prepare statement: $DBI::errstr\n";
      $dbh->disconnect();
      exit; 
    }
    $sth->execute;
    while ( @row = $sth->fetchrow() ) {
      $bytest = shift(@row);
      $bytest = $bytest/1048576;
    }

    $p = sprintf("%02.1f", 100.0 * ($bytest/$totalstor));
    #print "bytest=$bytest totalstor=$totalstor\n";
    print "\nTotal bytes online that have been read back from tape:\n";
    printf "%12s = $p percent of total storage of %12s\n", commify(int($bytest)), commify(int($totalstor));

$dbh->disconnect();

