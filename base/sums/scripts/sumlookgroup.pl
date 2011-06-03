eval 'exec /home/jsoc/bin/$JSOC_MACHINE/perl -S $0 "$@"'
    if 0;
#/home/production/cvs/JSOC/base/sums/scripts/sumlookgroup3.pl
#
#Show how the storage is distributed (free, del pend, arch pend, etc.)
#for each SUM by group_id.
#
use DBI;

sub usage {
  print "Show storage distribution for each SUM group.\n";
  print "Usage: sumlookgroup.pl [-f] [-hdb_host] db_name (e.g. jsoc_sums)\n";
  print "       -f = full mode. Count the data read from tape that is always in group 0\n";
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
  local($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst,$date,$sec2,$min2,$hour2,$mday2,$year2, $esec, $edate);
  $esec = time + $xsec;
  ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($esec);
  $sec2 = sprintf("%02d", $sec);
  $min2 = sprintf("%02d", $min);
  $hour2 = sprintf("%02d", $hour);
  $mday2 = sprintf("%02d", $mday);
  $mon2 = sprintf("%02d", $mon+1);
  $year4 = sprintf("%04d", $year+1900);
  $date = $year4.".".$mon2.".".$mday2._.$hour2.":".$min2.":".$sec2;
  $edate = $year4.$mon2.$mday2.$hour2.$min2;
  return($edate);
}

sub commify {
                  local $_  = shift;
                  1 while s/^([-+]?\d+)(\d{3})/$1,$2/;
                  return $_;
             }


$HOSTDB = "hmidb";      #host where DB runs
$FULL = 0;
while ($ARGV[0] =~ /^-/) {
  $_ = shift;
  if (/^-h(.*)/) {
    $HOSTDB = $1;
  }
  if (/^-f(.*)/) {
    $FULL = 1;
  }
}

if($#ARGV != 0) {
  &usage;
}
$DB = $ARGV[0];

$ldate = &labeldate();
print "	/SUM for db=$DB by group_id $ldate (Full mode=$FULL)\n";
print "Group 0 : Not assigned (Not true)\n";
print "      1 : Catch-all group for everything not listed below\n";
print "      2 : hmi_ground, hmi.lev0\n";
print "      3 : aia_ground, aia.lev0\n";
print "      4 : hmi_ground, hmi.tlm\n";
print "      5 : aia_ground, aia.tlm\n";
print "      6 : hmi above lev1\n";
print "      7 : aia above lev1\n";
print "      8 : Stanford Helioseismology Archive\n";
print "      9 : dsds. Migrated data from SOI/DSDS\n";
print "     10 : hmi.lev1\n";
print "     11 : sid_awe.awesome\n";
print "     12 : su_timh.awesome\n";
print "    102 : hmi.lev0_60d\n";
print "    103 : aia.lev0_60d\n";
print "    104 : hmi.tlm_60d\n";
print "    105 : aia.tlm_60d\n";
print "    310 : hmi.rdVtrack_fd05, hmi.rdVtrack_fd15, hmi.rdVtrack_fd30\n";
print "    311 : hmi.rdvpspec_fd05, hmi.rdVpspec_fd15, hmi.rdVpspec_fd30\n";
print "    400 : reserved for expansion of a group to an additional drive\n";
print "    401 : reserved for expansion of a group to an additional drive\n";
print "  10000 : aia.lev1 (will eventually become group 100)\n\n";

$hostdb = $HOSTDB;      #host where Postgres runs
$user = $ENV{'USER'};
if(!($PGPORT = $ENV{'SUMPGPORT'})) {
  print "You must have ENV SUMPGPORT set to the port number, e.g. 5434\n";
  exit;
}
$totalbytesap = 0;
$totalbytes = 0;
$totalavail = 0;
$totalbyteso = 0;
$totalbytes30 = 0;
$totalbytes100 = 0;

      $addsec = 86400 * 100;	#sec in 100 days
      $xeffdate = &reteffdate($addsec);
      $addsec = 86400 * 30;	#sec in 30 days
      $yeffdate = &reteffdate($addsec);

#First connect to database
  $dbh = DBI->connect("dbi:Pg:dbname=$DB;host=$hostdb;port=$PGPORT", "$user", "");
  if ( !defined $dbh ) {
    die "Cannot do \$dbh->connect: $DBI::errstr\n";
  }

  print "Query in progress, may take awhile...\n";
  print "	Rounded down to nearest Megabyte\n\n";
  printf("Group %12s %12s %12s %12s %12s\n", "DPnow", "DP1-30d", "DP31-100d", "DPlater", "AP(MB)");
  printf("----- %12s %12s %12s %12s %12s\n", "--------", "--------", "--------", "--------", "--------");

#######################################################################
#    $sql = "select partn_name, avail_bytes from sum_partn_avail";
#    $sth = $dbh->prepare($sql);
#    if ( !defined $sth ) {
#      print "Cannot prepare statement: $DBI::errstr\n";
#      $dbh->disconnect();
#      exit; 
#    }
#    # Execute the statement at the database level
#    $sth->execute;
#    # Fetch the rows back from the SELECT statement
#    @row = ();
#    while ( @row = $sth->fetchrow() ) {
#      $sum = shift(@row);
#      push(@sum, $sum);
#      $avail = shift(@row);
#      $avail = $avail/1048576;
#      push(@avail, $avail);
#    }
#######################################################################
    @groupids = (0,1,2,3,4,5,6,7,8,9,10,11,12,102,103,104,105,310,311,400,401,10000,-1);
    while(($group = shift(@groupids)) != -1) {
      if(!$FULL || $group==400 || $group==401) { #no Full mode for 400s
        $sql = "select sum(bytes) from sum_partn_alloc where (status=4 and archive_substatus=32 or status=2) and group_id=$group and effective_date <= '$effdate'";
      } 
      else {  #there can be data from tape that is always in group=0
        $sql = "select sum(bytes) from sum_main where storage_group=$group and ds_index in (select ds_index from sum_partn_alloc where (status=4 and archive_substatus=32 or status=2) and effective_date <= '$effdate')";
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

      #now get DP<=30d
      if(!$FULL  || $group==400 || $group==401) {
        $sql = "select sum(bytes) from sum_partn_alloc where (status=4 and archive_substatus=32 or status=2) and group_id=$group and effective_date <= '$yeffdate'";
      }
      else {
        $sql = "select sum(bytes) from sum_main where storage_group=$group and ds_index in (select ds_index from sum_partn_alloc where (status=4 and archive_substatus=32 or status=2) and effective_date <= '$yeffdate')";
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
        $bytes30 = shift(@row);
        $bytes30 = $bytes30/1048576;
        push(@bytes30, $bytes30);
      }

      #now get DP<=100d
      if(!$FULL  || $group==400 || $group==401) {
        $sql = "select sum(bytes) from sum_partn_alloc where (status=4 and archive_substatus=32 or status=2) and group_id=$group and effective_date <= '$xeffdate'";
      }
      else {
        $sql = "select sum(bytes) from sum_main where storage_group=$group and ds_index in (select ds_index from sum_partn_alloc where (status=4 and archive_substatus=32 or status=2) and effective_date <= '$xeffdate')";
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
      if(!$FULL  || $group==400 || $group==401) {
        $sql = "select sum(bytes) from sum_partn_alloc where (status=4 and archive_substatus=32 or status=2) and group_id=$group and effective_date > '$xeffdate'";
      }
      else {
        $sql = "select sum(bytes) from sum_main where storage_group=$group and ds_index in (select ds_index from sum_partn_alloc where (status=4 and archive_substatus=32 or status=2) and effective_date > '$xeffdate')";
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
        $bytes30 = shift(@bytes30);
        $totalbytes30 += $bytes30;
        $bytes100 = shift(@bytes100);
        $totalbytes100 += $bytes100;
        $byteso = shift(@byteso);
        $totalbyteso += $byteso;
        #printf("$sum %12d %12d %12d %12d\n", $avail,$bytes,$byteso,$bytesap);
        $BYTES = $bytes;
        $BYTES30 = $bytes30 - $bytes;
        $BYTES100 = $bytes100 - $bytes30;
        $TOTALBYTES30 = $totalbytes30 - $totalbytes;
        $TOTALBYTES100 = $totalbytes100 - $totalbytes30;
        printf("$group\t%12s %12s %12s %12s %12s\n", 
		commify(int($BYTES)), commify(int($BYTES30)), 
		commify(int($BYTES100)), 
		commify(int($byteso)), commify(int($bytesap)));
      }
    }
    printf("------------------------------------------------------------------------\n");
    printf("TOTAL:\t%12s %12s %12s %12s %12s\n", 
		commify(int($totalbytes)), commify(int($TOTALBYTES30)), commify(int($TOTALBYTES100)),
		commify(int($totalbyteso)), commify(int($totalbytesap)));

    #$totalstor = $totalbytes+$totalbytes100+$totalbyteso+$totalbytesap;
    $totalstor = $totalbytes+$TOTALBYTES30+$TOTALBYTES100+$totalbyteso;

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

