eval 'exec /home/jsoc/bin/$JSOC_MACHINE/perl -S $0 "$@"'
    if 0;
#/home/production/cvs/JSOC/base/sums/scripts/sumlookgroup.pl
#
#Show how the storage is distributed (free, del pend, arch pend, etc.)
#for each SUM by group_id.
#
use DBI;

sub usage {
  print "Show storage distribution for each SUM group.\n";
  print "Usage: sumlookgroup.pl [-hdb_host] db_name (e.g. jsoc_sums)\n";
  print "       The default db_host is $HOSTDB\n";
  print "\nYou must have ENV SUMPGPORT set to the port number, e.g. 5434\n";
  exit(1);
}

#Return date in form for a label e.g. 1998.01.07_14:42:04
#Also set effective_date in form 199801071442
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

sub commify {
                  local $_  = shift;
                  1 while s/^([-+]?\d+)(\d{3})/$1,$2/;
                  return $_;
             }


$HOSTDB = "hmidb";      #host where DB runs
while ($ARGV[0] =~ /^-/) {
  $_ = shift;
  if (/^-h(.*)/) {
    $HOSTDB = $1;
  }
}

if($#ARGV != 0) {
  &usage;
}
$DB = $ARGV[0];

$ldate = &labeldate();
print "	Storage on /SUM for db=$DB by group_id $ldate\n";
print "Group 0 : Not assigned\n";
print "      1 : Catch-all group for everything not listed below\n";
print "      2 : hmi_ground, hmi.lev0\n";
print "      3 : aia_ground, aia.lev0\n";
print "      4 : hmi_ground, hmi.tlm\n";
print "      5 : aia_ground, aia.tlm\n";
print "      6 : hmi lev1 and above\n";
print "      7 : aia lev1 and above\n";
print "      8 : Stanford Helioseismology Archive\n";
print "      9 : dsds. Migrated data from SOI/DSDS\n";
print "    102 : hmi.lev0_60d\n";
print "    103 : aia.lev0_60d\n";
print "    104 : hmi.tlm_60d\n";
print "    105 : aia.tlm_60d\n\n";

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

#First connect to database
  $dbh = DBI->connect("dbi:Pg:dbname=$DB;host=$hostdb;port=$PGPORT", "$user", "$password");
  if ( !defined $dbh ) {
    die "Cannot do \$dbh->connect: $DBI::errstr\n";
  }

  print "	Rounded down to nearest Megabyte\n";
  print "Query in progress, may take awhile...\n";
  printf("Group %12s %12s %12s %12s\n", "DPnow", "DPlater", "AP");
  printf("----- %12s %12s %12s %12s\n", "--------", "--------", "--------");

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
    @groupids = (1,2,3,4,5,6,7,8,9,102,103,104,105);
    while($group = shift(@groupids)) {
      $sql = "select sum(bytes) from sum_partn_alloc where status=2 and group_id=$group and effective_date <= '$effdate'";
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
      #now get DPlater 
      $sql = "select sum(bytes) from sum_partn_alloc where status=2 and group_id=$group and effective_date > '$effdate'";
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
        $byteso = shift(@byteso);
        $totalbyteso += $byteso;
        #printf("$sum %12d %12d %12d %12d\n", $avail,$bytes,$byteso,$bytesap);
        printf("$group\t%12s %12s %12s\n", 
		commify(int($bytes)), commify(int($byteso)), 
		commify(int($bytesap)));
      }
    }
    printf("------------------------------------------------------------\n");
    printf("TOTAL:\t%12s %12s %12s\n", 
		commify(int($totalbytes)), commify(int($totalbyteso)),
		commify(int($totalbytesap)));
$dbh->disconnect();


