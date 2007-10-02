eval 'exec /home/jsoc/bin/$JSOC_MACHINE/perl -S $0 "$@"'
    if 0;
#/home/production/cvs/jsoc/scripts/sum/sumlook.pl
#
#Show how the storage is distributed (free, del pend, arch pend, etc.)
#for each /SUM partition in the sum_partn_avail table.
#
use DBI;

sub usage {
  print "Show storage distribution for each /SUM partition.\n";
  print "Usage: sumlook.pl [-hdb_host] db_name (e.g. jsoc)\n";
  print "       The default db_host is $HOSTDB\n";
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
print "	Storage on /SUM partitions for db=$DB $ldate\n";
$hostdb = $HOSTDB;      #host where Postgres runs
$user = $ENV{'USER'};

#First connect to database
  $dbh = DBI->connect("dbi:Pg:dbname=$DB;host=$hostdb", "$user", "$password");
  if ( !defined $dbh ) {
    die "Cannot do \$dbh->connect: $DBI::errstr\n";
  }

  print "	Rounded down to nearest Megabyte\n";
  print "Query in progress, may take awhile...\n";
  printf("Part %12s %12s %12s %12s\n", "Free", "DPnow", "DPother", "AP");
  printf("----- %12s %12s %12s %12s\n", "--------", "--------", "--------", "--------");
  #print "Part\tFree\t\tDPnow\t\tDPother\t\tAP\n";
  #print "------\t\t------\t\t-----\t\t-------\t\t-----\n";
    $sql = "select partn_name, avail_bytes from sum_partn_avail";
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
    while ( @row = $sth->fetchrow() ) {
      $sum = shift(@row);
      push(@sum, $sum);
      $avail = shift(@row);
      $avail = $avail/1048576;
      push(@avail, $avail);
    }
    while($sum = shift(@sum)) {
      $sql = "select sum(bytes) from sum_partn_alloc where status=2 and wd like '$sum%' and effective_date <= $effdate";
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
      #now get DPother 
      $sql = "select sum(bytes) from sum_partn_alloc where status=2 and wd like '$sum%' and effective_date > $effdate";
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
      $sql = "select sum(bytes) from sum_partn_alloc where status=4 and wd like '$sum%'";
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
        $bytes = shift(@bytes);
        $avail = shift(@avail);
        $byteso = shift(@byteso);
        #printf("$sum %12d %12d %12d %12d\n", $avail,$bytes,$byteso,$bytesap);
        printf("$sum %12s %12s %12s %12s\n", commify(int($avail)), 
		commify(int($bytes)), commify(int($byteso)), 
		commify(int($bytesap)));
      }
    }
$dbh->disconnect();


