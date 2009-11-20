eval 'exec /home/jsoc/bin/$JSOC_MACHINE/perl -S $0 "$@"'
    if 0;
#/home/production/cvs/JSOC/base/sums/scripts/t120_rearchive.pl
#
#For a given tape id (e.g. 000001S1) will read all the files sequentailly
#and update the sum_partn_alloc table to make each ds archive pending
#again. Any tape file consists of potentially many data sets.
#Option to call with second arg that is the filenumber to start at.
#Also -l flag will give the last file number to read to.
#
use DBI;

sub usage {
  print "Read t120 tape sequentially & update sum_partn_alloc to arch pend.\n";
  print "Usage: t120_rearchive.pl [-hdb_host] [-ddb_name] [-llastfnum] tapeid [filenumber]\n";
  print "       The default db_host is $HOSTDB. The db_name is jsoc_sums\n";
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
$DB = "jsoc_sums";
$lastfnum = 0;
#$PGPORT = 5434;
if(!($PGPORT = $ENV{'SUMPGPORT'})) {
  print "You must have ENV SUMPGPORT set to the port number, e.g. 5430\n";
  exit;
}

while ($ARGV[0] =~ /^-/) {
  $_ = shift;
  if (/^-h(.*)/) {
    $HOSTDB = $1;
  }
  if (/^-d(.*)/) {
    $DB = $1;
  }
  if (/^-l(.*)/) {
    $lastfnum = $1;
  }   
}

if(($#ARGV != 0) && ($#ARGV !=1)) {
  &usage;
}
$startfn = 1;
if($#ARGV == 1) {
  $startfn = $ARGV[1];
}
$tapeid = $ARGV[0];

$ldate = &labeldate();
print "t120 rearchive for tapeid=$tapeid db=$DB $ldate\n";
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

    $sql = "select group_id, nxtwrtfn from sum_tape where tapeid='$tapeid'";
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
    $found = 0;
    while ( @row = $sth->fetchrow() ) {
      $group_id = shift(@row);
      $nxtwrtfnum= shift(@row);
      print "group_id = $group_id\n";
      print "next_wrie_file_number = $nxtwrtfnum\n";
      $found = 1;
      if($startfn >= $nxtwrtfnum) {
        print "ERROR: given start file# is too big\n";
        $dbh->disconnect();
        exit;
      }
    }
    if(!$found) {
      print "ERROR: no group_id for tapeid=$tapeid\n";
      $dbh->disconnect();
      exit;
    }
    if($lastfnum) {
      $nxtwrtfnum = $lastfnum;
    }
for($tapefn=$startfn; $tapefn < $nxtwrtfnum; $tapefn++) {
  print "\nProcess for tapeid=$tapeid filenumber=$tapefn\n";
  $sql = "select ds_index, online_status from sum_main where arch_tape='$tapeid' and arch_tape_fn=$tapefn";
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
    $found = 0; $called = 0;
    while ( @row = $sth->fetchrow() ) {
      $ds_index = shift(@row);
      $on_line = shift(@row);
      $found = 1;
      push(@dsindex, $ds_index);
      if(!$called && $on_line eq 'N') {	#call the first on to bring all online
        $called = 1;
        print "sumget $ds_index\n";	#bring them all on line if needed
        @get = `sumget $ds_index`;
        print "@get\n";
        if(!grep(/Normal exit/, @get)) {
          print "ERROR: sumget failure\n\n";
          $dbh->disconnect(); #!!!TEMP
          exit;
        }
      }
    }
    if(!$found) {
      print "No ds_index found for tape filenumber = $tapefn\n";
      next;
    }
    if(!$called) {
      print "All ds_index already on line\n";
    }

  while($ds_index = shift(@dsindex)) {
    $sql = "update sum_partn_alloc set status=4, archive_substatus=128, group_id=$group_id where ds_index=$ds_index and status=2";
    print "$sql\n";
    $sth = $dbh->prepare($sql);
    if ( !defined $sth ) {
      print "Cannot prepare statement: $DBI::errstr\n";
      $dbh->disconnect();
      exit; 
    }
    # Execute the statement at the database level
    $status = $sth->execute;
    print "execute status = $status\n";
  }
}
print "All done at tape file# $tapefn\n";
$dbh->disconnect();

