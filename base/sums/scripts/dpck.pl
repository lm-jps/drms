#!/usr/bin/perl
#/home/jim/STAGING/script/dpck.pl
#
#Find all the ds marked del pend in the sum_partn_alloc table, and print out
#those that don't have a valid directory. 
#
use DBI;

sub usage {
  print "Find del pend datasets without a valid dir.\n";
  print "Usage: dpck.pl\n";
  exit(1);
}

#Return date in form for a label e.g. 1998.01.07_14:42:04
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
  return($date);
}


$| = 1;			#flush output as we go
$DB = jsoc;
$HOSTDB = "hmidb";      #host where DB runs

$ldate = &labeldate();
print "$ldate\n";
$hostdb = $HOSTDB;      #host where Postgres runs
$user = $ENV{'USER'};
if(!($PGPORT = $ENV{'SUMPGPORT'})) {
  print "You must have ENV SUMPGPORT set to the port number, e.g. 5430\n";
  exit;
}

#First connect to database
  $dbh = DBI->connect("dbi:Pg:dbname=$DB;host=$hostdb;port=$PGPORT", "$user", "$password");
  if ( !defined $dbh ) {
    die "Cannot do \$dbh->connect: $DBI::errstr\n";
  }

    $sql = "select wd,ds_index from sum_partn_alloc where status=2";
    $sth = $dbh->prepare($sql);
    if ( !defined $sth ) {
      print "Cannot prepare statement: $DBI::errstr\n";
      $dbh->disconnect();
      exit; 
    }
    # Execute the statement at the database level
    $sth->execute;
    # Fetch the rows back from the SELECT statement
    @row = (); @wd = (); @dsindex = ();
    while ( @row = $sth->fetchrow() ) {
      push(@wd, shift(@row));
      push(@dsindex, shift(@row));
    }
    if(defined $sth) {
      $sth->finish;
    }
    $total = $#dsindex + 1;
    print "There are a total of $total del pend SUM entries\n";
    while($wd = shift(@wd)) {
      $dsindex = shift(@dsindex);
      #give msg if not valid wd
      if(!-e $wd) {
        print "#Del Pend wd $wd does not exist. ds_index=$dsindex\n";
      }
    }
$dbh->disconnect();
