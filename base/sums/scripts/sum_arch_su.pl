eval 'exec /home/jsoc/bin/$JSOC_MACHINE/perl -S $0 "$@"'
    if 0;
#/home/production/cvs/JSOC/base/sums/scripts/sum_arch_su.pl
#
#Takes a list of sunum's and marks them archive pending in the given database.
#Any sunum that is not already online, cannot be marked for archiving.
#
#
use DBI;

sub usage {
  print "Mark given sunum(s) as archive pending in the given DB\n";
  print "Usage: sum_arch_su.pl [-t30] [-ffile] [sunum,sunum,sunum] database\n";
  print "       -t = Number of days in the future to set effective_date.\n";
  print "            If no -t is given the effective_date is not changed.\n";
  print "       -f = file name that contains the sunums. One or more \n";
  print "            comma delimited sunum per line.\n";
  print "       sunum,sunum,sunum = If no -f, give comma delimited sunums\n";
  print "       database = SUMS db, e.g. jsoc_sums, jim, etc..\n";
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


$HOSTDB = "hmidb";      #host where DB runs
$INFILE = 0;
$TOUCH = 0;
while ($ARGV[0] =~ /^-/) {
  $_ = shift;
  if (/^-f(.*)/) {
    $INFILE = $1;
  }
  if (/^-t(.*)/) {
    $TOUCH = $1;
  }
}
if($INFILE) {
  if($#ARGV != 0) { &usage; }
  else { $DBIN = $ARGV[0]; }
}
else {
  if($#ARGV != 1) { &usage; }
  else {
    $sunums = $ARGV[0];
    $DBIN = $ARGV[1];
  }
}

#$ldate = &labeldate();
$addsec = 86400 * $TOUCH;    #sec in touch days
$xeffdate = &reteffdate($addsec);
#print "$xeffdate\n"; #!!TEMP

$hostdb = $HOSTDB;      #host where Postgres runs
$user = $ENV{'USER'};
if($user ne "production") {
  print "You must be user production to run\n";
  exit;
}

if($DBIN eq "jim") {
  $PORTIN = 5432;
}
else {
  $PORTIN = 5434;
}

  #connect to the databases
  $dbh = DBI->connect("dbi:Pg:dbname=$DBIN;host=$hostdb;port=$PORTIN ", "$user", "");
  if ( !defined $dbh ) {
    die "Cannot do \$dbh->connect: $DBI::errstr\n";
  }
  #printf("%16s %16s\n", "sunum", "effective_date");
  print "#sunum\t\teffective_date\n";
  if(!$INFILE) {
    $single = 1;
    $_ = $sunums;
    goto SINGLE;
  }
  else {
    $single = 0;
    open(ID, $INFILE) || die "Can't open $INFILE: $!\n";
  }
    while(<ID>) {
      if(/^#/ || /^\n/) { #ignore any comment or blank lines
        next;
      }
      chomp;
      %HofDsix = ();   #hash key is ds_index (sunum)
SINGLE:
      @dsix = split(/\,/);
      #print "\@dsix = @dsix\n"; #!!TEMP
      if($TOUCH) {
        #$sql = "update sum_partn_alloc set status=4, archive_substatus=128, effective_date=$xeffdate where ds_index in ($_) and (archive_substatus!=128 or status=2)";
        $sql = "update sum_partn_alloc set status=4, archive_substatus=128, effective_date=$xeffdate where ds_index in ($_)";
      }
      else {
        #$sql = "update sum_partn_alloc set status=4, archive_substatus=128 where ds_index in ($_) and (archive_substatus!=128 or status=2)";
        $sql = "update sum_partn_alloc set status=4, archive_substatus=128 where ds_index in ($_)";
      }
      #print "$sql\n";
      $sth = $dbh->prepare($sql);
      if ( !defined $sth ) {
        print "Cannot prepare statement: $DBI::errstr\n";
        $dbh->disconnect();
        exit; 
      }
      # Execute the statement at the database level
      $sth->execute;
      $sql = "select ds_index,status,archive_substatus,effective_date from sum_partn_alloc where ds_index in ($_)";
      #print "$sql\n";
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
        $dsix = shift(@row);
        $HofDsix{$dsix} = 1;
        $status = shift(@row);
        $archive_substatus = shift(@row);
        $eff_date = shift(@row);
        #printf("%16s %16s\n", $dsix, $eff_date);
        print "$dsix\t$eff_date\n";
      }
      while($ix = shift(@dsix)) {
        if(!$HofDsix{$ix}) {
          print "$ix\tnot found in sum_partn_alloc table\n";
        }
      }
      if($single) { break; }
    }
  if(!$single) { close(ID); }

$dbh->disconnect();


