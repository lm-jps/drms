eval 'exec /home/jsoc/bin/$JSOC_MACHINE/perl -S $0 "$@"'
    if 0;
#/home/production/cvs/JSOC/base/sums/scripts/sum_arch_recset.pl
#
#Takes a record set and marks all the storage units archive pending.
#Any sunum that is not already online, cannot be marked for archiving.
#This works with the SUMS database jsoc_sums.
#
#
use DBI;

sub usage {
  print "Mark the given record set sunums as archive pending\n";
  print " NOTE: You must be user production to run\n";
  print "Usage: sum_arch_recset.pl [-u] [-t30] [-g9] [-ffile] 'aia.lev1[2011-12-10/1m]'\n";
  print "       -u = Update mode, updates the DB, else advise only mode.\n";
  print "       -t = Number of days in the future to set effective_date.\n";
  print "            If no -t is given the effective_date is not changed.\n";
  print "       -g = Group # to put in the sum_partn_alloc table. Default 0.\n";
  print "       -f = file name that contains the record sets. One record set \n";
  print "            per line.\n";
  print "       If no -f, specify the record set\n";
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
$DBIN = "jsoc_sums";
$PORTIN = 5434;		#port for jsoc_sums DB
$INFILE = 0;
$TOUCH = 0;
$GROUP = 0;
$UPDATEMODE = 0;
while ($ARGV[0] =~ /^-/) {
  $_ = shift;
  if (/^-f(.*)/) {
    $INFILE = $1;
  }
  if (/^-t(.*)/) {
    $TOUCH = $1;
  }
  if (/^-g(.*)/) {
    $GROUP = $1;
  }
  if (/^-u(.*)/) {
    $UPDATEMODE = 1;
  }
}
if($INFILE) {
  if($#ARGV != -1) { &usage; }
}
else {
  if($#ARGV != 0) { &usage; }
  else {
    $recset = $ARGV[0];
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

#$cmd = "show_info -Sq $recset";
#@sunums = `$cmd`;
#print "@sunums\n";  #!!TEMP
#exit;

  #connect to the databases
  $dbh = DBI->connect("dbi:Pg:dbname=$DBIN;host=$hostdb;port=$PORTIN ", "$user", "");
  if ( !defined $dbh ) {
    die "Cannot do \$dbh->connect: $DBI::errstr\n";
  }
  #printf("%16s %16s\n", "sunum", "effective_date");
  if(!$UPDATEMODE) { print "ADVISE ONLY MODE\n"; }
  else { print "UPDATE MODE\n"; }
  print "#sunum\t\teffective_date\tgroup\tcadence\n";
  if(!$INFILE) {
    $single = 1;
    $_ = $recset;
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
SINGLE:
      $cmd = "show_info -Sq $_";
      @dsix = `$cmd`;
    while($dsix = shift(@dsix)) {
      chomp($dsix);
      $sql = "select group_id,effective_date from sum_partn_alloc where ds_index=$dsix";
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
      if(@row = $sth->fetchrow()) {
        $group_id = shift(@row);
        $group_id = $GROUP;		#override w/cmd arg
        $eff_date = shift(@row);
        $entry = sprintf("%s,%s,%s", $dsix, $eff_date, $group_id);
        #push(@entry, $entry);	#must save this before do next query
        if($UPDATEMODE) {		#ok to update DB
          if($TOUCH) {
            $sql = "update sum_partn_alloc set status=4, archive_substatus=128, effective_date=$xeffdate, group_id=$group_id  where ds_index=$dsix";
          }
          else {
            $sql = "update sum_partn_alloc set status=4, archive_substatus=128, group_id=$group_id where ds_index=$dsix";
            #$sql = "update sum_partn_alloc set archive_substatus=32 where ds_index=$dsix";
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
        }
        ($dsix, $eff_date, $group_id) = split(/\,/, $entry);
        $sql = "select cadence_days from sum_arch_group where group_id=$group_id";
        $sth = $dbh->prepare($sql);
        if ( !defined $sth ) {
          print "Cannot prepare statement: $DBI::errstr\n";
          $dbh->disconnect();
          exit; 
        }
        # Execute the statement at the database level
        $sth->execute;
        while ( @row = $sth->fetchrow() ) {
          $cadence = shift(@row);
        }
        if($cadence) { 
          print "$dsix\t$eff_date\t$group_id\t$cadence\n";
        }
        else { 
          print "$dsix\t$eff_date\t$group_id\t$cadence WARNING: Not Enabled for Archive\n";
        }
      }
      else {
        print "$dsix\tnot found\n";
      }
      if($single) { break; }
    }
  }
  if(!$single) { close(ID); }

$dbh->disconnect();

