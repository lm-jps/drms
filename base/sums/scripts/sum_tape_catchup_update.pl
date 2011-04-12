eval 'exec /home/jsoc/bin/$JSOC_MACHINE/perl -d -S  $0 "$@"'
    if 0;
#sum_tape_catchup_update.pl
#
#This will update the sum_tape table from Keh-Cheng's catch-up tape
#writes of Mar 2011. 
#Called with the dir to update. This dir has a file tapeid.list that has
#the tapeid to insert/update in the sum_tape table. The dir has the
#other files needed to get the info.
#
use DBI;
use Term::ReadKey;

$DB = "jsoc_sums";
$PGPORT=5434;

sub usage {
  print "Insert/Update the sum_tape table from Keh-Cheng's catch-up tape writes\n";
  print "Usage: sum_tape_catchup_update.pl in_dir group_id\n";
  print "  e.g. sum_tape_catchup_update.pl /scr21/production/tapearc/DONE\n";
  print "       Requires confirmation to run\n";
  exit(1);
}

if($#ARGV != 1) {
  &usage;
}
$DIR = $ARGV[0];
$group = $ARGV[1];
$date = &labeldate;
$count = 0;
#print "Need hmi password to run: passwd =";
print "Are you sure you want to do this [yes/no]: ";
ReadMode('noecho');
$passwd = ReadLine(0);
chomp($passwd);
ReadMode('normal');
print "\n";
if($passwd ne "yes") {
  print "I'm aborting\n";
  exit(1);
}
$user = "production";
$hostdb = "hmidb";      #host where Postgres runs
$cmd = "cat $DIR/tapeid.list";
print "$cmd\n";
@tapeids = `$cmd`;

#First connect to database
  $dbh = DBI->connect("dbi:Pg:dbname=$DB;host=$hostdb;port=$PGPORT", "$user", "");
  if ( !defined $dbh ) {
    die "Cannot do \$dbh->connect: $DBI::errstr\n";
  }
  $dbh->{AutoCommit} = 0;	#no auto commit. requires commit

print "Connected OK to Postgres w/AutoCommit off\n";
while($t = shift(@tapeids)) {
  if(/^#/ || /^\n/) { #ignore any comment or blank lines
    next;
  }
  else {
    #print "$t";
    chomp($t);
    $cmd = "cat $DIR/MD5SUM.$t"."*";
    @name = `$cmd`;
    $lastline = pop(@name);
    ($nxt,$a) = split(/\s/, $lastline);
    $nxtwrtfn = $nxt + 1;
  }
  $sqlcmd = "select tapeid from sum_tape where tapeid='$t'";
  print "SQL: $sqlcmd\n";
  $sth = $dbh->prepare($sqlcmd);
  if ( !defined $sth ) {
    die "Cannot prepare statement: $DBI::errstr\n";
  }
  # Execute the statement at the database level
  $sth->execute;
  $found = 0;
  while(@row = $sth->fetchrow()) {
    #$x = shift(@row);
    #print "row is: $x\n";
    $found = 1;
  }
  if(!$found) {
    #print "No row found for $t\n";
    $sql = "insert into sum_tape values ('$t',$nxtwrtfn,-1,$group,0,2,'$date')";
    print "$sql\n";
  }
  else {
    #print "Existing row for $t\n";
    $sql = "update sum_tape set nxtwrtfn=$nxtwrtfn, group_id=$group, avail_blocks=0, closed=2, last_write='$date' where tapeid='$t'";
  }
  print "$sql\n";
  $sth = $dbh->prepare($sql);
  if ( !defined $sth ) {
    die "Cannot prepare statement: $DBI::errstr\n";
  }
  # Execute the statement at the database level
  $sth->execute;
  $count++;
}
close(ID);

if(defined $sth) {
  $sth->finish;
}

  $dbh->commit;         #if skip this commit, it's all rolled back

$dbh->disconnect();
print "Updated or Inserted $count rows in sum_tape\n";

#Return date in form e.g. 1990-01-01 00:00:00
sub labeldate {
  local($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst,$date,$sec2,$min2,$hour2,$mday2);
  ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
  $sec2 = sprintf("%02d", $sec);
  $min2 = sprintf("%02d", $min);
  $hour2 = sprintf("%02d", $hour);
  $mday2 = sprintf("%02d", $mday);
  $mon2 = sprintf("%02d", $mon+1);
  $year4 = sprintf("%04d", $year+1900);
  $date = "$year4"."-$mon2"."-$mday2"." $hour2".":"."$min2".":"."$sec2";
  return($date);
}

