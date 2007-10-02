#!/usr/bin/perl
#(original for Oracle) hmidb:/home/ora10/SUM/sum_tape_insert.pl
#
#Takes a file of tapeids for the t120 and inserts them into the sum_tape
#table of the given DB with the appropriate initial values. 
#
use DBI;
use Term::ReadKey;

$DB = "jsoc";

#$IDTBL = "/home/ora10/SUM/tapeid.list"; # our initial set of tapes
#$IDTBL = "/home/jim/cvs/jsoc/scripts/sum/tapeid.list"; #for t120

sub usage {
  print "Initialize the sum_tape table for database = $DB (edit \$DB to change).\n";
  print "The tapeids to use are in the given input file\n";
  print "Usage: sum_tape_insert.pl in_file\n";
  print "       Requires hmi password to run\n";
  exit(1);
}

if($#ARGV != 0) {
  &usage;
}
$IDTBL = $ARGV[0];
print "Need hmi password to run: passwd =";
ReadMode('noecho');
$passwd = ReadLine(0);
chomp($passwd);
ReadMode('normal');
print "\n";
if($passwd ne "hmi4sdo") {
  print "Invalid passwd\n";
  exit(1);
}
$user = "jim";
$password = "jimshoom";
$hostdb = "hmidb";      #host where Postgres runs

#First connect to database
  $dbh = DBI->connect("dbi:Pg:dbname=$DB;host=$hostdb", "$user", "$password");
  if ( !defined $dbh ) {
    die "Cannot do \$dbh->connect: $DBI::errstr\n";
  }

print "Connected to Postgres OK\n";
open(ID, $IDTBL) || die "Can't open $IDTBL: $!\n";
while(<ID>) {
  if(/^#/ || /^\n/) { #ignore any comment or blank lines
    next;
  }
  print "$_";
  chomp;
  $sqlcmd = "insert into sum_tape (TAPEID,NXTWRTFN,SPARE,GROUP_ID,AVAIL_BLOCKS,CLOSED) values ('$_',1,-1,-1,1000000000,-1)";
  print "SQL: $sqlcmd\n";
  $sth = $dbh->prepare($sqlcmd);
  if ( !defined $sth ) {
    die "Cannot prepare statement: $DBI::errstr\n";
  }
  # Execute the statement at the database level
  $sth->execute;
}
close(ID);

if(defined $sth) {
  $sth->finish;
}
$dbh->disconnect();
