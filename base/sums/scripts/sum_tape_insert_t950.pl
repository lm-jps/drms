eval 'exec /home/jsoc/bin/$JSOC_MACHINE/perl -S  $0 "$@"'
    if 0;
#sum_tape_insert_t950.pl
#
#Takes a file of tapeids for the t950 and inserts them into the sum_tape
#table of the given DB with the appropriate initial values. 
#
use DBI;
use Term::ReadKey;

#$DB = "jsoc";
$DB = "jim";

#$IDTBL = "/home/ora10/SUM/tapeid.list"; # our initial set of tapes
#$IDTBL = "/home/jim/cvs/JSOC/base/sums/script/tapeid.list"; #for t120

sub usage {
  print "Initialize the sum_tape table for database = $DB (edit \$DB to change).\n";
  print "The tapeids to use are in the given input file\n";
  print "Usage: sum_tape_insert_t950.pl in_file\n";
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
  if(/^tapeid /) {	#accept format from tape_svc log
   ($a, $b, $c, $d, $e, $f) = split(/ /);
   print "$f\n";
   $_ = $f;
  }
  else {
    print "$_";
    chomp;
  }
  $sqlcmd = "insert into sum_tape (TAPEID,NXTWRTFN,SPARE,GROUP_ID,AVAIL_BLOCKS,CLOSED) values ('$_',1,-1,-1,1600000000,-1)";
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
