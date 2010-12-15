eval 'exec /home/jsoc/bin/$JSOC_MACHINE/perl -S $0 "$@"'
    if 0;
use DBI;

$DB = "jsoc_sums";
$PGPORT=5434;

#$user = "jim";
$user = "production";
#$password = "XX";
$hostdb = "hmidb";      #host where Postgres runs
$filename = "/home/production/.pgpass";
open(FI, "$filename") or die "can't open $filename: $!";
while(<FI>) {
  ($a,$b,$c,$d,$password) = split(/:/);
  chomp($password);
  last;
}

#First connect to database
  $dbh = DBI->connect("dbi:Pg:dbname=$DB;host=$hostdb;port=$PGPORT", "$user", "$password");
  if ( !defined $dbh ) {
    die "Cannot do \$dbh->connect: $DBI::errstr\n";
  }

print "Connected to Postgres OK\n";

if(defined $sth) {
  $sth->finish;
}
$dbh->disconnect();
