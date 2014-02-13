eval 'exec /home/jsoc/bin/$JSOC_MACHINE/perl -S $0 "$@"'
    if 0;
#
#This will find all the /SUMs partitions on a given server, e.g. d04,
#for a given time period.
#
#sum_time_server_scan.pl [-w] [-sd04] 2014.02.12_00:00:00 2014.02.12_00:06:00
#
#If no -s given, will search all /SUM partitions.
#Range is inclusive local time.
#
use DBI;

sub usage {
  print "Find all the /SUMs partitions on a server for a given time\n";
  print "sum_time_server_scan.pl [-w] [-sd04] 2014.02.12_00:00:00 2014.02.12_00:06:00\n";
  print "       -s = The /SUM on this server, e.g. d04\n";
  print "            An empty arg will search all servers.\n";
  print "       -w = Output the working dir only.\n"; 
  print "Range is inclusive local time\n";
  exit(1);
}

$DBNAME = "jsoc_sums";
$DBHOST = "hmidb3";  #decided not to use drmsparams for portability
$PGPORT = 5434;
$DBUSER = "production"; 

print "$0 ";
$cnt = $#ARGV;
for($i =0; $i <= $cnt; $i++) {
  print "$ARGV[$i] ";
}
print "\n";
while ($ARGV[0] =~ /^-/) {
  $_ = shift;
  if (/^-s(.*)/) {
    $server = $1;
  }
  if (/^-w(.*)/) {
    $wflg = 1;
  }
}
if($#ARGV != 1) { &usage; }
else {
  $starttime = $ARGV[0];
  $endtime = $ARGV[1];
}

if($server) {	#find the /SUM on the given server
  $cmd = "df /SUM*";
  @df = `$cmd`;
  @localdf = grep(/^$server/, @df);
  while($x = shift(@localdf)) {
    ($y, $part) = split(/:/, $x);
    ($sump, $x) = split(/\s+/, $part);
    push(@sparts, $sump);
  }
}

  #connect to the databases
  $dbh = DBI->connect("dbi:Pg:dbname=$DBNAME;host=$DBHOST;port=$PGPORT ", "$DBUSER", "");
  if ( !defined $dbh ) {
    die "Cannot do \$dbh->connect: $DBI::errstr\n";
  }
  #$sql = "select online_loc, owning_series, bytes, ds_index, creat_date, username  from sum_main where ds_index=524678104"; #!!TEMP for test

  $sql = "select online_loc, owning_series, bytes, ds_index, creat_date, username  from sum_main where creat_date >= '$starttime' and  creat_date <= '$endtime'";
  #print "$sql\n";

      $sth = $dbh->prepare($sql);
      if ( !defined $sth ) {
        print "Cannot prepare statement: $DBI::errstr\n";
        $dbh->disconnect();
        exit;
      }
      # Execute the statement at the database level
      $sth->execute;
      print "   online_loc   | series | bytes    | ds_index| creat_date    |   username\n";
      while ( @row = $sth->fetchrow() ) {
        @rowsv = @row;
        $x = shift(@row);
        if($server) {
          ($a,$wd,$c) = split(/\//, $x);
          if(grep(/$wd/, @sparts)) {
            if($wflg) {
              print "$x\n";
            } else{ print "@rowsv\n"; }
          }
        }
        else { print "@rowsv\n"; }
      }


$dbh->disconnect();

