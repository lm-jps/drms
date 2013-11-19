eval 'exec /home/jsoc/bin/$JSOC_MACHINE/perl -S $0 "$@"'
    if 0;
#/home/production/cvs/JSOC/base/sums/scripts/tape_by_ds_shelf.pl
#
#Normally run as a cron job by production on n02:
#45 2 * * * /home/production/cvs/JSOC/base/sums/scripts/tape_by_ds_shelf.pl jsoc_sums

use DBI;
use FindBin qw($RealBin);
use lib "$RealBin/../../../localization";
use drmsparams;

#use POSIX ":sys_wait_h";

#query ds name for tapes
#NOTE: also must get all ds hmi > lev1
@dsnames = ('hmi.tlm', 'aia.tlm', 'hmi.lev0a', 'aia.lev0', 'hmi.lev1');
#group# array must correspond to @dsnames above
@dsgroups = (4, 5, 2, 3, 10);
$LOG = "/home/production/cvs/JSOC/base/sums/scripts/gChart_data/tape_by_ds_shelf.log";

sub usage {
  print "Get the # of tapes in_robot, and on_shelf for these datasets:\n";
  print "@dsnames\n";
  print "Usage: tape_by_ds_shelf.pl [-h] db_name (e.g. jsoc_sums)\n";
  print "       The default db_host is $HOSTDB\n";
  #print "\nYou must have ENV SUMPGPORT set to the port number, e.g. 5434\n";
  exit(1);
}

#Return date in form for a label e.g. 1998.01.07_14:42:04
#Also set effective_date ($effdate) in form 199801071442
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

  $host = `hostname -s`;
  chomp($host);
  if($host ne "d02") {
    print "Error: This must be run on d02 (where the T950 is)\n";
    exit;
  }

$HOSTDB = drmsparams::SUMS_DB_HOST;      #host where DB runs
$PGPORT = 5434;

while ($ARGV[0] =~ /^-/) {
  $_ = shift;
  if (/^-h(.*)/) {
    &usage;
  }
#  if (/^-f(.*)/) {
#    $FAST = 1;
#  }
#  if (/^-x(.*)/) {
#    $execute = 1;
#  }
}

if($#ARGV != 0) {
  &usage;
}
$DB = $ARGV[0];
$ldate = &labeldate();
$logfile =substr($ldate, 0, 10);
$logfile = "$LOG."."$logfile";
open(LG, ">$logfile") || die "Can't open $logfile: $!\n";

print "tape_by_ds_shelf.pl run on $ldate\n";
print LG "#tape_by_ds_shelf.pl run on $ldate\n";
$hostdb = $HOSTDB;      #host where Postgres runs
$user = $ENV{'USER'};
$ENV{'SUMPGPORT'} = $PGPORT;
#if(!($PGPORT = $ENV{'SUMPGPORT'})) {
#  print "You must have ENV SUMPGPORT set to the port number, e.g. 5434\n";
#  exit;
#}

#First connect to database
  $dbh = DBI->connect("dbi:Pg:dbname=$DB;host=$hostdb;port=$PGPORT", "$user", "");
  if ( !defined $dbh ) {
    die "Cannot do \$dbh->connect: $DBI::errstr\n";
  }
@dsnames2 = @dsnames;
while($ds = shift(@dsnames)) {
  $pos = index($ds, '.');
  $name1 = substr($ds, 0, $pos);
  $name2 = substr($ds, $pos+1);
  $name = $name1.$name2;
  push(@names, $name);
  $grp = shift(@dsgroups);
  #must query sum_main for owning_series, e.g. hmi.lev1 is both grp 6 and 10
  $sql = "select distinct arch_tape from sum_main where owning_series='$ds'";
  #$sql = "select tapeid from sum_tape where group_id=$grp and closed=2";
  print "$sql\n";
  print LG "#$sql\n";
  $sth = $dbh->prepare($sql);
  if ( !defined $sth ) {
    print "Cannot prepare statement: $DBI::errstr\n";
    $dbh->disconnect();
    exit;
  }
  $sth->execute;
  #$i = 0;
  while ( @row = $sth->fetchrow() ) {
    $_ = shift(@row);
    if(!$_){ next; }
    if(/^#/ || /^\n/) { #ignore any comment or blank lines
      next;
    }
    #print "$_\n"; #!!TEMP
    push(@$name, $_);
  }
}
#And finally do for hmi > lev1 group 6 after 2011-04-07. Before this there
#were mixed groups on the tapes. 
$name = "hmi>lev1";
push(@names, $name);
push(@dsnames2, $name);
$sql = "select tapeid from sum_tape where group_id=6 and closed=2 and last_write > '2011-04-07'";
print "$sql\n";
print LG "#$sql\n";
  $sth = $dbh->prepare($sql);
  if ( !defined $sth ) {
    print "Cannot prepare statement: $DBI::errstr\n";
    $dbh->disconnect();
    exit;
  }
  $sth->execute;
  while ( @row = $sth->fetchrow() ) {
    $_ = shift(@row);
    if(!$_){ next; }
    if(/^#/ || /^\n/) { #ignore any comment or blank lines
      next;
    }
    #print "$_\n"; #!!TEMP
    push(@$name, $_);
  }

@mtx = `/usr/sbin/mtx -f /dev/t950 status`;
#print LG "mtx:\n@mtx\n"; #!!TEMP
if(!@mtx) {  #command failed
  @mtx = `/usr/sbin/mtx -f /dev/t950 status`;
}
print "\nClosed tapes in and out of robot:\n";
print LG "\n#Closed tapes in and out of robot:\n";
while($n = shift(@names)) {
  $countin = $countout = 0;
  #print "$n:\n";
  #print LG "#$n:\n";
  while($t = shift(@$n)) {
    #print "$t\n";
    if(grep(/$t/, @mtx)) {
      #print "Tape $t is in T950\n";
      #print LG "Tape $t is in T950\n";
      $countin++;
    }
    else {
      #print "Tape $t is not in T950\n";
      #print LG "Tape $t is not in T950\n";
      $countout++;
    }
  }
  $dsname = shift(@dsnames2);
  print "Total for $dsname tapes in robot=$countin  tapes out of robot=$countout\n";
  print LG "Total for $dsname tapes in robot=$countin  tapes out of robot=$countout\n";
}
close(LG);

