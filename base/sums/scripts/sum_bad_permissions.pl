#!/usr/bin/perl
#/home/jim/cvs/JSOC/scripts/sum/sum_bad_permissions.pl
#
#Cleans up SUM storage and DB tables for Storage Units with bad permissions
#like was found for:
#d00:/SUM1/D60303> lx
#total 224
#drwxr-sr-x     3 production SOI     54 Sep 27 23:51 ./
#drwxrwsr-x  5083 production SOI 122880 Oct  2 10:19 ../
#-rw-r--r--     1 production SOI     49 Sep 27 23:51 Records.txt
#drwxr-sr-x     2 production SOI      6 Sep 27 23:51 S00000/
#------Sr--     1 production SOI   8728 Sep 27 23:51 set_keys.c
#
#d00:/SUM1/D60218> lx
#total 980
#drwxr-sr-x     3 production SOI     53 Sep 27 16:33 ./
#drwxrwsr-x  5083 production SOI 122880 Oct  2 10:19 ../
#-rw-r--r--     1 production SOI     45 Sep 27 16:33 Records.txt
#drwxr-sr-x     2 production SOI      6 Sep 27 16:33 S00000/
#-------r--     1 production SOI 785752 Sep 27 16:33 show_keys
#
#Usage: sum_bad_permissions.pl input_file
#where input_file has the bad Storage Units listed one per line, e.g.:
#/SUM1/D60303
#/SUM1/D60218
#/SUM5/D60322
#
#NOTE: The bad SU are usually found by the tapearc program failing with
#a permission error when trying to gtar the SU to tape.
#
use DBI;
use Term::ReadKey;

$DB = "jsoc";

sub usage {
  print "Cleans up SUM storage and DB tables for Storage Units with bad permissions\n";
  print "Usage: sum_bad_permissions.pl in_file\n";
  print "The SU to clean up are in the given input file, e.g.:\n";
  print "/SUM1/D60218\n";
  print "/SUM5/D60322\n";
  print "       Requires hmi password to run\n";
  exit(1);
}

if($#ARGV != 0) {
  &usage;
}
$infile = $ARGV[0];
$user = $ENV{'USER'};
if($user ne "production") {
  print "You must be user \'production\' to run sum_bad_permissions.pl\n";
  exit;
}
if(!($PGPORT = $ENV{'SUMPGPORT'})) {
  print "You must have ENV SUMPGPORT set to the port number, e.g. 5430\n";
  exit;
}

print "This is going to modify the SUMS DB tables.\n";
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
open(ID, $infile) || die "Can't open $infile: $!\n";

#connect to database
  $dbh = DBI->connect("dbi:Pg:dbname=$DB;host=$hostdb;port=$PGPORT", "$user", "$password");
  if ( !defined $dbh ) {
    die "Cannot do \$dbh->connect: $DBI::errstr\n";
  }
print "Connected to Postgres OK\n";
while(<ID>) {
  if(/^#/ || /^\n/) { #ignore any comment or blank lines
    next;
  }
  print "$_";
  chomp;
  $cmd = "sudo chmod -R 777 $_";
  print "$cmd\n";
  `$cmd`;
  $cmd = "/bin/rm -rf $_";
  print "$cmd\n";
  `$cmd`;
  $sqlcmd = "delete from sum_main where online_loc = '$_'";
  print "$sqlcmd\n";
  $sth = $dbh->prepare($sqlcmd);
  if ( !defined $sth ) {
    die "Cannot prepare statement: $DBI::errstr\n";
  }
  # Execute the statement at the database level
  $sth->execute;
  $sqlcmd = "delete from sum_partn_alloc where wd = '$_'";
  print "$sqlcmd\n";
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
