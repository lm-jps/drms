#!/home/jsoc/bin/linux_x86_64/perl -w

# Creates a new DRMS user:
#  1. creates a new db user with name specified by $dbuser
#  2. creates a new db namespace with name specific by $dbns (should be su_$dbuser in most cases).
#  3. sets default namespace for db user $dbuser (should be su_$dbuser in most cases)

# template cmd-line:
#  newdrmsuser.pl jsoc hmidb 5432 arta changeme su_arta user 0

use DBI;
use DBD::Pg;

my($dbname);    # name of the db instance to connect to
my($dbhost);    # name of the db host on which the db instance resides
my($dbport);    # port on $dbhost through which connections are made
my($dbuser);    # database user name (to create)
my($dbpass);    # database password (to create)
my($dbns);      # database namespace (to create with the masterlists call)
my($dbnsgroup); # totally unused, but it must be provided in the cmd-line
my($doit);

my($dbh);       # perl db handle
my($dsn);       # string thingy that identifies the db to connect to
my($stmnt);
my($res);
my($cmd);

$#ARGV == 7 || die "Improper argument list.\n";

$dbname = $ARGV[0];
$dbhost = $ARGV[1];
$dbport = $ARGV[2];
$dbuser = $ARGV[3];
$dbpass = $ARGV[4];
$dbns = $ARGV[5];
$dbnsgroup = $ARGV[6];
$doit = $ARGV[7];

# connect to the database
$dsn = "dbi:Pg:dbname=$dbname;host=$dbhost;port=$dbport";
print "Connection to database with '$dsn' as user '$dbuser' ... ";

# Despite ALL documentation saying otherwise, it looks like the error codes/string
# provided by DBI are all UNDEFINED, unless there is some kind of failure. So, 
# never try to look at $dbh->err or $dbh->errstr if the call succeeded.
$dbh = DBI->connect($dsn, 'postgres', ''); # will need to put pass in .pg_pass

if (defined($dbh))
{
   print "success!\n";

   # create user $dbuser
   $stmnt = "CREATE USER $dbuser";
   ExecStatment(\$dbh, $stmnt, $doit, "Unable to create db user $dbuser.\n");

   $stmnt = "ALTER USER $dbuser WITH password '$dbpass'";
   ExecStatment(\$dbh, $stmnt, $doit, "Unable to assign default password to $dbuser.\n");

   $stmnt = "GRANT jsoc to $dbuser";
   ExecStatment(\$dbh, $stmnt, $doit, "Unable to add $dbuser to db group jsoc.\n");

   # run masterlists
   $cmd = "masterlists dbuser=$dbuser namespace=$dbns nsgrp=$dbnsgroup";
   print "running cmd-line ==> $cmd\n";
   
   if ($doit)
   {
      system($cmd);

      if ($? == -1)
      {
         die "Failed to execute '$cmd'.\n";
      }
      elsif ($? & 127)
      {
         die "masterlists crashed.\n";
      }
      elsif ($? >> 8 != 0)
      {
         die "masterlists ran unsuccessfully.\n";
      }
   }

   # assign default namespace
   $stmnt = "INSERT INTO admin.sessionns VALUES ('$dbuser', '$dbns')";
   ExecStatment(\$dbh, $stmnt, $doit, "Unable to assign default session namespace.\n");

   $dbh->disconnect();
}
else
{
   print "failure!!!!\n";
}

# AL FINAL

sub NoErr
{
   my($rv) = $_[0];
   my($dbh) = $_[2];
   my($stmnt) = $_[2];
   my($ok) = 1;

   if (!defined($rv) || !$rv)
   {
      if (defined($$dbh) && defined($$dbh->err))
      {
         print STDERR "Error " . $$dbh->errstr . ": Statement '$stmnt' failed.\n";
      }

      $ok = 0;
   } 

   return $ok;
}

sub ExecStatment
{
   my($dbh, $stmnt, $doit, $msg) = @_;
   my($res);

   print "executing db statment ==> $stmnt\n";

   if ($doit)
   {
      $res = $$dbh->do($stmnt);
      NoErr($res, $dbh, $stmnt) || die $msg;
   }
}
