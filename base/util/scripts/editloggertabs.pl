#!/home/jsoc/bin/linux_x86_64/perl -w

use IO::Dir;
use FileHandle;
use File::stat;
use Fcntl ':flock';

use DBI;
use DBD::Pg;

# Exit codes
use constant kSuccess => 0;
use constant kInvalidArg => 1;
use constant kCantUpdate => 2;
use constant kAlreadyRunning => 3;
use constant kCantDrop => 4;
use constant kCantCreate => 5;
use constant kCantDelete => 6;

# Main table enum
use constant kMainModname => 0;
use constant kMainSubmodname => 1;
use constant kMainTablename => 2;
use constant kMainWarninterval => 3;
use constant kMainLastwarn => 4;

# Sub table enum
use constant kSubSubmodname => 0;
use constant kSubDate => 1;
use constant kSubLogDate => 2;
use constant kSubExec => 3;
use constant kSubStatus => 4;
use constant kSubMessage => 5;
use constant kSubUser => 6;
use constant kSubMachine => 7;
use constant kSubPid => 8;
use constant kSubPpid => 9;

# Main logger table
# This table contains a list of all project-specific tables that the logger is monitoring.
#
# Prime key: modname,submodname
#
# modname - the name of the project that is being monitored by the status-monitoring code.
# submodname - the name of the sub-project
# tablename - the name of the project-specific database table.
# warninterval - the interval of time that must elapse between the last time a warning
#   was sent and the next time a warning is sent.
# lastwarn - the timestamp of the last time that a warning was issued.

my(@headersmain) = qw(modname submodname tablename warninterval lastwarn);

# Project-specific tables
# These tables contain project-specific log messages.
#
# Prime key: submodname
#
# submodname - the name of the sub-project
# date - the record-insertion date (when the data from the project's log was added to the 
#    project-specific table).
# logdate - the date that the log entry was created.
# exec - the executable that generates the log messages.
# status - the status of the project (did it fail? did it run successfully).
# message - a message corresponding to and explaining the status.
# username - the linux account of the user running the executable.
# machine - the machine on which the executable was running.
# pid - the process ID of the executable instance.
# ppid - the parent process ID of the executable instance.

my(@headerssub) = qw(submodname date logdate exec status message username machine pid ppid);

my($lckfh);
my($gotlock);
my($nolock);
my($dsn);
my($dbh);
my($sth);
my($conftable);
my($stmnt);
my($rrows);
my($row);
my(@rows);
my(%unarchLogs);
my(%archLogs);
my($tableExists);

my($dbname);
my($dbhost);
my($dbport);
my($dbuser);
my($schemaname);
my($tablename);
my($tarbin);
my($gzbin);
my($action);
my($addfile);
my($pkeyval);
my(@addrow);

# Allow only one instance of this program 
unless (flock(DATA, LOCK_EX|LOCK_NB)) 
{
   print "$0 is already running. Exiting.\n";
   exit(kAlreadyRunning);
}

# Read cmd-line arguments
if ($#ARGV != 7)
{
   print STDERR "Improper argument list.\n";
   exit(kInvalidArg);
}

$dbname = $ARGV[0];
$dbhost = $ARGV[1];
$dbport = $ARGV[2];
$dbuser = $ARGV[3];
$schemaname = $ARGV[4];
$tablename = $ARGV[5]; 
$tabletype = $ARGV[6]; # main or sub
$action = $ARGV[7]; # 'add=<filename>', 'add=A,B,C,...', 'del=<prime-key value>' 'disp', 'create', 'drop'

$conftable = "$schemaname\.$tablename";

# Parse action
if ($action =~ /^add=(.+)/i)
{
   $addfile = $1;
   if (!(-e $addfile))
   {
      @addrow = split(/,/, $addfile);
      $addfile = "";
      $action = "addrow";
   }
   else
   {
      $action = "addfile";
   }
}
elsif ($action =~ /^del=(.+)/i)
{
   $pkeyval = $1;
   $action = "del";
}
elsif ($action !~ /^del$/i && $action !~ /^disp$/i && $action !~ /^create$/i && $action !~ /^drop$/i)
{
   print STDERR "Invalid action argument '$action'.\n";
   exit kInvalidArg;
}
elsif ($tabletype !~ /^main$/i && $tabletype !~ /^sub$/i)
{
   print STDERR "Invalid tabletype argument '$tabletype'.\n";
   exit kInvalidArg;
}

$action = lc($action);
$tabletype = lc($tabletype);

# connect to the database
$dsn = "dbi:Pg:dbname=$dbname;host=$dbhost;port=$dbport";
print "Connection to database with '$dsn' as user '$dbuser' ... ";

# Despite ALL documentation saying otherwise, it looks like the error codes/string
# provided by DBI are all UNDEFINED, unless there is some kind of failure. So, 
# never try to look at $dbh->err or $dbh->errstr if the call succeeded. Of course,
# you can't tell if the call succeeded unless you look at $dbh->err. Stupid.
$dbh = DBI->connect($dsn, $dbuser, ''); # password should be provided by .pgpass

if (defined($dbh))
{
   $tableExists = 0;

   print "success!\n";

   # Check for table existence
   $stmnt = "SELECT * FROM information_schema.tables WHERE table_schema = '$schemaname' AND table_name = '$tablename'";
   $rrows = $dbh->selectall_arrayref($stmnt, undef);

   if (NoErr($rrows, \$dbh, $stmnt))
   {
      my(@rows) = @$rrows;

      if ($#rows == 0)
      {
         # Table exists
         $tableExists = 1;
      }
   }

   if (!$tableExists)
   {
      if ($action eq "create")
      {
         if (!CreateTable(\$dbh, $conftable, $tabletype))
         {
            print STDERR "Unable to create table '$conftable'.\n";
            $rv = kCantCreate;
         }
         else
         {
            print "Successfully created $tabletype table '$conftable'.\n";
         }
      }
      else
      {
         print STDERR "Table $schemaname\.$tablename does not exist - nothing to do.\n";
         $rv = kInvalidArg;
      }
   }
   else
   {
      # Configuration table does exist

      if ($action eq "addrow")
      {
         if (!AddRecord(\$dbh, $conftable, $tabletype, @addrow))
         {
            print STDERR "Unable to add the following record to '$conftable':\n";
            print STDERR "@addrow\n";
            $rv = kInvalidRow;
         }
         else
         {
            print "Successfully added one row to '$conftable'.\n";
         }
      }
      elsif ($action eq "addfile")
      {
         # Open file and call AddRecord() for each row
         if (open(ADDFILE, "<$addfile"))
         {
            my(@onerow);
            my($line);
            my($atleastone) = 0;

            while (defined($line = <ADDFILE>))
            {
               chomp($line);
               @onerow = split(/,/, $line);
               if (!AddRecord(\$dbh, $conftable, $tabletype, @onerow))
               {
                  print STDERR "Unable to add the following record to '$conftable':\n";
                  print STDERR "$line\n";
                  print STDERR "Continuing with the next line in '$addfile'.\n";
               }
               else
               {
                  $atleastone = 1;
               }
            }

            if (!$atleastone)
            {
               $rv = kInvalidRow;
            }
            else
            {
               print "Successfully added at least one row to '$conftable'.\n";
            }

            close(ADDFILE);
         }
         else
         {
            print STDERR "Unable to read input file '$addfile'.\n";
            $rv = kFileIO;
         }
      }
      elsif ($action eq "del")
      {
         if (!DelRecord(\$dbh, $conftable, $tabletype, $pkeyval))
         {
            print STDERR "Unable to delete row whose prime key is '$pkeyval' from $tabletype table.\n";
            $rv = kCantDelete;
         }
         else
         {
            print "Successfully deleted one row from '$conftable'.\n";
         }
      }
      elsif ($action eq "create")
      {
         print STDERR "Configuration table '$conftable' already exists.\n";
         $rv = kInvalidArg;
      }
      elsif ($action eq "drop")
      {
         if (!DropTable(\$dbh, $conftable))
         {
            print STDERR "Unable to delete configuration table.\n";
            $rv = kCantDrop;
         }
         else
         {
            print "Successfully deleted $tabletype table '$conftable'.\n"
         }
      }
      elsif ($action eq "disp")
      {
         DisplayTable(\$dbh, $conftable, $tabletype);
      } 
      else
      {
         print STDERR "Invalid action '$action'.\n";
         $rv = kInvalidArg;
      }
   }

   $dbh->disconnect();
}
else
{
   print STDERR "Failure connecting to database: '$DBI::errstr'\n";
}

# FIN

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

sub CreateTable
{
   my($dbh, $conftable, $tabletype) = @_;
   my($stmnt);
   my($rv);
   my($res);

   $rv = 1;

   if ($tabletype eq "main")
   {
      $stmnt = "CREATE TABLE $conftable ($headersmain[kMainModname] text NOT NULL, $headersmain[kMainSubmodname] text NOT NULL, $headersmain[kMainTablename] text NOT NULL, $headersmain[kMainWarninterval] interval DEFAULT '1 day', $headersmain[kMainLastwarn] timestamp with time zone DEFAULT '1966-12-25 00:54:00 PST', PRIMARY KEY ($headersmain[kMainModname],$headersmain[kMainSubmodname]))";
   } 
   elsif ($tabletype eq "sub")
   {
      $stmnt = "CREATE TABLE $conftable ($headerssub[kSubSubmodname] text NOT NULL, $headerssub[kSubDate] timestamp with time zone DEFAULT '1966-12-25 00:54:00 PST', $headerssub[kSubLogDate] timestamp with time zone DEFAULT '1966-12-25 00:54:00 PST', $headerssub[kSubExec] text DEFAULT 'Unknown', $headerssub[kSubStatus] text DEFAULT 'Unknown', $headerssub[kSubMessage] text DEFAULT 'NA', $headerssub[kSubUser] text DEFAULT 'Unknown', $headerssub[kSubMachine] text DEFAULT 'Unknown', $headerssub[kSubPid] integer DEFAULT -1, $headerssub[kSubPpid] integer DEFAULT -1, PRIMARY KEY ($headerssub[kSubSubmodname],$headerssub[kSubDate]))";
   } 
   else
   {
      print STDERR "Unsupported table type '$tabletype'.\n";
      $rv = 0;
   }

   if ($rv)
   {
      $res = $$dbh->do($stmnt);
      if (!NoErr($res, $dbh, $stmnt))
      {
         $rv = 0;
      }
   }

   return $rv;
}

sub DropTable
{
   my($dbh, $conftable) = @_;
   my($stmnt);
   my($rv);
   my($res);

   $rv = 1;

   $stmnt = "DROP TABLE $conftable";
   $res = $$dbh->do($stmnt);
    if (!NoErr($res, $dbh, $stmnt))
   {
      $rv = 0;
   }

   return $rv;
}

sub DisplayTable
{
   my($dbh, $conftable, $tabletype) = @_;
   my($stmnt);
   my($rrows);

   if ($tabletype eq "main")
   {
      $stmnt = "SELECT $headersmain[kMainModname], $headersmain[kMainSubmodname], $headersmain[kMainTablename], $headersmain[kMainWarninterval], $headersmain[kMainLastwarn] from $conftable";
   }
   elsif ($tabletype eq "sub")
   {
      $stmnt = "SELECT $headerssub[kSubSubmodname], $headerssub[kSubDate], $headerssub[kSubLogDate], $headerssub[kSubExec], $headerssub[kSubStatus], $headerssub[kSubMessage], $headerssub[kSubUser], $headerssub[kSubMachine], $headerssub[kSubPid], $headerssub[kSubPpid] from $conftable";
   }
   else
   {
      print STDERR "";
      return;
   }

   $rrows = $$dbh->selectall_arrayref($stmnt, undef);

   if (NoErr($rrows, $dbh, $stmnt))
   {
      my($ptbuf);

      # Print header
      if ($tabletype eq "main")
      {
         $ptbuf = sprintf("%32s%32s%32s%16s%48s", $headersmain[kMainModname], $headersmain[kMainSubmodname], $headersmain[kMainTablename], $headersmain[kMainWarninterval], $headersmain[kMainLastwarn]);
      
         print "$ptbuf\n";
         print "----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n";

         if ($#$rrows >= 0)
         {
            foreach $row (@$rrows)
            {
               my($modname, $submodname, $tabname, $warnint, $lastwarn) = @$row;
               $ptbuf = sprintf("%32s%32s%32s%16s%48s", $modname, $submodname, $tabname, $warnint, $lastwarn);
               print "$ptbuf\n";
            }
         }
      }
      elsif ($tabletype eq "sub")
      {
         $ptbuf = sprintf("%32s%32s%32s%64s%16s%128s%16s%16s%16s%16s", $headerssub[kSubSubmodname], $headerssub[kSubDate], $headerssub[kSubLogDate], $headerssub[kSubExec], $headerssub[kSubStatus], $headerssub[kSubMessage], $headerssub[kSubUser], $headerssub[kSubMachine], $headerssub[kSubPid], $headerssub[kSubPpid]);

         print "$ptbuf\n";
         print "---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n";

         if ($#$rrows >= 0)
         {
            foreach $row (@$rrows)
            {
               my($submodname, $date, $logdate, $exec, $lstatus, $lmsg, $user, $machine, $pid, $ppid) = @$row;

#print "1-$submodname, 2-$date, 3-$logdate, 4-$exec, 5-$lstatus, 6-$lmsg, 7-$user, 8-$machine, 9-$pid, 10-$ppid\n";

               $ptbuf = sprintf("%32s%32s%32s%64s%16s%128s%16s%16s%16s%16s", $submodname, $date, $logdate, $exec, $lstatus, $lmsg, $user, $machine, $pid, $ppid);
               print "$ptbuf\n";
            }
         }
      }
   }
}

sub AddRecord
{
   my($ok);
   my($dbh, $conftable, $tabletype, @rowin) = @_;
   my(@row);
   my(@colnames);
   my($stmnt);
   my($rv);
   my($ival);
   my($rowstr);
   my($colnamesstr);

   $ok = 1;
   $ival = 0;

   if ($tabletype eq "main")
   {
      foreach my $item (@rowin)
      {
         if (defined($item))
         {
            push(@row, $item);
            push(@colnames, $headersmain[$ival]);
         }

         $ival++;
      }
   }
   elsif ($tabletype eq "sub")
   {
      foreach my $item (@rowin)
      {
         if (defined($item))
         {
            push(@row, $item);
            push(@colnames, $headerssub[$ival]);
         }

         $ival++;
      }
   }
   else
   {
      print STDERR "Unsupported table type '$tabletype'.\n";
      $ok = 0;
   }

   if ($ok)
   {
      # Perl mumbo jumbo - $" identifies the delimiter used when converting arrays
      # to strings.
      $" = ',';
      $rowstr = "@row";
      $colnamesstr = "@colnames";

      $stmnt = "INSERT into $conftable ($colnamesstr) VALUES ($rowstr)";
      $rv = $$dbh->do($stmnt);
      print "$stmnt\n";
      $ok = NoErr($rv, $dbh, $stmnt);
   }

   return $ok;
}

sub DelRecord
{
   my($dbh, $conftable, $tabletype, $pkeyvalstr) = @_;
   my($ok);
   my($pkeywhere);
   my($stmnt);
   my(@pkeyval);

   @pkeyval = split(/,/, $pkeyvalstr);

   $ok = 1;

   if ($tabletype eq "main")
   {
      if ($#pkeyval == 1)
      {
         $pkeywhere = "$headersmain[kMainModname] = '$pkeyval[0]' AND $headersmain[kMainSubmodname] = '$pkeyval[1]'";
      }
      else
      {
         print STDERR "Invalid prime key value '$pkeyvalstr'.\n";
         $ok = 0;
      }
   }
   elsif ($tabletype eq "sub")
   {
      if ($#pkeyval == 1)
      {
         $pkeywhere = "$headerssub[kSubSubmodname] = '$pkeyval[0]' AND $headerssub[kSubDate] = '$pkeyval[1]'";
      }
      else
      {
         print STDERR "Invalid prime key value '$pkeyvalstr'.\n";
         $ok = 0;
      }
   }
   else
   {
      print STDERR "Unsupported table type '$tabletype'.\n";
      $ok = 0;
   }

   if ($ok)
   {
      $stmnt = "DELETE from $conftable WHERE $pkeywhere";

      $rv = $$dbh->do($stmnt);

      # The lame DBD:Pg documentation is actually correct here - if no rows were deleted, 
      # $rv will be the string '0E0'
      print "$stmnt\n";

      $ok = NoErr($rv, $dbh, $stmnt);

      if (defined($rv) && $rv eq '0E0')
      {
         # No rows deleted, so the pkeyvalstr provided was invalid.
         $ok = 0;
      }  
   }

   return $ok;
}

__DATA__
