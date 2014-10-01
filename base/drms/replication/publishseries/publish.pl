#!/home/jsoc/bin/linux_x86_64/perl -w

# publish.pl ~arta/cvs/JSOC/proj/replication/etc/repserver.cfg su_arta.blah 0

use FileHandle;
use File::Copy;
use IO::Dir;
use Fcntl ':flock';
use FindBin qw($Bin);
use lib "$Bin/..";
use toolbox qw(GetCfg);
use DBI;
use DBD::Pg;
use lib "$RealBin/../../../localization";
use drmsparams;

use constant kSuccess => 0;
use constant kInvalidArg => 1;
use constant kDbConnection => 2;
use constant kInvalidSQL => 3;
use constant kRunCmd => 4;
use constant kFileIO => 5;
use constant kSlonikFailed => 6;
use constant kSeriesNotFound => 7;
use constant kDrmsParams => 8;

$| = 1; # autoflush

# This script takes 2 arguments: the path to the server-side configuration file, and a series name.
my($conf);
my($series);
my($schema);
my($drmsParams);
my($oksub);
my($table);
my($ns);
my($tab);
my($rv);
my(@cfgdata);
my(%cfg);
my($lckfh);
my($parselck);

$rv = kSuccess;

if ($#ARGV != 2)
{
   PrintUsage();
   $rv = kInvalidArg;
}
else
{
   $conf = $ARGV[0];
   $series = $ARGV[1];
   $oksub = $ARGV[2];

   if (-e $conf)
   {
      # fetch needed parameter values from configuration file
      if (toolbox::GetCfg($conf, \%cfg))
      {
         $rv = kInvalidArg;
      }
   } 
   else
   {
      $rv = kInvalidArg;
   }

   ($schema, $table) = ($series =~ /(\S+)\.(\S+)/);
}

if ($rv == &kSuccess)
{
    $drmsParams = new drmsparams();
    
    if (!defined($drmsParams))
    {
        print STDERR "ERROR: Cannot get DRMS parameters.\n";
        $rv = &kDrmsParams;
    }
}

if ($rv == kSuccess)
{
   # Acquire publication lock. This prevents more than one publication 
   # script from simultaneously running.
   $parselck = "$cfg{'kServerLockDir'}/$cfg{'kPubLockFile'}";
   if (toolbox::AcquireLock($parselck, \$lckfh))
   {
      # Connect to the db
      my($mdbh);   # master db handle
      my($sdbh);   # slave db handle
      my($dbname);
      my($dbhost);
      my($dbport);
      my($dbuser);
      my($dsn);
      my($stmnt);
      my($sqlpath);
      my($checktab);
      my($res);
      my($subid);
      my($repsetid);

      $dbname = $cfg{'MASTERDBNAME'};
      $dbhost = $cfg{'MASTERHOST'};
      $dbport = $cfg{'MASTERPORT'};
      $dbuser = $cfg{'REPUSER'};
      $sqlpath = $cfg{'REP_PS_TMPDIR'} . "/createtab_$series.sql";

      if (-e $sqlpath)
      {
         if (!unlink($sqlpath))
         {
            # Error deleting file
            print STDERR "Unable to delete sql file '$sqlpath'; error code $!.; ABORTING!\n";
            $rv = kFileIO;
         }
      }
     
      if ($rv == kSuccess)
      {
         # The host address from the configuration file is accessible only from hmidb and hmidb2.
         # So this script will not run anywhere else.
         $dsn = "dbi:Pg:dbname=$dbname;host=$dbhost;port=$dbport";
         $mdbh = DBI->connect($dsn, $dbuser, ''); # password should be provided by .pgpass
         print("Connecting to master database with '$dsn' as user '$dbuser'...");
      }

      if (defined($mdbh))
      {
         print("success!\n");
         print "Checking to see if the series to be published exists on the master.\n";

         $res = TableExists(\$mdbh, $schema, $table);
         if ($res == -1)
         {
            print "Unable to check for existence of $series on master.\n";
            $rv = kInvalidSQL;
         }
         elsif (!$res)
         {
            print "The series $series does not appear to exist. ABORTING!\n";
            $rv = kInvalidArg;
         } 
         else
         {
            print "The series $series exists, continuing...\n";
         }
      }
      else
      {
         print("failure!\n");
         print STDERR "Failure connecting to database: '$DBI::errstr'\n";
         $rv = kDbConnection;
      }

      if ($rv == kSuccess)
      {
         print "Checking to see if the series to be published has already been published.\n";
         
         $res = TableIsReplicated(\$mdbh, $schema, $table, $cfg{'CLUSTERNAME'});
         if ($res == -1)
         {
            print STDERR "Unable to check for previous publication of $series.\n";
            $rv = kInvalidSQL;
         }
         elsif ($res)
         {
            print "The series $series has already been published. ABORTING!\n";
            $rv = kInvalidArg;
         }
         else
         {
            print "The series $series has not yet been published, continuing...\n";
         }
      }

      if ($rv == kSuccess)
      {
         print "All initial checks were successful, continuing...\n";

         # Connecting to the slave db
         $dbname = $cfg{'SLAVEDBNAME'};
         $dbhost = $cfg{'SLAVEHOST'};
         $dbport = $cfg{'SLAVEPORT'};
         $dbuser = $cfg{'REPUSER'};

         $dsn = "dbi:Pg:dbname=$dbname;host=$dbhost;port=$dbport";

         $sdbh = DBI->connect($dsn, $dbuser, ''); # password should be provided by .pgpass
         print("Connecting to slave database with '$dsn' as user '$dbuser'...");

         if (defined($sdbh))
         {
            print("success!\n");

            # Checking for the existence of the schema on the slave host of table to replicate 
            print "Checking for the existene of schema '$schema' on the slave database.\n";
            $res = SchemaExists(\$sdbh, $schema);

            if ($res == -1)
            {
               print STDERR "Unable to check existence of $schema on slave database.\n";
               $rv = kInvalidSQL;
            }
            elsif ($res)
            {
               print "The namespace '$schema' already exists; skipping execution of createns.\n";
               $checktab = 1;
            } 
            else
            {
               # Running createns
               $checktab = 0;

               if (RunCmd("$cfg{kModDir}/createns ns=$schema nsgroup=user dbusr=$dbuser > $sqlpath", 1) != 0)
               {
                  print STDERR "Failure to create schema '$schema' on slave host.\n";
                  $rv = kRunCmd;
               }
               else
               {
                  print "Successfully ran createns.\n";
               }
            }
         }
         else
         {
            print("failure!\n");
            print STDERR "Failure connecting to database: '$DBI::errstr'\n";
            $rv = kDbConnection;
         }
      }

      if ($rv == kSuccess)
      {
         # Checking for existence of the table to replicate on the slave host
         print "Checking for the existence of table '$series' on the slave database.\n";
         $res = TableExists(\$sdbh, $schema, $table);

         if ($res == -1)
         {
            print STDERR "Unable to check existence of $series on slave database.\n";
            $rv = kInvalidSQL;
         }
         else
         {
            if ($checktab && $res == 1)
            {
               print "Table to replicate, $series, already exists on slave db host, ABORTING!\n";
               $rv = kInvalidArg;
            } 
            else
            {
               # Running createtabstruct
               if (RunCmd("$cfg{kModDir}/createtabstructure in=$series out=$series owner=$dbuser >> $sqlpath", 1) != 0)
               {
                  print STDERR "Failure to create series '$series' on slave host.\n";
                  $rv = kRunCmd;
               }
               else
               {
                  print "Successfully ran createtabstructure.\n";
               }
            }
         }
      }

      if ($rv == kSuccess)
      {
         # Execute the sql to create the series on the slave
         if (-e $sqlpath)
         {
            my($script);
            my(@scriptarr);
            my(@seriesInfo);

            # I guess I have to open this file and read the contents and pass them to the slave db
            if (open(SCRIPT, "<$sqlpath"))
            {
               @scriptarr = <SCRIPT>;
               close(SCRIPT);
               $script = join("", @scriptarr);

               print "Executing SQL ==> $script\n";

               $res = $sdbh->do($script);
               if (!NoErr($res, \$sdbh, $script))
               {
                  print "Failure creating series '$series' on slave db.\n";
                  $rv = kInvalidSQL;
               }
               else
               {
                   print "Successfully created series '$series' on slave db, continuing...\n";
                   
                   my($wl) = undef;
                   
                   $wl = $drmsParams->get('WL_HASWL');
                   
                   if (defined($wl) && $wl)
                   {
                       # Add series to drms.allseries. The machine hosting the db is in the DRMS parameter SERVER (port - DRMSPGPORT,
                       # dbname - DBNAME). The all-series series is assumed to be named 'drms.allseries'.
                       print "Getting series info for series $series...\n";
                       @seriesInfo = GetSeriesInfo($dbhost, $dbport, $dbhame, $series);
                       
                       if ($#seriesInfo >= 0)
                       {
                           my($info) = join(',', @seriesInfo);
                           print "Got info: $info\n";
                           
                           if (RunCmd("$cfg{kScriptDir}/updateAllSeries.py op=insert --info='$info'", 1) != 0)
                           {
                               print STDERR "Failure to insert series '$series' into drms.alleries.\n";
                               $rv = &kRunCmd;
                           }
                           else
                           {
                               print "Successfully ran updateAllSeries.py.\n";
                           }
                       }
                       else
                       {
                           print "Failed to get series info.\n";
                           $rv = &kSeriesNotFound;
                       }
                   }
               }
            }
            else
            {
               # Failure
               $rv = kFileIO;
            }
         }
      }

      if ($rv == kSuccess)
      {
         # Remove sql script file
         if (-e $sqlpath)
         {
            unlink($sqlpath);
         }
      }

      if ($rv == kSuccess)
      {
         # Obtain next subscription ID and next replication-set ID
         ($repsetid, $subid) = NextIDs(\$mdbh, $cfg{'CLUSTERNAME'});

         if ($subid == -1 || $repsetid == -1)
         {
            print STDERR "Unable to obtain next subscription ID and replication-set ID.\n";
            $rv = kInvalidSQL;
         }
      }

      if ($rv == kSuccess)
      {
         if (CreateRepSet($cfg{'CLUSTERNAME'}, $cfg{'MASTERDBNAME'}, $cfg{'MASTERHOST'}, $cfg{'MASTERPORT'},
                          $cfg{'REPUSER'}, $cfg{'SLAVEDBNAME'}, $cfg{'SLAVEHOST'}, $cfg{'SLAVEPORT'},
                          $repsetid + 1, $subid + 1, $series, $cfg{'kSlonikCmd'}))
         {
            print STDERR "Unable to create replication set.\n";
            $rv = kSlonikFailed;
         }
         else
         {
            print "Subscription of the new set was successful, continuing...\n";
         }
      }

      if ($rv == kSuccess)
      {
         print "Checking for propagation of published table to slave temporary replication set $repsetid\n";

         while (1)
         {
            $res = PropagatedToSlave(\$sdbh, $cfg{'CLUSTERNAME'}, $subid + 1);
            if ($res == -1)
            {
               print STDERR "";
               $rv = kInvalidSQL;
               last;
            }
            elsif ($res == 1)
            {
               # Table propagated to slave db.
               print "\nTable $series successfully propagated to replication set on slave database.\n";
               last;
            }
            else
            {
               print ".";
               sleep(1);
            }
         }
      }

      if (defined($sdbh))
      {
         $sdbh->disconnect();
      }

      if (defined($mdbh))
      {
         $mdbh->disconnect();
      }

      toolbox::ReleaseLock(\$lckfh);
   }

   exit(0);
}


sub PrintUsage
{
   print "You done screwed up!\n";
}

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

sub RunCmd
{
   my($cmd) = $_[0];
   my($doit) = $_[1];
   my($ret) = 0;

   if ($doit)
   {
      print "Running '$cmd'.\n";
      system($cmd);

      if ($? == -1)
      {
         print STDERR "Failed to execute '$cmd'.\n";
         $ret = -1;
      }
      elsif ($? & 127)
      {
         print STDERR "$cmd command failed to run properly.\n";
         $ret = -2;
      }
      elsif ($? >> 8 != 0)
      {
         my($err) = $? >> 8;
         print STDERR "$cmd command returned error code $err.\n";
         $ret = 1;
      }
   }
  
   return $ret;
}

sub TableExists
{
   my($rdbh) = $_[0];
   my($schema) = $_[1];
   my($table) = $_[2];
   my($stmnt);
   my($rv);
   
   $stmnt = "SELECT * FROM pg_tables WHERE tablename ILIKE '$table' AND schemaname ILIKE '$schema'";
   $rv = 0;
   
   print "Executing SQL ==> $stmnt\n";
   $rrows = $$rdbh->selectall_arrayref($stmnt, undef);

   if (NoErr($rrows, $rdbh, $stmnt))
   {
      my(@rows) = @$rrows;
      if ($#rows == 0)
      {
         # Table exists
         $rv = 1;
      }
   } 
   else
   {
      print STDERR "Error executing SQL ==> $stmnt\n";
      $rv = -1;
   }

   return $rv;
}

sub TableIsReplicated
{
   my($rdbh) = $_[0];
   my($schema) = $_[1];
   my($table) = $_[2];
   my($clustname) = $_[3];
   my($stmnt);
   my($rv);

   $stmnt = "SELECT * FROM _$clustname.sl_table WHERE tab_relname ILIKE '$table' AND tab_nspname ILIKE '$schema'";
   $rv = 0;

   print "Executing SQL ==> $stmnt\n";
   $rrows = $$rdbh->selectall_arrayref($stmnt, undef);

   if (NoErr($rrows, $rdbh, $stmnt))
   {
      my(@rows) = @$rrows;
      if ($#rows == 0)
      {
         # Table is under replication
         $rv = 1;
      }
   } 
   else
   {
      print STDERR "Error executing SQL ==> $stmnt\n";
      $rv = -1;
   }

   return $rv;
}

sub SchemaExists
{
   my($rdbh) = $_[0];
   my($schema) = $_[1];
   my($stmnt);
   my($rrows);
   my($rv);
   
   $stmnt = "SELECT * FROM pg_namespace WHERE nspname ILIKE '$schema'";
   $rv = 0;
   
   print "Executing SQL ==> $stmnt\n";
   $rrows = $$rdbh->selectall_arrayref($stmnt, undef);

   if (NoErr($rrows, $rdbh, $stmnt))
   {
      my(@rows) = @$rrows;
      if ($#rows == 0)
      {
         # Table exists
         $rv = 1;
      }
   } 
   else
   {
      print STDERR "Error executing SQL ==> $stmnt\n";
      $rv = -1;
   }

   return $rv;
}

# returns setid, then tabid
sub NextIDs
{
   my($rdbh) = $_[0];
   my($clustname) = $_[1];
   my($stmnt);
   my($rrows);
   my(@rv);

   $stmnt = "SELECT max(T1.set_id), max(T2.tab_id) FROM _$clustname.sl_set AS T1, _$clustname.sl_table AS T2";
   $rv[0] = -1;
   $rv[1] = -1;
   
   print "Executing SQL ==> $stmnt\n";
   $rrows = $$rdbh->selectall_arrayref($stmnt, undef);

   if (NoErr($rrows, $rdbh, $stmnt))
   {
      my(@rows) = @$rrows;
      if ($#rows == 0)
      {
         # Success
         $rv[0] = $rows[0]->[0];
         $rv[1] = $rows[0]->[1];
      }
   } 
   else
   {
      print STDERR "Error executing SQL ==> $stmnt\n";
   }

   return @rv;
}

sub CreateRepSet
{
   my($clustname) = $_[0];
   my($masterdbname) = $_[1];
   my($masterdbhost) = $_[2];
   my($masterdbport) = $_[3];
   my($repuser) = $_[4];
   my($slavedbname) = $_[5];
   my($slavedbhost) = $_[6];
   my($slavedbport) = $_[7];
   my($repsetid) = $_[8];
   my($subid) = $_[9];
   my($series) = $_[10];
   my($slonikapp) = $_[11];
   my($lcseries);
   my($slonikcmd);
   my($rv);

   $rv = 0;
   $lcseries = lc($series);

   $slonikcmd = 
   "cluster name = $clustname;\n" . 
   "node 1 admin conninfo = 'dbname=$masterdbname host=$masterdbhost port=$masterdbport user=$repuser';\n" .
   "node 2 admin conninfo = 'dbname=$slavedbname host=$slavedbhost port=$slavedbport user=$repuser';\n\n" .
   "create set (id=$repsetid, origin=1, comment='NEW TEMPORARY REPLICATION SET');\n" .
   "set add table (set id=$repsetid, origin=1, id=$subid, fully qualified name='$lcseries');\n" .
   "subscribe set (id=$repsetid, provider=1, receiver=2, forward=no);\n";

   print "Executing slonik ==>\n$slonikcmd\n";

   if (open(SLONIK, "| $slonikapp"))
   {
      print SLONIK $slonikcmd;
      close(SLONIK);
   } 
   else
   {
      print STDERR "Failure running slonik.\n";
      $rv = 1;
   }

   return $rv;
}

sub PropagatedToSlave
{
   my($rsdbh) = $_[0];
   my($clustname) = $_[1];
   my($subid) = $_[2];
   
   my($rv) = 0;
   my($stmnt) = "SELECT count(*) FROM _$clustname.sl_table WHERE tab_id = $subid";
   my($rrows);

   $rrows = $$rsdbh->selectall_arrayref($stmnt, undef);

   if (NoErr($rrows, $rsdbh, $stmnt))
   {
      my(@rows) = @$rrows;
      if ($#rows == 0)
      {
         if ($rows[0]->[0] > 0)
         {
            $rv = 1;
         }
      }
   }
   else
   {
      print STDERR "Error executing SQL ==> $stmnt\n";
      $rv = -1;
   }

   return $rv;
}

sub GetSeriesInfo
{
    my($series) = shift;
    my($dbhost) = shift;
    my($dbport) = shift;
    my($dbname) = shift;
    my($serieslc) = lc($series);
    my(@rv) = ();
    
    $stmnt = "SELECT seriesname, author, owner, unitsize, archive, retention, tapegroup, primary_idx, created, description, dbidx, version FROM drms_series() WHERE lower(seriesname) = '" . $serieslc . "'";
    
    print "Executing SQL ==> $stmnt\n";
    $rrows = $$rdbh->selectall_arrayref($stmnt, undef);
    
    if (NoErr($rrows, $rdbh, $stmnt))
    {
        my(@rows) = @$rrows;

        if ($#rows == 0)
        {
            # Series exists.
            # Each element is a reference to an array - gotta dereference and this sucks.
            @rv = ($dbhost, $dbport, $dbname, $rows[0]->[0], $rows[0]->[1], $rows[0]->[2], $rows[0]->[3], $rows[0]->[4], $rows[0]->[5], $rows[0]->[6], $rows[0]->[7] , $rows[0]->[8], $rows[0]->[9], $rows[0]->[10], $rows[0]->[11]);
        }
        else
        {
            # Series does not exist (or there is more than one entry in drms_series() for the series, which is bad).
            print STDERR "Series $series does not exist.\n";
        }
    }
    else
    {
        print STDERR "Error executing SQL ==> $stmnt\n";
    }

    return @rv;
}
