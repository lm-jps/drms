#!/home/jsoc/bin/linux_x86_64/activeperl -w

use FileHandle;
use File::Copy;
use File::Spec;
use IO::Dir;
use Fcntl ':flock';
use FindBin qw($Bin);
use lib "$Bin/..";
use toolbox qw(GetCfg);
# For modifying the subscription config and lst tables.
use lib "$Bin/../subscribe_manage";
use subtablemgr;
use DBI;
use DBD::Pg;
use JSON -support_by_pp;
use Email::MIME;
use Email::Sender::Simple qw(sendmail);
use Data::Dumper;

use constant kSuccess => 0;
use constant kInvalidArg => 1;
use constant kSlonikFailed => 2;
use constant kInvalidSQL => 3;
use constant kDelSeriesFailed => 4;
use constant kEditLstFailed => 5;

use constant kLockFile => "gentables.txt";

$| = 1; # autoflush

# This script takes 2 arguments: the path to the server-side configuration file, and a series name.
my($conf);
my($series);
my($schema);
my($table);
my($ns);
my($tab);
my($rv);
my(@cfgdata);
my(%cfg);

$rv = &kSuccess;

if ($#ARGV != 1)
{
   PrintUsage();
   $rv = &kInvalidArg;
}
else
{
   $conf = $ARGV[0];
   $series = $ARGV[1];

   if (-e $conf)
   {
      # fetch needed parameter values from configuration file
      if (toolbox::GetCfg($conf, \%cfg))
      {
         $rv = &kInvalidArg;
      }
   }
   else
   {
      $rv = &kInvalidArg;
   }

   ($schema, $table) = ($series =~ /(\S+)\.(\S+)/);
}

if ($rv == &kSuccess)
{
   # Connect to the db
   my($dbh);
   my($dbname);
   my($dbhost);
   my($dbport);
   my($dbuser);
   my($dsn);
   my($stmnt);
   my(@insts);
   
   $dbname = $cfg{'MASTERDBNAME'};
   $dbhost = $cfg{'MASTERHOST'};
   $dbport = $cfg{'MASTERPORT'};
   $dbuser = $cfg{'REPUSER'};

   # The host address from the configuration file is accessible only from hmidb and hmidb2.
   # So this script will not run anywhere else.
   $dsn = "dbi:Pg:dbname=$dbname;host=$dbhost;port=$dbport";

   $dbh = DBI->connect($dsn, $dbuser, ''); # password should be provided by .pgpass
   print("Connecting to database with '$dsn' as user '$dbuser' ... ");

   if (defined($dbh))
   {
       print("success!\n");

       my($tabid); # id of the series table that we are removing from replication

       $tabid = GetRepTableID(\$dbh, $cfg{'CLUSTERNAME'}, $schema, $table);

       if ($tabid == -2)
       {
          $rv = &kInvalidSQL;
       }
       elsif ($tabid == -1)
       {
          print "Series '$series' does not appear to be under slony replication.\n";
          $rv = &kInvalidArg;
       }
       else
       {
          print "Table ID for series '$series' is $tabid.\n";
       }

       if ($rv == &kSuccess)
       {
           # Get list of institutions subscribed to this series.
           my($sublistCmd);
           my($rsp);
           my($json);
           my($txt);
           
           $sublistCmd = $cfg{'kPubSubList'} . " cfg=$conf series=" . lc($series);
           print $sublistCmd . "\n";
           $rsp = `$sublistCmd`;
           $json = JSON->new->utf8;
           $txt = $json->decode($rsp);
           @insts = $txt->{nodelist}->{lc($series)};
           print Dumper(@insts);
       }
       
       # Make sure we don't truly unpublish anything for now.
       exit;
       
       if ($rv == &kSuccess)
       {
          # Need to run a slonk command. To do this, we pipe some text to the slonik binary
          my($slonikcmd);

          print "Dropping table from Slony replication set: cluster=$cfg{'CLUSTERNAME'}, masterdb=$cfg{'MASTERDBNAME'}, masterhost=$cfg{'MASTERHOST'}, masterport=$cfg{'MASTERPORT'}, repuser=$cfg{'REPUSER'}, slavedb=$cfg{'SLAVEDBNAME'}, slavehost=$cfg{'SLAVEHOST'}, slaveport=$cfg{'SLAVEPORT'}\n";

          $slonikcmd = 
          "cluster name = $cfg{'CLUSTERNAME'};\n" . 
          "node 1 admin conninfo = 'dbname=$cfg{'MASTERDBNAME'} host=$cfg{'MASTERHOST'} port=$cfg{'MASTERPORT'} user=$cfg{'REPUSER'}';\n" .
          "node 2 admin conninfo = 'dbname=$cfg{'SLAVEDBNAME'} host=$cfg{'SLAVEHOST'} port=$cfg{'SLAVEPORT'} user=$cfg{'REPUSER'}';\n\n" .
          "SET DROP TABLE (ORIGIN = 1, ID = $tabid);";

          print "Executing slonik ==>\n$slonikcmd\n";

          if (open(SLONIK, "| $cfg{'kSlonikCmd'}"))
          {
             print SLONIK $slonikcmd;
             close(SLONIK);
          }
          else
          {
             print STDERR "Failure running slonik.\n";
             $rv = &kSlonikFailed;
          }
       }

       if ($rv == &kSuccess)
       {
          # Check that replicated table was successfully dropped from replication.
          if (GetRepTableID(\$dbh, $cfg{'CLUSTERNAME'}, $schema, $table) != -1)
          {
             print STDERR "Table-dropping Slonik failed.\n";
             $rv = &kSlonikFailed;
          }
       }
       
       if ($rv == &kSuccess)
       {
           if ($#insts >= 0)
           {
               # Notify subscribers that their series was unpublished. This means that their subscription was dropped too.
               my($message);
               my($instList);
               
               $instList = join("\n", @insts);
               
               $message = Email::MIME->create(
               header => [ From => "production@sun.stanford.edu", To => $cfg{'kPubUnpubNotify'}, Subject => "$series unpublished", ],
               attributes => { encoding => 'quoted-printable', charset  => 'utf8', },
               body => "This email message was automatically generated by the subscription system.\n'$series' has been unpublished. As a result, all subscribers to this series have been unsubscribed from it.\nIf this series is re-published, the affected sites can re-subscribe to the series by first deleting the series, and then subscribing to it.\nThe sites can use the delete_series -k DRMS command to delete the series - the 'k' indicates that the existing SUs should NOT be deleted.\nBy retaining the SUs, the sites will not have to re-download them when they re-subscribe to the series.\n\nThe affected sites are:\n\n$instList\n",
               );
               
               # Send the message.
               sendmail($message);
           }
       }

       if ($rv == &kSuccess)
       {
          # Ensure that the unpublication has successfully propagated from the master
          # to the slave, then delete the data series from the slave.
          # Connect to the slave db, don't use the master!
          my($slavedbh);
          my($id);

          # Delete the DRMS part of the series being unpublished from the Slony slave.
          # DO NOT DELETE THE SUMS STORAGE UNITS!!
          $dsn = "dbi:Pg:dbname=$cfg{'SLAVEDBNAME'};host=$cfg{'SLAVEHOST'};port=$cfg{'SLAVEPORT'}";

          $slavedbh = DBI->connect($dsn, $cfg{'REPUSER'}, ''); # password should be provided by .pgpass
          print("Connecting to slave database with '$dsn' as user '$cfg{REPUSER}' ... ");
          
          if (defined($slavedbh))
          {
             print("success!\n");

             print("Checking for propagation of un-publication to slave db...");

             # Wait until slony replication has propagated to the slave.
             while (1)
             {
                $id = GetRepTableID(\$slavedbh, $cfg{'CLUSTERNAME'}, $schema, $table);
                if ($id == -1)
                {
                   print("\nUn-publication successfully propagated!\n");
                   last;
                }
                elsif ($id == -2)
                {
                   $rv = &kInvalidSQL;
                   last;
                }
                else
                {
                   # Table un-publication has not been propagated to slave yet.
                   print("\.");
                }

                sleep(1);
             }

             if ($rv == &kSuccess)
             {
                if (DeleteSeries(\$slavedbh, $schema, $table, 1) != 0)
                {
                   $rv = &kDelSeriesFailed;
                }
             }

             $slavedbh->disconnect();
          }
          else
          {
             print("failure!\n");
             print STDERR "Failure connecting to slave database: '$DBI::errstr'\n";
          }
       }

       if ($rv == &kSuccess)
       {
          # Remove the just-unpublished series entries from the remote sites' .lst files.
           if (EditLstFiles("$cfg{'kServerLockDir'}/subscribelock.txt", "$cfg{'kServerLockDir'}/parselock.txt", $cfg{kServerLockDir} . "/" . &kLockFile, "$cfg{'tables_dir'}", $cfg{kCfgTable}, $cfg{kLstTable}, $dbname, $dbhost, $dbport, $dbuser, $schema, $table, 1) != 0)
          {
             $rv = &kEditLstFailed;
          }
       }

       $dbh->disconnect();
   }
   else
   {
      print STDERR "Failure connecting to database: '$DBI::errstr'\n";
   }

   if ($rv == &kSuccess)
   {
      print "Unpublication completed successfully!\n";
   }
   else
   {
      print "Unpublication failed!\n";
   }
}

exit $rv;

# end

sub PrintUsage
{

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

sub GetRepTableID
{
   my($rdbh) = $_[0];
   my($clust) = $_[1];
   my($schema) = $_[2];
   my($table) = $_[3];
   my($stmnt);
   my($rrows);
   my($tabid);

   $tabid = -1;

   $stmnt = "SELECT tab_id FROM _$clust.sl_table WHERE tab_nspname='" . lc($schema) . "' AND tab_relname='" . lc($table) . "'";
   $rrows = $$rdbh->selectall_arrayref($stmnt, undef);
   
   if (NoErr($rrows, $rdbh, $stmnt))
   {
      my(@rows) = @$rrows;
      if ($#rows == 0)
      {
         # Table exists
         $tabid = $rows[0]->[0]; # first column in single row returned
      }
   }
   else
   {
      print STDERR "Error executing SQL ==> $stmnt\n";
      $tabid = -2;
   }

   return $tabid;
}

sub RunCmd
{
   my($cmd) = $_[0];
   my($doit) = $_[1];
   my($ret) = 0;

   if ($doit)
   {
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

sub DeleteSeries
{
   my($rdbh) = $_[0];
   my($schema) = $_[1];
   my($table) = $_[2];
   my($doit) = $_[3];
   my($rv);
   my($stmnt);
   my($res);
   
   $rv = 0;

   print "Deleting series =>\n";

   $stmnt = "DROP TABLE " . lc($schema) . "\." . lc($table);
   print "SQL => $stmnt\n";

   if ($doit)
   {
      $res = $$rdbh->do($stmnt);
      $rv = !NoErr($res, $rdbh, $stmnt);
   }

   if ($rv == 0)
   {
      $stmnt = "DROP SEQUENCE " . lc($schema) . "\." . lc($table) . "_seq";
      print "SQL => $stmnt\n";

      if ($doit)
      {
         $res = $$rdbh->do($stmnt);
         $rv = !NoErr($res, $rdbh, $stmnt);
      }
   }

   if ($rv == 0)
   {
      $stmnt = "DELETE from $schema\.drms_link WHERE lower(seriesname) = '" . lc($schema) . "\." . lc($table) . "'";
      print "SQL => $stmnt\n";

      if ($doit)
      {
         $res = $$rdbh->do($stmnt);
         $rv = !NoErr($res, $rdbh, $stmnt);
      }
   }

   if ($rv == 0)
   {
      $stmnt = "DELETE from $schema\.drms_keyword WHERE lower(seriesname) = '" . lc($schema) . "\." . lc($table) . "'";
      print "SQL => $stmnt\n";

      if ($doit)
      {
         $res = $$rdbh->do($stmnt);
         $rv = !NoErr($res, $rdbh, $stmnt);
      }
   }

   if ($rv == 0)
   {
      $stmnt = "DELETE from $schema\.drms_segment WHERE lower(seriesname) = '" . lc($schema) . "\." . lc($table) . "'";
      print "SQL => $stmnt\n";

      if ($doit)
      {
         $res = $$rdbh->do($stmnt);
         $rv = !NoErr($res, $rdbh, $stmnt);
      }
   }

   if ($rv == 0)
   {
      $stmnt = "DELETE from $schema\.drms_series WHERE lower(seriesname) = '" . lc($schema) . "\." . lc($table) . "'";
      print "SQL => $stmnt\n";

      if ($doit)
      {
         $res = $$rdbh->do($stmnt);
         $rv = !NoErr($res, $rdbh, $stmnt);
      }
   }

   return $rv;
}

my($subscribelockpath);

sub DropLock
{
   unlink "$subscribelockpath";
   exit(1);
}

sub EditLstFiles
{
    my($sublck) = $_[0];
    my($parselck) = $_[1];
    my($gentablck) = $_[2];
    my($tablesdir) = $_[3];
    my($cfgtab) = $_[4];
    my($lsttab) = $_[5];
    my($dbname) = $_[6];
    my($dbhost) = $_[7];
    my($dbport) = $_[8];
    my($dbuser) = $_[9];
    my($schema) = $_[10];
    my($table) = $_[11];
    my($doit) = $_[12];
    my($rv);
    my($series);
    my($line);
    my($origdir);
    
    $rv = 0;
    $subscribelockpath = $sublck;
    
    # EditLstFiles must obtain the subscription lock before proceeding since the subscription service
    # and this function will both try to modify the lst files.
    #
    # Since the subscription service is a bash script, it cannot do flock properly. Instead, 
    # we need to use the file system to lock a file.
    #
    # EditLstFiles must also acquire the parse lock since parse_slon_logs.pl will attempt to 
    # read the .lst files. Since the .lst file being modified is first renamed to .lst.bak, 
    # if the parser happened to attempt to read a .lst file right after the .lst rename, 
    # the results could be disastrous.
    system("(set -o noclobber; echo $$ > $subscribelockpath) 2> /dev/null");
    
    if ($? == 0)
    {
        $SIG{INT} = "DropLock";
        $SIG{TERM} = "DropLock";
        $SIG{HUP} = "DropLock";
        
        # Acquire parse lock
        my($lckfh);
        
        if (AcquireLock($parselck, \$lckfh))
        {
            $origdir = $ENV{'PWD'};
            chdir($tablesdir);
            tie(my(%allfiles), "IO::Dir", ".");
            my(@files) = keys(%allfiles);
            my(@lstfiles) = map({$_ =~ /(.+\.lst)$/ ? $1 : ()} @files);
            my($tblmgr);
            
            $series = lc($schema) . "\." . lc($table);
            
            foreach $item (@lstfiles)
            {
                print "moving $item to $item.bak\n";
                
                if ($doit)
                {
                    if (!move("$item", "$item.bak"))
                    {
                        print STDERR "Unable to move $item to $item.bak\n";
                        $rv = 1;
                        last;
                    }
                    
                    if (!open(LSTFILE, "<$item.bak"))
                    {
                        print STDERR "Unable to open $item.bak for reading.\n";
                        move("$item.bak", "$item");
                        $rv = 1;
                        last;
                    } 
                    
                    if (!open(OUTFILE, ">$item"))
                    {
                        print STDERR "Unable to open $item for writing.\n";
                        move("$item.bak", "$item");
                        $rv = 1;
                        last;
                    } 
                    
                    while (defined($line = <LSTFILE>))
                    {
                        chomp($line);
                        
                        if ($line !~ /$series/)
                        {
                            print OUTFILE "$line\n";
                        }
                    }
                    
                    if (!close(OUTFILE))
                    {
                        # Error writing file, revert.
                        move("$item.bak", "$item");
                        $rv = 1;
                    } 
                    else
                    {
                        unlink "$item.bak";
                    }
                    
                    close(LSTFILE);
                }
            } # loop over lst files
            
            untie(%allfiles);
            
            # Now update the subscription configuration and lst database tables. Eventually, 
            # we will rid ourselves of the text files and exclusively use the database tables to 
            # hold all the nodes' series lists.
            if ($rv == 0)
            {                
                if (!defined($tblmgr))
                {
                    $tblmgr = new SubTableMgr($gentablck, $cfgtab, $lsttab, $tablesdir, $dbname, $dbhost, $dbport, $dbuser);
                    $rv = ($tblmgr->GetErr() != &kSuccess);
                }
                
                if ($rv == 0)
                {
                    $tblmgr->RemoveSeries($series, undef);
                    $rv = ($tblmgr->GetErr() != &kSuccess);
                }
            }
            
            ReleaseLock(\$lckfh);
            
            unlink "$subscribelockpath";
            
            chdir($origdir);
            
            $SIG{INT} = 'DEFAULT';
            $SIG{TERM} = 'DEFAULT';
            $SIG{HUP} = 'DEFAULT';
        }
        else
        {
            $rv = 1;
        }
    }
    else
    {
        print STDERR "Warning:: couldn't obtain subscribe lock; bailing.\n";
        print STDERR "Run editlstfile.pl manually.\n";
        $rv = 1;
    }
    
    return $rv;
}

my(%fpaths);

sub AcquireLock
{
   my($path) =$_[0];
   my($lckfh) = $_[1];
   my($gotlock);
   my($natt);

   if (-e $path)
   {
      $$lckfh = FileHandle->new("<$path");
      $fpaths{fileno($$lckfh)} = $path;
   }
   else
   {
      $$lckfh = FileHandle->new(">$path");
   }
   $gotlock = 0;

   $natt = 0;
   while (1)
   {
      if (flock($$lckfh, LOCK_EX|LOCK_NB)) 
      {
         $gotlock = 1;
         last;
      }
      else
      {
         if ($natt < 10)
         {
            print "Lock '$path' in use - trying again in 1 second.\n";
            sleep 1;
         }
         else
         {
            print "Couldn't acquire lock after $natt attempts; bailing.\n";
         }
      }

      $natt++;
   }

   return $gotlock;
}

sub ReleaseLock
{
   my($lckfh) = $_[0];
   my($lckfn);
   my($lckfpath);

   $lckfn = fileno($$lckfh);
   $lckfpath = $fpaths{$lckfn};

   flock($$lckfh, LOCK_UN);
   $$lckfh->close;

   if (defined($lckfpath))
   {
      chmod(0664, $lckfpath);
      delete($fpaths{$lckfn});
   }
}
