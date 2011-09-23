#!/usr/bin/perl -w

use FindBin qw($Bin);
use lib "$Bin/..";
use toolbox qw(GetCfg);
use FileHandle;
use DBI;
use DBD::Pg;
use Data::Dumper;

use constant kSuccess => 0;
use constant kInvalidArg => 1;
use constant kInvalidSQL => 2;
use constant kFileIO => 3;
use constant kDbConn => 4;
use constant kUnexpectedResp => 5;

use constant kOpPopJMD => "POPULATEJMD";
use constant kOpTrigger => "INSTALLTRIGGER";
use constant kPopJMD => "popjmd";
use constant kJMDTrigger => "jmdtrigger";

my($arg);
my($op);
my($conf);
my($sublist);
my($agentinfo);
my($logfile);
my($logfh);
my($outstm);
my($errstm);

my(%cfg);
my($rv) = kSuccess;


while (defined($arg = shift(@ARGV)))
{
   if ($arg eq "-o")
   {
      $op = shift(@ARGV);
   }
   elsif ($arg eq "-c")
   {
      $conf = shift(@ARGV);
   }
   elsif ($arg eq "-s")
   {
      $sublist = shift(@ARGV);
   }
   elsif ($arg eq "-a")
   {
      $agentinfo = shift(@ARGV);
   }
   elsif ($arg eq "-l")
   {
      $logfile = shift(@ARGV);
   }
   else
   {
      $rv = kInvalidArg;
   }
}

if ($rv == kSuccess)
{
   # open log file (or stdout if no log file provided)
   if (defined($logfile))
   {
      $logfh = FileHandle->new(">>$logfile");
      unless (defined($logfh))
      {
         print STDERR "Unable to open $logfile for appending to.\n";
         $rv = kFileIO;
      }
      
      $outstm = $logfh;
      $errstm = $logfh;
   }
   else
   {
      $outstm = \*STDOUT;
      $errstm = \*STDERR;
   }
}

if ($rv == kSuccess)
{
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
}

if ($rv == kSuccess)
{
   # switch on op
   if (defined($op))
   {

      my(@needstriggers) = WhoNeedsTriggers($sublist, $outstm, $errstm);

      if ($op eq kOpPopJMD)
      {
         my($dsn);
         my($dbh);
         my($stmnt);
         my($series);
         my($ns);
         my($pkeylist);
         my($rrow); # reference to array
         my(@row);

         if ($#needstriggers >= 0)
         {
            $dsn = "dbi:Pg:dbname=$cfg{pg_dbname};host=$cfg{pg_host};port=$cfg{pg_port}";
            $dbh = DBI->connect($dsn, $cfg{pg_user}, ''); # will need to put pass in .pg_pass

            if (defined($dbh))
            {
               foreach $seriesname (@needstriggers)
               {
                  $series = lc($seriesname);
                  $ns = ($series =~ /^(\S+)\.\S+/)[0];

                  # Need to collect prime key names for each series
                  $stmnt = "SELECT dbidx FROM $ns.drms_series WHERE lower(seriesname) = '$series'";

                  LogPrint($outstm, "executing db statement ==> $stmnt\n", 0);

                  $rrow = $dbh->selectall_arrayref($stmnt, undef);
                  if (NoErr($rrow, \$dbh, $stmnt, $outstm, $errstm))
                  {
                     # $rrow is a reference to an array of references to an array.
                     @row = @$rrow;

                     if ($#row == 0)
                     {
                        $pkeylist = $row[0]->[0];
                     }
                     else
                     {
                        my($nrows) = $#row + 1;
                        LogPrint($errstm, "Unexcepted response from db server - $nrows rows returned \n", 1);
                        $rv = kUnexpectedResp;
                     }
                  }

                  $stmnt = "INSERT INTO sunum_queue (recnum,sunum,series_name) SELECT recnum, sunum, '$series' FROM $series WHERE recnum IN (SELECT max(recnum) FROM $series GROUP BY $pkeylist)";
                  ExecStatement(\$dbh, $stmnt, 0, "Unable to insert $seriesname data rows into sunum_queue.\n", $outstm, $errstm);
               }

               $dbh->disconnect();
            }
            else
            {
               LogPrint($errstm, "Unable to connect to the db.\n", 1);
               $rv = kDbConn;
            }
         }
      }
      elsif ($op eq kOpTrigger)
      {
         if ($#needstriggers >= 0)
         {
            $dsn = "dbi:Pg:dbname=$cfg{pg_dbname};host=$cfg{pg_host};port=$cfg{pg_port}";
            $dbh = DBI->connect($dsn, $cfg{pg_user}, ''); # will need to put pass in .pg_pass
            
            if (defined($dbh))
            {
               my($trgname);
               my($trgfxn);

               foreach $seriesname (@needstriggers)
               {
                  $series = lc($seriesname);

                  $trgname = "${series}_trg";
                  $trgfxn = "${series}_fc";

                  $trgname =~ s/\./_/;
                  $trgfxn =~ s/\./_/;
               
                  $stmnt = TriggerStatement($trgname, $series, $trgfxn);
                  ExecStatement(\$dbh, $stmnt, 1, "Unable to create trigger $trgname for series $seriesname.\n", $outstm, $errstm);
               }
               
               $dbh->disconnect();
            }
            else
            {
               LogPrint($errstm, "Unable to connect to the db.\n", 1);
               $rv = kDbConn;
            }
         }
      }
      else
      {
         LogPrint($errstm, "Invalid operation '$op'.\n", 1);
         $rv = kInvalidArg;
      }
   }
   else
   {
      LogPrint($errstm, "Missing required operation argument.\n", 1);;
      $rv = kInvalidArg;
   }
}

if (defined($logfh))
{
   $logfh->close();
}

exit $rv;

sub LogPrint
{
   my($stm) = $_[0];
   my($msg) = $_[1];
   my($errecho) = $_[2];

   print $stm $msg;

   if (defined($errecho) && $errecho && ($stm ne \*STDERR))
   {
      print STDERR $msg;
   }
}

sub WhoNeedsTriggers
{
   my($sublist) = $_[0];
   my($outstm) = $_[1];
   my($errstm) = $_[2];

   my(@needstriggers) = ();

   if (defined($sublist))
   {
      if (-e $sublist)
      {
         # Parse this list, looking for the JMDTrigger argument
         if (open(SUBLIST, "<$sublist"))
         {
            my(@list) = <SUBLIST>;
            my($tflag) = kJMDTrigger;
            my($sline);
            my($series);
            my($dsn);
            my($dbh);
            my($stmnt);

            close(SUBLIST);

            foreach $sline (@list)
            {
               chomp($sline);
               if ($sline =~ /^\s*(\S+)\s+subscribe\s+(\S+\s+)*$tflag/)
               {
                  # Install a generic trigger - at this point, the replicated series will have
                  # been created and populated. 
                  $series = $1;
                  push(@needstriggers, $series);
               }
            }
         }
         else
         {
            LogPrint($errstm, "Unable to open for reading $sublist.\n", 1);
            $rv = kFileIO;
         }
      }
      else
      {
         LogPrint($errstm, "Unable to open for reading $sublist.\n", 1);
         $rv = kFileIO;
      }
   }
   else
   {
      LogPrint($errstm, "Missing required subscription-list file argument.\n", 1);
      $rv = kInvalidArg;
   }

   return @needstriggers;
}

sub TriggerStatement
{
   my($trgname) = $_[0];

   my($series) = $_[1];
   my($trgfxn) = $_[2];

   my(@stmntarr) = <DATA>;
   my($stmnt) = join("",@stmntarr);

   $stmnt =~ s/\<TRIGNAME\>/$trgname/g;
   $stmnt =~ s/\<SERIES\>/$series/g;
   $stmnt =~ s/\<TRIGFXN\>/$trgfxn/g;

   return $stmnt;
}

sub NoErr
{
   my($rv) = $_[0];
   my($dbh) = $_[1];
   my($stmnt) = $_[2];
   my($outstm) = $_[3];
   my($errstm) = $_[4];

   my($ok) = 1;

   if (!defined($rv) || !$rv)
   {
      if (defined($$dbh) && defined($$dbh->err))
      {
         LogPrint($errstm, "Error " . $$dbh->errstr . ": Statement '$stmnt' failed.\n", 1);
      }

      $ok = 0;
   } 

   return $ok;
}

sub ExecStatement
{
   my($dbh, $stmnt, $doit, $msg, $outstm, $errstm) = @_;
   my($res);

   LogPrint($outstm, "executing db statment ==> $stmnt\n", 0);

   if ($doit)
   {
      $res = $$dbh->do($stmnt);
      NoErr($res, $dbh, $stmnt, $outstm, $errstm) || die $msg;
   }
}

__DATA__
DROP TRIGGER IF EXISTS <TRIGNAME> ON <SERIES>;

DROP FUNCTION IF EXISTS <TRIGFXN>() CASCADE;

-- CREATE LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION <TRIGFXN>() returns TRIGGER as $<TRIGNAME>$
BEGIN
IF (TG_OP='INSERT' AND new.sunum > 0) THEN
        INSERT INTO sunum_queue (sunum,recnum,series_name) VALUES (new.sunum,new.recnum,'<SERIES>');
END IF;
RETURN NEW;
END
$<TRIGNAME>$ LANGUAGE plpgsql;

CREATE TRIGGER <TRIGNAME> AFTER INSERT ON <SERIES> 
    FOR EACH ROW EXECUTE PROCEDURE <TRIGFXN>();
