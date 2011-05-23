#!/home/jsoc/bin/linux_x86_64/perl -w

# This script will check to see if slony log-generation is actively occurring. Every time
# that slony generates a log file, it updates a file (identified by the configuration
# argument kPSLlogReady). Slony should produce a log file at most 5 minutes after
# the previous log file was generated. (With our current data load, the interval is
# typically 1 minute between log-files.).  So if timestamp of the log-ready file
# indicates that the log-ready file is more than 5 minutes old, slony log-production
# has stopped.

use strict;

use File::stat;
use FindBin qw($Bin);
use FileHandle;
use lib "$Bin/../../../libs/logmonitor";
use Logmontools qw(loggerMsgPrt);

use constant kMaxInteval => 5;
use constant kModName => "slonyrep";
use constant kSubModName => "chklogprod";
use constant kBaseLogName => "chkslogs";

my($cfg);
my($logfname);
my($line);
my($cntrfile);
my($kMSLogDir);
my($mdate);
my($timenow);
my($msg);
my($logfh);

if ($#ARGV != 0)
{
   print STDERR "Incorrect syntax.\n";
   Usage();
   exit(1);
}

$cfg = $ARGV[0];
$logfname = $ARGV[1];

# Extract the path to the counter file from the configuration file.
open(CNFFILE, "<$cfg") || die "Unable to read configuration file '$cfg'.\n";

while (defined($line = <CNFFILE>))
{
   chomp($line);
   
   if ($line =~ /^\#/ || length($line) == 0)
   {
      next;
   }

   # Collect arguments of interest
   if ($line =~ /^\s*kPSLlogReady=(.+)/)
   {
      $cntrfile = $1;
   }
   elsif ($line =~ /^\s*kMSLogDir=(.+)/)
   {
      $kMSLogDir = $1;
   }
}

close(CNFFILE);

LogPrintLM(\$logfh, "START_PROCESS", "Starting");

# check $cntrfile timestamp
LogPrint(\$logfh, "Looking for slony last-log file '$cntrfile'.");

if (-e $cntrfile)
{
   my($sb) = stat("$cntrfile");

   if (defined($sb))
   {
      LogPrint(\$logfh, "Found '$cntrfile'");
      $mdate = $sb->mtime;         # seconds since epoch
      $timenow = time();

      if ($timenow - $mdate >= 360)
      {
         LogPrintLM(\$logfh, "ERROR", "More than five minutes have elapsed since the last slon log was produced; slony-log production is down.");
      }
      else
      {
         LogPrint(\$logfh, "Slony-log generation appears to be functioning fine.");
      }
   }
}
else
{
   LogPrintLM(\$logfh, "ERROR", "Unable to access last-log file - something is wrong.");
}

LogPrintLM(\$logfh, "END_PROCESS", "Ending");

exit(0);

# Al Final

sub Usage
{
   print STDOUT "checkslonlogs.pl <replication server configuration file>\n";
}

sub InitLog
{
   my($rlf) = @_;
   my($logfname);
   my($rv);

   $rv = 1;

   if (!defined($$rlf))
   {
      $logfname = $kMSLogDir . "/" . kBaseLogName . "\.log";
      $$rlf = FileHandle->new(">>$logfname");

      if (!defined($$rlf))
      {
         print STDERR "Unable to initialize log file '$logfname'\n";
         $rv = 0;
      }
   }

   return $rv;
}

sub LogPrintLM
{
   my($rlf, $status, $msg) = @_;

   InitLog($rlf) || exit(1);

   my($logentry) = loggerMsgPrt($status, $msg, kModName, kSubModName, $0);

   print {$$rlf} "$logentry.\n";
}

sub LogPrint
{
   my($rlf, $msg) = @_;

   InitLog($rlf) || exit(1);

   print {$$rlf} "$msg\n";
}

