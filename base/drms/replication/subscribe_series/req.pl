#!/usr/bin/perl -w

# Issues general, non-subscribe_series requests to the server.
# Assumes that the ssh-key stuff has already been set up.

# req.pl <request file> <log file> <replication server user> <replication server host> <replication server trigger directory> <node> <working directory>

# use IO::Tee; - not a standard library, so don't use it
use FileHandle;
use IO::File;

use constant kEOF => "__EOF__";
use constant kMAXTRIESRESP => 30;

my($logfh);
my($line);
my($series);
my($req);
my($args);
my($trigger);
my($cmd);
my($resp);
my($natt);
my($err);
my($logfile);
my($rsuser);
my($rsserver);
my($rstriggerdir);
my($node);
my($workdir);
my(@files);
my($eof);
my($maxtriesresp);

$err = 0;

my(@reqcontents);

if ($#ARGV != 6)
{
   LogError("Incorrect usage.\n");
   exit(1);
}

$req = $ARGV[0];
$logfile = $ARGV[1];
$rsuser = $ARGV[2];
$rsserver = $ARGV[3];
$rstriggerdir = $ARGV[4];
$node = $ARGV[5];
$workdir = $ARGV[6];

$trigger = "$workdir/trigger.tmp";

# Don't run this script if the client-generated trigger, or the server-generated
# response are present on the server - this indicates that a previous 
# request is still pending.
my(@flagfiles);

push(@flagfiles, "$node\.req");
push(@flagfiles, "$node\.resp");

LogWrite("Checking for existing request.\n");

if (FindRemoteFiles($rsuser, $rsserver, $rstriggerdir, @flagfiles) > 0)
{
   # Already processing a request - bail
   LogError("A request is already being processed...unable to continue.\n");
   FlushLog();
   exit(1);
}

LogWrite("Opening request file $req.\n");

if (!open(REQF, "<$req"))
{
   LogError("Unable to open repro-request file $req\.n");
   FlushLog();
   exit(1);
}

LogWrite("Validating request file.\n");

while (defined($line = <REQF>))
{
   chomp $line;

   if ($line !~ /^#/ && length($line) > 0)
   {
      # Check for valid request before pinging server
      if ($line =~ /(\S+)\s+(\S+)(\s+\S+)*/)
      {
         $series = $1;
         $req = $2;

         if (defined($3))
         {
            $args = $3;
         }
         else
         {
            $args = "";
         }
      }

      # print "series:$series req:$req args:$args\n";

      # filter out acceptable requests, print an error message for invalid ones
      if ($req =~ /repro/i || $req =~ /list/i)
      {
         my($reqline) = "$series  $req";
         if (defined($args))
         {
            $reqline = $reqline . "  $args";
         }

         LogWrite("  Request $reqline looks good.\n");
         push(@reqcontents, $reqline);
      }
      else
      {
         LogEcho("  Invalid request '$req', continuing with next line.\n");
      }
   }
}

close(REQF);

# Write trigger file
LogWrite("Writing trigger file $trigger.\n");
if (!open(REQFILE, ">$trigger"))
{
   LogError("Unable to open local copy of trigger file $trigger for writing.\n");
   FlushLog();
   exit(1);
}

my($rline);
foreach $rline (@reqcontents)
{
   print REQFILE "$rline\n";
}

$rline = kEOF;
print REQFILE $rline;

close(REQFILE);

# Send trigger file to server - if a trigger file already exists (with __EOF__), then fail.
# This shouldn't happen as this script should be run only by subscribe_series, and only
# one subscribe_series should run at a time.
# Otherwise, send the trigger file, then wait for the server to process and send node.response.
# Must have a time out when waiting for the trigger file - if the time out happens, then delete
# the trigger file and return an error code.
$cmd = "scp $trigger $rsuser\@$rsserver:$rstriggerdir/$node\.req > /dev/null 2>&1";
LogWrite("Sending trigger file to replication server: $cmd.\n");

if (RunCmd($cmd) != 0)
{
   LogError("Unable to copy trigger file to server.\n");
   $err = 1;
}
else
{
   push(@files, "$node\.resp");
   $eof = kEOF;
}

# Use the response
$natt = 0;
$maxtriesresp = kMAXTRIESRESP;
while ($natt < $maxtriesresp && $err != 1)
{
   LogWrite("Waiting for response file: attempt $natt\n");

   if (FindRemoteFiles($rsuser, $rsserver, $rstriggerdir, @files) > 0)
   {
      # response file exists - check for EOF, then use it.
      $resp = `ssh $rsuser\@$rsserver 'cat $rstriggerdir/$node\.resp | grep $eof'`;

      if (length($resp) > 0)
      {
         $cmd = "scp $rsuser\@$rsserver:$rstriggerdir/$node\.resp $workdir > /dev/null 2>&1";

         if (RunCmd($cmd) == 0)
         {
            if (!open(RESP, "<$workdir/$node\.resp"))
            {
               LogError("Unable to open response file $workdir/$node\.resp.\n");
               $err = 1;
            }
            else
            {
               LogWrite("Got response - copied to $workdir/$node\.resp\n");
               # For now, just write the response file to stdout - the caller must decide what to do.
               # format:
               #   <request line 1>
               #   success: <0 or 1>
               #   message: <server message>
               #   __EOR__
               #   <request line 2>
               #   success: <0 or 1>
               #   message: <server message>
               #   __EOR__
               #   ...
               #   __EOF__
               while (defined($line = <RESP>))
               {
                  print STDOUT $line;
               }
               
               close(RESP);
               
               # Things worked out - remove response file
               unlink("$workdir/$node\.resp");

               last;
            }
         }
         else
         {
            LogError("Unable to transfer the response file from the server.\n");
            $err = 1;
         }
      }
   }

   sleep 2;
   $natt++;
}

if ($natt == $maxtriesresp)
{
   LogError("Unable to locate response file after $natt tries; bailing out.\n");
   $err = 1;
}

# Always delete the trigger on the server, but delete the local triggers only if
# no errors occurred.  Deletion of the trigger file on the server will
# cause the server to clean up the response file, and allow future requests
# to succeed.
DeleteTriggers($rsuser, $rsserver, $rstriggerdir, $node, $trigger, !$err);
FlushLog();

exit($err);

sub RunCmd
{
   my($cmd) = $_[0];
   my($ret) = 0;

   system($cmd);

   if ($? == -1)
   {
      LogError("Failed to execute '$cmd'.\n");
      $ret = -1;
   }
   elsif ($? & 127)
   {
      LogError("$cmd command failed to run properly.\n");
      $ret = -2;
   }
   elsif ($? >> 8 != 0)
   {
      my($err) = $? >> 8;
      LogError("$cmd command returned error code $err.\n");
      $ret = 1;
   }
  
   return $ret;
}

sub DeleteTriggers
{
   my($rsuser) = $_[0];
   my($rsserver) = $_[1];
   my($rstriggerdir) = $_[2];
   my($node) = $_[3];
   my($localtrigger) = $_[4];
   my($dolocal) = $_[5];

   my($ret);
   my($cmd);
   my(@files);

   $ret = 0;

   # remote copy
   push(@files, "$node\.req");

   if (FindRemoteFiles($rsuser, $rsserver, $rstriggerdir, @files) > 0)
   {
      # delete trigger file from server
      $cmd = "ssh $rsuser\@$rsserver 'rm -f $rstriggerdir/$node\.req'";
      if (RunCmd($cmd) != 0)
      {
         LogError("Failed to properly clean-up trigger file on server.\n");
         $ret = 1;
      }
   }

   # local copy
   if ($dolocal && -f $localtrigger)
   {
      if (unlink($localtrigger) != 1)
      {
         LogError("Failed to properly clean-up local copy of trigger file.\n");
         $ret = 1;
      }
   }

   return $ret;
}

# Returns the number of files found
sub FindRemoteFiles
{
   my($rsuser) = shift(@_);
   my($rsserver) = shift(@_);
   my($rstriggerdir) = shift(@_);
   my(@files) = @_;

   my($cmd);
   my($file);
   my(@resp);
   my($ret);

   if ($#files >= 0)
   {
      $cmd = "ssh $rsuser\@$rsserver 'find $rstriggerdir -maxdepth 1 -name $files[0]";
      shift(@files);

      foreach $file (@files)
      {
         $cmd = "$cmd -o -name $file";
      }

      $cmd = "$cmd'";
   }

   LogWrite("Looking for remote files - running [$cmd]\n");
   @resp = `$cmd`;
   $ret = $#resp + 1;
}

sub LogEcho
{
   my($str) = $_[0];

   if (!defined($logfh))
   {
      $logfh = FileHandle->new(">$logfile");
   }

   print $logfh $str;
   print STDOUT $str;
}

sub LogError
{
   my($str) = $_[0];

   if (!defined($logfh))
   {
      $logfh = FileHandle->new(">$logfile");
   }

   print $logfh $str;
   print STDERR $str;
}

sub LogWrite
{
   my($str) = $_[0];

   if (!defined($logfh))
   {
      $logfh = FileHandle->new(">$logfile");
   }

   print $logfh $str;
}

sub FlushLog
{
   if (defined($logfh))
   {
      $logfh->close;
   }
}
