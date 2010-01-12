#!/usr/bin/perl -w

# Usage:
#    manage_logs.pl -s <conf file> -c <counter file name> [-t <tar cmd> -g <gzip cmd>] [-l <log file>]
#    manage_logs.pl -s subscription_manager.cfg -c slony_counter.txt -t /bin/tar -z /bin/gzip -l tmp.txt

# Runs on the slony server (at Stanford, this is webdb)

use IO::Dir;
use FileHandle;
use File::Copy;

use constant kTarChunk => 64;

my($arg);
my($siteDir);   # dir on server containing site-specific slony log files
my($cnfPath);   # path to the subscription_manager configuration file
my($cntrFile);  # name of the counter file
my($dotar);     # if set, the tar up files
my($tarbin);    # path to tar
my($zipbin);    # path to zip
my($logfile);   # file to log output to
my($line);

# Fetch cmd-line arguments.
while ($arg = shift(@ARGV))
{
   if (($pos = index($arg, "-s", 0)) == 0)
   {
      $cnfPath = shift(@ARGV);
   }
   elsif (($pos = index($arg, "-c", 0)) == 0)
   {
      $cntrFile = shift(@ARGV);
   }
   elsif (($pos = index($arg, "-t", 0)) == 0)
   {
      $dotar = 1;
      $tarbin = shift(@ARGV);
   }
   elsif (($pos = index($arg, "-z", 0)) == 0)
   {
      $zipbin = shift(@ARGV);
   }
   elsif (($pos = index($arg, "-l", 0)) == 0)
   {
      $logfile = shift(@ARGV);
   }
}

if (!defined($cnfPath) || !defined($cntrFile) || (defined($tarbin) && !defined($zipbin)))
{
   die "Invalid arguments.\n";
}

# Ingest the global configuration file.
if (!(-f $cnfPath))
{
   die "Unable to locate configuration file '$cnfPath'.\n";
}

open(CNFFILE, "<$cnfPath") || die "Unable to read configuration file '$cnfPath'.\n";

while (defined($line = <CNFFILE>))
{
   chomp($line);
   
   if ($line =~ /^\#/ || length($line) == 0)
   {
      next;
   }

   # Collect arguments of interest
   if ($line =~ /^\s*subscribers_dir=(.+)/)
   {
      $siteDir = $1;
   }
}

close(CNFFILE);

# Open logfile
if (defined($logfile))
{
   # Opens the file too
   $logfh = FileHandle->new(">$logfile");
}

# Iterate through site-specific directories
chdir($siteDir);
tie(my(%sites), "IO::Dir", ".");

my(@subdirs);
my($asite);
my($cntrPath);
my($cntrVal);
my(@lfiles);
my(@sqlfiles);
my(@tarfiles);
my($lasttarred);

@subdirs = keys(%sites);

while (defined($asite = shift(@subdirs)))
{
   if ($asite =~ /^\.$/ || $asite =~ /^\.\.$/)
   {
      # Skip the "." and ".." files
      next;
   }

   # Get counter file
   $cntrVal = undef;
   $cntrPath = "${asite}/${cntrFile}";

   if (-f $cntrPath)
   {
      # Get counter value
      open(CTRFILE, "<$cntrPath") || die "Unable to read $cntrPath.\n";
      if (defined($line = <CTRFILE>))
      {
         chomp($line);
         $cntrVal = $line;
      }

      close(CTRFILE);
   }

   tie(my(%logfiles), "IO::Dir", $asite);
   @lfiles = keys(%logfiles);

   # Get sorted (by counter number) lists of parsed slony logs and tar files
   @sqlfiles = sort({($a =~ /^slony\S*_log\S*_(\d+)\.sql/)[0] <=> ($b =~ /^slony\S*_log\S*_(\d+)\.sql/)[0]} map({$_ =~ /^(slony\S*_log\S*_\d+\.sql)/ ? $1 : ()} @lfiles));
   @tarfiles = sort({($a =~/(\d+)-\d+/)[0] <=> ($b =~ /(\d+)-\d+/)[0]} map({$_ =~ /^(slony\S*_logs\S*_\d+-\d+\.tar.*)/ ? $1 : ()} @lfiles));

   # Untie before trying to delete files
   untie(%logfiles);

   if ($#tarfiles >= 0)
   {
      my($lasttarfile) = $tarfiles[$#tarfiles];
      ($lasttarred) = ($lasttarfile =~ /^slony\S*_logs\S*_\d+-(\d+)\.tar.*/);
   }
   else
   {
      $lasttarred = undef;
   }
   
   # If a valid counter file exists, remove all sql files and tar files associated with counter files already ingested.
   # Just do a linear search for now - could optimize and do a bsearch since the files are sorted, but probably not worth it
   my($ccounter);
   my($fullpath);
   my($item);
   my($isfile);
   my($sfilestart);
   my($sfileend);
   my(@totar);

   #sql files
   $isfile = 0;
   foreach $item (@sqlfiles)
   {
      if ($item =~ /^slony\S*_log\S*_(\d+)\.sql/)
      {
         $ccounter = $1;

         if ((defined($lasttarred) && $ccounter <= $lasttarred) || 
             (defined($cntrVal) && $ccounter <= $cntrVal))
         {
            # Already tarred or ingested, okay to remove
            $fullpath = "${asite}/${item}";
            LogWrite($logfh, "Deleting sql file previously ingested: $fullpath.\n", 0);
            unlink($fullpath);
         }
         elsif ($dotar)
         {
            # Need to tar these up
            push(@totar, "${item}");

            # Keep track of the first and last counter value in the tar file
            if ($isfile == 0)
            {
               $sfilestart = $ccounter;
            }

            $sfileend = $ccounter;
            $isfile++;
         }
      }
   }

   # tar files
   if (defined($cntrVal))
   {
      foreach $item (@tarfiles)
      {
         if ($item =~ /^slony\S*_logs\S*_\d+-(\d+)\.tar.*/)
         {
            $ccounter = $1;

            if ($ccounter <= $cntrVal)
            {
               # Already ingested, okay to remove
               $fullpath = "${asite}/${item}";
               LogWrite($logfh, "Deleting tar file previously ingested: $fullpath.\n", 0);
               unlink($fullpath);
            }
         }
      }
   }

   # Tar up remaining sql files
   if ($dotar && $#totar >= 0)
   {
      # First tar to a temporary file, then validate the tar. If the tar file is good, then 
      # rename the tar file with a permanent name and delete the source sql files.
      my($tarmcd);
      my($res);
      my($sfilelist) = "";

      # Change directories to site dir.
      chdir($asite);

      # Grrr - tar won't create an empty archive
      $fullpath = $totar[0];

      # Clean up anything that might have gotten left behind during previous incomplete runs
      if (-e ".tmp.tar")
      {
         unlink(".tmp.tar");
      }

      if (-e ".tmp.tar.gz")
      {
         unlink(".tmp.tar.gz");
      }

      RunTar($tarbin, "cf", ".tmp.tar", "", $fullpath);

      # Chunk tar cmds so that we don't overrun the cmd-line with too many chars.
      $isfile = 1;

      foreach $fullpath (@totar[1..$#totar])
      {
         $sfilelist = "$sfilelist $fullpath";
         if ($isfile % kTarChunk == 0)
         {
            # Execute tar cmd.
            RunTar($tarbin, "rf", ".tmp.tar", "", $sfilelist);
            $sfilelist = "";
         }

         $isfile++;
      }

      if (length($sfilelist) > 0)
      {
         RunTar($tarbin, "rf", ".tmp.tar", "", $sfilelist);
      }

      # Validate tar file
      $res = Validatetar(".tmp.tar", $sfilestart, $sfileend);

      if ($res == 0)
      {
         my($gzcmd);
         my($tarfilename) = sprintf("slony_logs_%d-%d.tar.gz", $sfilestart, $sfileend);

         $gzcmd = "$zipbin --best .tmp.tar";

         LogWrite($logfh, "Gzipping '.tmp.tar' to '.tmp.tar.gz'\n", 0);

         $res = system($gzcmd);
         ($res == 0) || die("gzip cmd '$gzcmd' failed to run properly.\n"); 
         
         LogWrite($logfh, "Moving '.tmp.tar.gz' to '$tarfilename'.\n", 0);
         if (!move(".tmp.tar.gz", $tarfilename))
         {
            LogWrite($logfh, "Unable to move .tmp.tar.gz to $tarfilename.\n", 1);
         }
         
         # Remove sql files just tarred.
         while (defined($fullpath = shift(@totar)))
         {
             LogWrite($logfh, "Deleting tarred sql file: $fullpath.\n");
             unlink($fullpath);
         }
      }

      chdir("..");
   }
} # end site loop

untie(%sites);

if (defined($logfh))
{
   $logfh->close;
}

# Done
exit(0);

# subroutines
sub LogWrite
{
   my($logfh, $str, $tostderr) = @_;

   if (defined($logfh))
   {
      print $logfh $str;
   }

   if ($tostderr)
   {
      print STDERR $str;
   }
}

sub RunTar
{
   my($tarbin, $op, $file, $list, $options) = @_;
   my($tarcmd);
   my($res);

   $tarcmd = "$tarbin $op $file $list $options";
   LogWrite($logfh, "Running tar '$tarcmd'\n");

   $res = system($tarcmd);
   ($res == 0) || die("tar cmd '$tarcmd' failed to run properly.\n");   
}

sub Validatetar
{
   my($tarname, $sfilestart, $sfileend) = @_;
   my($res);
   my($error);

   # examine contents
   $res = `$tarbin tf $tarname`;
   $error = $? >> 8;

   if ($error != 0)
   {
      LogWrite($logfh, "ERROR executing '$tarbin tf $tarname', error '$error'.\n");
      return $error;
   } 
   else 
   {
      my($tarstart) = undef;
      my($tarend) = undef;
      my(@sfiles);
      my($tcount) = 0;
      
      @sfiles = split(/\s*\n\s*/, $res);

      if ($#sfiles >= 0)
      {
         $tarstart = $sfiles[0];
         $tarend = $sfiles[$#sfiles];
         $tcount = $#sfiles + 1;
      }
      else
      {
         print STDERR "Unexpected empty tar.\n";
         return -9;
      }

      my($tfilestart) = ($tarstart =~ /^slony\S*_log\S*_(\d+)\.sql/);
      my($tfileend) = ($tarend =~ /^slony\S*_log\S*_(\d+)\.sql/);

      if ($sfilestart != $tfilestart)
      {
         LogWrite($logfh, "SQL logfile counter minimum ($tfilestart) does not match expected value ($sfilestart).\n");
         return -1;
      }

      if ($sfileend != $tfileend)
      {
         LogWrite($logfh, "SQL logfile counter maximum ($tfileend) does not match expected value ($sfileend).\n");
         return -2;
      }

      if ($tcount != ($sfileend - $sfilestart + 1))
      {
         LogWrite($logfh, "Number of SQL logfiles in tar ($tcount) does not match the expected number ($sfileend - $sfilestart + 1).\n");
         return -3;
      }
   }
   return 0;
}
__DATA__
