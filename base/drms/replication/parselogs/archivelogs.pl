#!/usr/bin/perl -w 

# Art 2-12-2010
#
# Perl version of the previous archive_slon_logs scripts (perl just
# so I can complete it more quickly). This script writes .tar.gz files
# that contain the begin and end counter values of the log files
# contained with the tar files.

# The format of the tar file names is:
#    slogs_([0-9]+)-([0-9]+).tar.gz

# Usage:
#   archivelogs.pl -l <log dir> -a <archive dir> -f <log filename format> -x <lock file> -m <mod path> -s <log series>
#   archivelogs.pl -l /usr/local/pgsql/slon_logs/ -a /usr/local/pgsql/slon_logs/archive -f "slony\d+_log_\d+_(\d+)\.sql\.parsed" -x parselock.txt -m /b/devtest/arta/JSOC/bin/suse_x86_64 -s su_production.slonylogs

use IO::Dir;
use FileHandle;
use Fcntl ':flock';
use File::Copy;

use constant kTarChunk => 64;

my($arg);
my($cfg);
my($logdir);
my($archivedir);
my($format);
my($cmd);
my($accessrepcmd);
my($logseries);
my($tarcmd);
my($zipcmd);
my($line);
my($serverLockDir);

my($lockfh);

my(@lfiles);
my(@sorted);

# only one version of this program running
unless (flock(DATA, LOCK_EX|LOCK_NB)) 
{
   print "$0 is already running. Exiting.\n";
   exit(1);
}

while ($arg = shift(@ARGV))
{
   if ($arg eq "-c")
   {
      $cfg = shift(@ARGV);
   }
   elsif ($arg eq "-f")
   {
      $format = shift(@ARGV);
   }
   elsif ($arg eq "-x")
   {
      $parselock = shift(@ARGV);
   }
   elsif ($arg eq "-s")
   {
      $logseries = shift(@ARGV);
   }
   elsif ($arg eq "-t")
   {
      $tarcmd = shift(@ARGV);
   }
   elsif ($arg eq "-z")
   {
      $zipcmd = shift(@ARGV);
   }
   else
   {
      PrintUsage();
      exit(1);
   }
}

open(CNFFILE, "<$cfg") || die "Unable to read configuration file '$cfg'.\n";

while (defined($line = <CNFFILE>))
{
   chomp($line);
   
   if ($line =~ /^\#/ || length($line) == 0)
   {
      next;
   }

   # Collect arguments of interest
   if ($line =~ /^\s*kServerLockDir=(.+)/)
   {
      $serverLockDir = $1;
   }
   elsif ($line =~ /^\s*kPSLlogsSourceDir=(.+)/)
   {
      $logdir = $1;
   }
   elsif ($line =~ /^\s*kPSLarchiveDir=(.+)/)
   {
      $archivedir = $1;
   }
   elsif ($line =~ /^\s*kPSLaccessRepro=(.+)/)
   {
      $accessrepcmd = $1;
   }
}

close(CNFFILE);

# parse_slony_logs and this script must also share a lock so that this script
# doesn't accidentally tar files that are currently being written.
# Must open file handle with write intent to use LOCK_EX

$lockfh = FileHandle->new(">$serverLockDir/$parselock");

my($natt) = 0;
while (1)
{
   if (flock($lockfh, LOCK_EX|LOCK_NB)) 
   {
      print "Created parse-lock file '$serverLockDir/$parselock'.\n";
      last;
   }
   else
   {
      if ($natt < 10)
      {
         print "parse_slony_logs is currently modifying files - waiting for completion.\n";
         sleep 1;
      }
      else
      {
         print "couldn't obtain parse lock; bailing.\n";
         exit(1);
      }
   }

   $natt++;
}

tie(my(%logs), "IO::Dir", $logdir);
@lfiles = keys(%logs);
@sorted = sort({($a =~ /^$format/)[0] <=> ($b =~ /^$format/)[0]} map({$_ =~ /^($format)/ ? $1 : ()} @lfiles));

if ($#sorted < 0)
{
   # no log files to archive
   print "No log file to archive, exiting.\n";
   untie(%logs);
   flock($lockfh, LOCK_UN);
   $lockfh->close;
   exit(0);
}

# Create tarfile
my($fcounter) = -1;
my($lcounter) = -1;
my($nfiles) = $#sorted + 1;

if ($sorted[0] =~ /^$format/)
{
   $fcounter = sprintf("%020d", $1);
}

if ($sorted[$#sorted] =~ /^$format/)
{
   $lcounter = sprintf("%020d", $1);
}

print "Archiving logs files.\n";
my($tarfile) = "$archivedir/slogs_$fcounter-$lcounter.tar.gz";
my(@fullpaths);
my($ifile);
my($currwd);
my(@filelist);
my($ftar);
my($ltar);

@fullpaths = map({"$logdir/$_"} @sorted);

$currwd = $ENV{'PWD'};
chdir($logdir);
RunTar(0, $tarcmd, $zipcmd, "cfz", $archivedir, $tarfile, "", @sorted);
chdir($currwd);

# Validate tar
@filelist = RunTar(1, $tarcmd, $zipcmd, "tf", $archivedir, $tarfile, "", "");
$ftar = ($filelist[0] =~ /$format/)[0];
$ltar = ($filelist[$#filelist] =~ /$format/)[0];

untie(%logs);

print "$#filelist + 1 == $nfiles, $ftar == $fcounter, $ltar == $lcounter\n";

if ($#filelist + 1 != $nfiles || $ftar != $fcounter || $ltar != $lcounter)
{
   print STDERR "Could not create tar file properly.\n"
}
else
{
   print "Successfully created tar file '$tarfile'.\n";

   # Remove files from log dir
   foreach $ifile (@fullpaths)
   {
      print "Remove file $ifile.\n";
      unlink($ifile);
   }
}

####### ART ###########

exit;

#######################

# Remove parse lock
print "Removing parse-lock file.\n";
flock($lockfh, LOCK_UN);
$lockfh->close;

# Copy archived tar file into SUMS
local $ENV{"LD_LIBRARY_PATH"} = "/usr/local/pgsql/lib";
$cmd = "$accessrepcmd logs=$logseries path=$archivedir action=str regexp=\"slogs_([0-9]+)-([0-9]+)[.]tar[.]gz\"";
print "running $cmd\n";
system($cmd);

if ($? == -1)
{
   print STDERR "Failed to execute '$cmd'.\n";
   exit(1);
}
elsif ($? & 127)
{
   print STDERR "Failed to copy archive files into SUMS - accessreplogs crashed.\n";
   exit(1);
}
elsif ($? >> 8 != 0)
{
   print STDERR "Failed to copy archive files into SUMS - accessreplogs ran unsuccessfully.\n";
   exit(1);
}
else
{
   # Remove original tar files (put them in a trash folder that gets cleaned up once in a while)
   # For now, just keep originals until we're sure that this is working properly.
   my($backup) = "$archivedir/trash";
   if (!(-e $backup))
   {
      if (!mkdir($backup))
      {
         print STDERR "Could not create subdirectory 'trash'.\n";
         exit(1);
      }
   }

   tie(my(%tars), "IO::Dir", $archivedir);
   my(@selfiles);
   @lfiles = keys(%tars);
   @selfiles = map({$_ =~ /slogs_[0-9]+-[0-9]+[.]tar[.]gz/ ? $_ : ()} @lfiles);
   @fullpaths = map({"$archivedir/$_"} @selfiles);

   foreach $ifile (@fullpaths)
   {
      if (!move($ifile, $backup))
      {
         print STDERR "Error moving file '$ifile' to '$backup'.\n";
         untie(%tars);
         exit(1);
      }
   }

   untie(%tars);
}

sub RunTar
{
   my($capture, $tarbin, $zipbin, $op, $archivedir, $tarfile, $options, @list) = @_;
   my($cmd);
   my($res);
   my($onefile);
   my(@chunk);
   my($ifile);
   my(@res);
   my($compress);
   my($tmpfile);

   $tmpfile = "$archivedir/.tmp.tar";

   if ($op =~ /z/)
   {
      # remove compression for now
      $op =~ s/z//g;
      $file = $tmpfile;
      $compress = 1;
   }
   else
   {
      $file = $tarfile;
      $compress = 0;
   }

   if ($op =~ /c/ || $op =~ /r/)
   {
      $op =~ s/c/r/g;

      # chunk it
      @chunk = ();
      $ifile = 1;

      foreach $onefile (@list)
      {
         push(@chunk, $onefile);

         if ($ifile % kTarChunk == 0)
         {
            # Execute tar cmd.
            $cmd = "$tarbin $op $file $options @chunk";
            print "Running tar '$cmd'\n";

            if ($capture)
            {
               @res = `$cmd`;
            }
            else
            {
               $res[0] = system($cmd);
               ($res[0] == 0) || die("tar cmd '$cmd' failed to run properly.\n");
            }

            @chunk = ();
         }

         $ifile++;
      }

      if ($#chunk >= 0)
      {
          # Execute tar cmd.
         $cmd = "$tarbin $op $file $options @chunk";
         print "Running tar '$cmd'\n";

         if ($capture)
         {
            @res = `$cmd`;
         }
         else
         {
            $res[0] = system($cmd);
            ($res[0] == 0) || die("tar cmd '$cmd' failed to run properly.\n");
         }
      }
   }
   else
   {
      $cmd = "$tarbin $op $file $options @list";
      print "Running tar '$cmd'\n";

      if ($capture)
      {
         @res = `$cmd`;
      }
      else
      {
         $res[0] = system($cmd);
         ($res[0] == 0) || die("tar cmd '$cmd' failed to run properly.\n");
      }
   }

   if ($compress)
   {
      # compress now
      $gzcmd = "$zipbin --best $tmpfile";

      $res[0] = system($gzcmd);
      ($res[0] == 0) || die("gzip cmd '$gzcmd' failed to run properly.\n"); 

      if (!move("$archivedir/.tmp.tar.gz", $tarfile))
      {
         print STDERR "Unable to move .tmp.tar.gz to $tarfile.\n";
      }
   }

   return @res;
}

exit(0);

__DATA__
