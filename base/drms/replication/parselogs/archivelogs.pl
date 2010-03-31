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
use Archive::Tar;
use File::Copy;


my($arg);
my($logdir);
my($archivedir);
my($format);
my($cmd);
my($modpath);
my($logseries);

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
   if ($arg eq "-l")
   {
      $logdir = shift(@ARGV);
   }
   elsif ($arg eq "-a")
   {
      $archivedir = shift(@ARGV);
   }
   elsif ($arg eq "-f")
   {
      $format = shift(@ARGV);
   }
   elsif ($arg eq "-x")
   {
      $parselock = shift(@ARGV);
   }
   elsif ($arg eq "-m")
   {
      $modpath = shift(@ARGV);
   }
   elsif ($arg eq "-s")
   {
      $logseries = shift(@ARGV);
   }
   else
   {
      PrintUsage();
      exit(1);
   }
}

# parse_slony_logs and this script must also share a lock so that this script
# doesn't accidentally tar files that are currently being written.
# Must open file handle with write intent to use LOCK_EX

$lockfh = FileHandle->new(">$logdir/$parselock");

my($natt) = 0;
while (1)
{
   if (flock($lockfh, LOCK_EX|LOCK_NB)) 
   {
      print "Created parse-lock file '$logdir/$parselock'.\n";
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
my($tar) = Archive::Tar->new;
my($tarfile) = "$archivedir/slogs_$fcounter-$lcounter.tar.gz";
my(@fullpaths);
my($ifile);
my($currwd);

@fullpaths = map({"$logdir/$_"} @sorted);

$currwd = $ENV{'PWD'};
chdir($logdir);
$tar->add_files(@sorted);
chdir($currwd);

$tar->write($tarfile, COMPRESS_GZIP);

# Validate tar
my(@filelist) = $tar->list_files();
my($ftar) = ($filelist[0] =~ /$format/);
my($ltar) = ($filelist[$#filelist] =~ /$format/);

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

# Remove parse lock
print "Removing parse-lock file.\n";
flock($lockfh, LOCK_UN);
$lockfh->close;

# Copy archived tar file into SUMS
local $ENV{"LD_LIBRARY_PATH"} = "/usr/local/pgsql/lib";
$cmd = "$modpath/accessreplogs logs=$logseries path=$archivedir action=str regexp=\"slogs_([0-9]+)-([0-9]+)[.]tar[.]gz\"";
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


exit(0);

__DATA__
