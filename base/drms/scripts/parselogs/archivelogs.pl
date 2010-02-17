#!/usr/bin/perl -w 

# Art 2-12-2010
#
# Perl version of the previous archive_slon_logs scripts (perl just
# so I can complete it more quickly). This script writes .tar.gz files
# that contain the begin and end counter values of the log files
# contained with the tar files.

# The format of the tar file names is:
#    slogs_([0-9]+)-([0-9]+)_.tar.gz

# Usage:
#   archivelogs.pl -l <log dir> -a <archive dir> -f <log filename format> -x <lock file>
#   archivelogs.pl -l /usr/local/pgsql/slon_logs/ -a /usr/local/pgsql/slon_logs/archive -f "slony\d+_log_\d+_(\d+)\.sql\.parsed" -x parselock.txt

use IO::Dir;
use FileHandle;
use Fcntl ':flock';
use Archive::Tar;


my($arg);
my($logdir);
my($archivedir);
my($format);

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

@fullpaths = map({"$logdir/$_"} @sorted);

$tar->add_files(@fullpaths);
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

# Remove lock
print "Removing parse-lock file.\n";
flock($lockfh, LOCK_UN);
$lockfh->close;

exit(0);

__DATA__
