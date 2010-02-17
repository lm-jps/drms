#!/usr/bin/perl -w 

# Art 2-17-2010
# Returns 1 if the lock was obtained; returns 0 otherwise
# drms_flock.pl -w 10 -f home/arta/slonydev/testarchive/testlogs/parselock.txt -c ""

use FileHandle;
use Fcntl ':flock';

my($arg);
my($cmd);
my($timeout);
my($lockfile);
my($lockfh);

# defaults
$timeout = 10;
$lockfile = "drmsflock.txt";

while ($arg = shift(@ARGV))
{
   if ($arg eq "-c")
   {
      $cmd = shift(@ARGV);
   }
   elsif ($arg eq "-w")
   {
      $timeout = sprintf("%d", shift(@ARGV));
   }
   elsif ($arg eq "-f")
   {
      $lockfile = shift(@ARGV);
   }
   else
   {
      PrintUsage();
      exit(0);
   }
}

$lockfh = FileHandle->new(">$lockfile");

my($natt) = 0;
while (1)
{
   if (flock($lockfh, LOCK_EX|LOCK_NB)) 
   {
      last;
   }
   else
   {
      if ($natt < $timeout)
      {
         sleep 1;
      }
      else
      {
         print STDERR "Couldn't obtain lock after $timeout tries.\n";
         exit(0);
      }
   }

   $natt++;
}

# execute the script passed in
system($cmd);

exit(1);
