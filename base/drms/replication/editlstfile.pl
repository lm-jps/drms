#!/usr/bin/perl -w

use FileHandle;
use File::Copy;
use IO::Dir;
use Fcntl ':flock';

my($plockfh);
my($natt);
my($parselock);
my($tablesdir);
my($schema);
my($table);
my($series);

$parselock = $ARGV[0];
$tablesdir = $ARGV[1];
$schema = $ARGV[2];
$table = $ARGV[3];

$plockfh = FileHandle->new(">$parselock");

$natt = 0;
while (1)
{
   if (flock($plockfh, LOCK_EX|LOCK_NB)) 
   {
      print "Created parse-lock file '$parselock'.\n";
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

chdir($tablesdir);
tie(my(%allfiles), "IO::Dir", ".");
my(@files) = keys(%allfiles);
my(@lstfiles) = map({$_ =~ /(.+\.lst)$/ ? $1 : ()} @files);

$series = "$schema\.$table";

foreach $item (@lstfiles)
{
   if (!move("$item", "$item.bak"))
   {
      print STDERR "Unable to move $item to $item.bak\n";
      exit(1);
   }

   if (!open(LSTFILE, "<$item.bak"))
   {
      print STDERR "Unable to open $item.bak for reading.\n";
      move("$item.bak", "$item");
      exit(1);
   } 

   if (!open(OUTFILE, ">$item"))
   {
      print STDERR "Unable to open $item for writing.\n";
      move("$item.bak", "$item");
      exit(1);
   } 

   # Right now .lst contains only table name, not schema; must add schema
   # ADD SCHEMA
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
      exit(1);
   }
   else
   {
      unlink "$item.bak";
   }

   close(LSTFILE);
}

untie(%allfiles);

flock($plockfh, LOCK_UN);
$plockfh->close;


exit(0);
