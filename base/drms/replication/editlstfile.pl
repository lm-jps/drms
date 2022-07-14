#!/usr/bin/perl -w

use FileHandle;
use File::Copy;
use IO::Dir;
use Fcntl ':flock';

my($natt);
my($parselock);
my($tablesdir);
my($schema);
my($table);
my($series);
my($subscribelockpath);

$parselock = $ARGV[0];
$tablesdir = $ARGV[1];
$schema = $ARGV[2];
$table = $ARGV[3];
$subscribelockpath = $parselock;


system("(set -o noclobber; echo $$ > $subscribelockpath) 2> /dev/null");

if ($? == 0)
{
   $SIG{INT} = "ReleaseLock";
   $SIG{TERM} = "ReleaseLock";
   $SIG{HUP} = "ReleaseLock";


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
         unlink "$subscribelockpath";
         exit(1);
      }

      if (!open(LSTFILE, "<$item.bak"))
      {
         print STDERR "Unable to open $item.bak for reading.\n";
         move("$item.bak", "$item");
         unlink "$subscribelockpath";
         exit(1);
      } 

      if (!open(OUTFILE, ">$item"))
      {
         print STDERR "Unable to open $item for writing.\n";
         move("$item.bak", "$item");
         unlink "$subscribelockpath";
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
         unlink "$subscribelockpath";
         exit(1);
      }
      else
      {
         unlink "$item.bak";
      }

      close(LSTFILE);
   }

   untie(%allfiles);

   unlink "$subscribelockpath";

   $SIG{INT} = 'DEFAULT';
   $SIG{TERM} = 'DEFAULT';
   $SIG{HUP} = 'DEFAULT';

  }
  else
  {
     print "Warning:: couldn't obtain subscribe lock; bailing.\n";
     exit(1);
  }

sub ReleaseLock
{
   unlink "$subscribelockpath";
   exit(1);
}



exit(0);
