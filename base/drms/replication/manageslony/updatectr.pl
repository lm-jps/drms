#!/usr/bin/perl


# This is run by the slon process after it creates a log file.  The log file is the LAST argument.
# Arguments before the last one are passed directly in the -x argument (slon -x "runparser.pl arg1 arg2").
# There should be only one argument, other than the log file argument. This argument is a path to the
# server-wide replication configuration file
#     updatectr.pl <replication server configuration file> <slony log file>

my($cfg);
my($logfname);
my($cntrfile);
my($line);

if ($#ARGV != 1)
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
      last; # we don't need anything else
   }
}

close(CNFFILE);

# Update the sync counter in the file tracking this.
@logfnameparts = split(/\//, $logfname);

$logfile = $logfnameparts[$#logfnameparts];

if ($logfile =~ /_(\d+).sql/)
{
   $logcntr = sprintf("%lld", $1);
}

# Truncate the old file, if it exists
open(CTRFILE, ">$cntrfile") || die "Unable to open counter file '$cntrfile'.\n";

# Do the write
print CTRFILE "$logcntr\n";
print CTRFILE "__EOF__\n";

close CTRFILE;
exit(0);

sub Usage
{
   print STDOUT "updatectr.pl <replication server configuration file> <slony log file>\n";
}
