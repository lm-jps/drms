#!/usr/bin/perl -w 

use Cwd;

my($sumslogdir);
my($sumsmanager);
my($script);

if (!(-e "suflag.txt") && !(-e "config.local"))
{
    # Use config.local.template
    system("./configure");
}

# Get the variables from config.local that are needed
if (-e "config.local")
{
    my($line);

    open(CONFIGLOC, "<config.local");
    while (defined($line = <CONFIGLOC>))
    {
        chomp($line);
        if ($line =~ /SUMS_LOG_BASEDIR\s+(.+)/)
        {
            $sumslogdir = $1;
        }
        elsif ($line =~ /SUMS_MANAGER\s+(.+)/)
        {
            $sumsmanager = $1;
        }
    }
    close(CONFIGLOC);

    if (defined($sumslogdir))
    {
        $script = "$sumslogdir/sum_rm.cfg";
        print STDOUT "*** generating template $script ***\n";

        open(SUMSRMCFG, ">$script") || die "this script $script must be run by someone with write access\n  to the directory $sumslogdir (which must also exist),\n  e.g. $sumsmanager, if you will be running SUMS on this server";

        print SUMSRMCFG "# configuration file for sum_rm program\n#\n";
        print SUMSRMCFG "# when done, sleep for n seconds before re-running\n";
        print SUMSRMCFG "SLEEP=900\n";
        print SUMSRMCFG "# delete until this many Mb free on SUMS partitions\n";
        print SUMSRMCFG "MAX_FREE_0=100000\n";
        print SUMSRMCFG "# log file (only opened at startup and pid gets appended to this name)\n";
        print SUMSRMCFG "LOG=$sumslogdir/sum_rm.log\n";
        print SUMSRMCFG "# whom to bother when there's a notable problem\n";
        print SUMSRMCFG "MAIL=$sumsmanager\n";
        print SUMSRMCFG "# to prevent sum_rm from doing anything set non-0\n";
        print SUMSRMCFG "NOOP=0\n";
        print SUMSRMCFG "# sum_rm can only be enabled for a single user\n";
        print SUMSRMCFG "USER=$sumsmanager\n";
        print SUMSRMCFG "# dont run sum_rm between these NORUN hours of the day (0-23)\n";
        print SUMSRMCFG "# comment out to ignore or set them both to the same hour\n";
        print SUMSRMCFG "# The NORUN_STOP must be >= NORUN_START\n";
        print SUMSRMCFG "# dont run when the hour first hits NORUN_START\n";
        print SUMSRMCFG "NORUN_START=7\n";
        print SUMSRMCFG "# start running again when the hour first hits NORUN_STOP\n";
        print SUMSRMCFG "NORUN_STOP=7\n";

        close(SUMSRMCFG);

        print STDOUT "\nBe sure to review file $script\n";
        print STDOUT "  and make appropriate modifications if you will be running SUMS\n"
    }
}
