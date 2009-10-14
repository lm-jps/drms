#!/usr/bin/perl -w 

# Determine which compilers are installed; then set make variables to indicate that
# Also, set DEFAULT values for Stanford-specific (if running at Stanford) make variables.
# To override the DEFAULT Stanford values, create a config.local file.

use constant ICCMAJOR => 9;
use constant ICCMINOR => 0;
use constant IFORTMAJOR => 9;
use constant IFORTMINOR => 0;
use constant GCCMAJOR => 3;
use constant GCCMINOR => 0;
use constant GFORTMAJOR => 4;
use constant GFORTMINOR => 2;

my($arg);
my($pos);
my($outfile);
my($major);
my($minor);
my($hasicc);
my($hasifort);
my($hasgcc);
my($hasgfort);

while ($arg = shift(@ARGV))
{
    if (($pos = index($arg, "-f", 0)) == 0)
    {
	$outfile = substr($arg, 2);
    }
}

$hasicc = 0;
$hasifort = 0;
$hasgcc = 0;
$hasgfort = 0;

if (defined($outfile))
{
    # Try icc
    $ans = `icc -V 2>&1`;      

    if (defined($ans) && $ans =~ /Version\s+(\d+)\.(\d+)/)
    {
        $major = $1;
        $minor = $2;

        if (IsVersion($major, $minor, ICCMAJOR, ICCMINOR))
        {
            $hasicc = 1;
        }
    }

    # Try gcc
    if (!$hasicc)
    {
        $ans = `gcc -v 2>&1`;
        if (defined($ans) && $ans =~ /gcc\s+version\s+(\d+)\.(\d+)/)
        {
            $major = $1;
            $minor = $2;

            if (IsVersion($major, $minor, GCCMAJOR, GCCMINOR))
            {
                $hasgcc = 1;
            }
        }
    }

    # Try ifort
    $ans = `ifort -V 2>&1`;
    if (defined($ans) && $ans =~ /Version\s+(\d+)\.(\d+)/)
    {
        $major = $1;
        $minor = $2;

        if (IsVersion($major, $minor, IFORTMAJOR, IFORTMINOR))
        {
            $hasifort = 1;
        }
    }

    # Try gfortran
    if (!$hasifort)
    {
        $ans = `gfortran -v 2>&1`;
        if (defined($ans) && $ans =~ /gcc\s+version\s+(\d+)\.(\d+)/)
        {
            $major = $1;
            $minor = $2;

            if (IsVersion($major, $minor, GFORTMAJOR, GFORTMINOR))
            {
                $hasgfort = 1;
            }
        }
    }

    open(OUTFILE, ">>$outfile") || die "Couldn't open file $outfile for writing.\n";

    # Error messages
    if (!$hasicc && !$hasgcc)
    {
        print "Fatal error: Acceptable C compiler not found! You will be unable to build the DRMS library.\n";
    }
    elsif ($hasicc)
    {
        print OUTFILE "JSOC_AUTOCOMPILER = icc\n";
    }
    else
    {
        print OUTFILE "JSOC_AUTOCOMPILER = gcc\n";
    }

    if (!$hasifort && !$hasgfort)
    {
        print "Warning: Acceptable Fortran compiler not found! Fortran interface will not be built, and you will be unable to build Fortran modules.\n";
    }
    elsif ($hasifort)
    {
        print OUTFILE "JSOC_AUTOFCOMPILER = ifort\n";
    }
    else
    {
        print OUTFILE "JSOC_AUTOFCOMPILER = gfortran\n";
    }

    # Set DEFAULT values for Stanford-specific (if running at Stanford) make variables.
    if (-e "suflag.txt") 
    {
       my($line);

       if (open(SUFLAG, "<suflag.txt"))
       {
          while (defined($line = <SUFLAG>))
          {
             chomp($line);
             if ($line !~ /^#/ && $line =~ /\S+/)
             {
                print OUTFILE "$line\n";
             }
          }

          close(SUFLAG);
       }
    }

    close(OUTFILE);
}

sub IsVersion
{
    my($maj1) = $_[0];
    my($min1) = $_[1];
    my($maj2) = $_[2];
    my($min2) = $_[3];

    my($res) = 0;
    
    if ($maj1 > $maj2 || ($maj1 == $maj2 && $min1 >= $min2))
    {
        $res = 1;
    }
    
    return $res;
}
