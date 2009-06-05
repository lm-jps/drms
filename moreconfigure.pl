#!/usr/bin/perl -w 

# Determine which compilers are installed; then set make variables to indicate that

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
        print "ERROR: Acceptable C compiler not found!\n";
    }
    elsif ($hasicc)
    {
        print OUTFILE "AUTOCOMPILER = icc\n";
    }
    else
    {
        print OUTFILE "AUTOCOMPILER = gcc\n";
    }

    if (!$hasifort && !$hasgfort)
    {
        print "ERROR: Acceptable Fortran compiler not found!\n";
    }
    elsif ($hasifort)
    {
        print OUTFILE "AUTOFCOMPILER = ifort\n";
    }
    else
    {
        print OUTFILE "AUTOFCOMPILER = gfortran\n";
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
