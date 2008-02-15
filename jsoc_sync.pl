#!/usr/bin/perl -w 

# script for synchronizing your CVS working directory with the CVS JSOC module (new tree)

# must run from root of JSOC tree (not necessarily from $JSOCROOT)

# run this on each machine to be used.
#    n02 - for linux_X86_64 machines
#    n00 - for linux4 machines such as n00, phil, etc.
# 
#    n12 formerly used for AMD x86-64 can also be used instead of n02
#    lws Itaniam processors no longer supported for JSOC

$LATESTREL = "Ver_LATEST";
$MODSPEC = "modulespec.txt";
$CVSLOG = "cvsupdate.log";

my($aidx) = 0;
my($arg);
my($pos);
my($rev) = "";
my($line);

my($cvsmod);
my(%mods);
my($su);

while ($arg = shift(@ARGV))
{
    if ($arg eq "-R")
    {
	$rev = "-r $LATESTREL";
    }
    elsif (($pos = index($arg, "-l", 0)) == 0)
    {
	$CVSLOG = substr($arg, 2);
    }

    $aidx++;
}

# Initialize mods map
$DRMS = "DRMS";
$JSOC = "JSOC";
$LEV0 = "LEV0TBLS";

$mods{$DRMS} = $DRMS;
$mods{$JSOC} = $JSOC;
$mods{$LEV0} = $LEV0;

if (-e "suflag.txt")
{
    $su = 1;
}
else
{
    $su = 0;
}

# remove old log file
if (-e $CVSLOG)
{
    unlink $CVSLOG;
}

if ($su)
{
    print "Synchronizing JSOC (Stanford) user\n";

    if (-e $MODSPEC)
    {
	open(SPECFILE, $MODSPEC);
	while ($line = <SPECFILE>)
	{
	    if ($line =~ /^\#.*/)
	    {
		next;
	    }
	    elsif (length($line) == 0)
	    {
		next;
	    }
	    elsif ($line =~ /\s*(.+)\s*(\#*\s*)?/)
	    {
		$cvsmod = $1;
		
		if (defined($cvsmod = $mods{uc($1)}))
		{
		    if ($cvsmod ne $DRMS)
		    {
			CallCVS($rev, $cvsmod);
		    }
		    else
		    {
			print STDERR "Your working directory contains the full JSOC code. Can't specify the CVS module 'DRMS' - skipping.\n";
		    }
		}
		else
		{
		    print STDERR "Invalid CVS module name $1 - skipping.\n";
		}
	    }
	    else
	    {
		print STDERR "Syntax error '$line' in module specification file $MODSPEC\n";
	    }
	}

	close SPECFILE;
    }
    else
    {
	# Just do CVS JSOC module
	CallCVS($rev, $JSOC);
    }
}
else
{
    print "Synchronizing DRMS (base system only) user\n";

    if (-e $MODSPEC)
    {
	open(SPECFILE, $MODSPEC);
	while ($line = <SPECFILE>)
	{
	    if ($line =~ /^\#.*/)
	    {
		next;
	    }
	    elsif (length($line) == 0)
	    {
		next;
	    }
	    elsif ($line =~ /\s*(.+)\s*(\#*\s*)?/)
	    {
		$cvsmod = $1;
		
		if (defined($cvsmod = $mods{uc($1)}))
		{
		    if ($cvsmod ne $JSOC)
		    {
			CallCVS($rev, $cvsmod);
		    }
		    else
		    {
			print STDERR "Your working directory contains the base JSOC code. Can't specify the CVS module 'JSOC' - skipping.\n";
		    }
		}
		else
		{
		    print STDERR "Invalid CVS module name $1 - skipping.\n";
		}
	    }
	    else
	    {
		print STDERR "Syntax error '$line' in module specification file $MODSPEC\n";
	    }
	}

	close SPECFILE;
    }
    else
    {
	CallCVS($rev, $DRMS);
    }
}

print "JSOC synchronization finished.\n";

sub CallCVS
{
    my($rev, $cvsmod) = @_;
    my($updatecmd);
    my($wd);
    my($parent);

    $updatecmd = "cvs checkout -AP $rev $cvsmod";
    $wd = `pwd`;
    chomp($wd);

    # Ack, need to manually determine parent directory - don't use ".."
    if ($wd =~ /(.+)\/[^\/]+$/)
    {
	$parent = $1;
    }

    #Things that didn't really work:
    #$updatecmd = "(cd ..; $updatecmd | sed 's/^/STDOUT:/') 2>&1 |";
    #$updatecmd = "(cd $parent; $updatecmd) |";

    $updatecmd = "(cd $parent; $updatecmd) 1>>$CVSLOG 2>&1";
    print "Calling '$updatecmd'.\n";
    system($updatecmd);
}
