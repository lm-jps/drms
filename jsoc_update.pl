#!/usr/bin/perl -w 

# script for synchronizing your CVS working directory with the CVS JSOC module (new tree)

# must run from root of JSOC tree (not necessarily from $JSOCROOT)

# run this on each machine to be used.
#    n02 - for linux_X86_64 machines
#    n00 - for linux4 machines such as n00, phil, etc.
# 
#    n12 formerly used for AMD x86-64 can also be used instead of n02
#    lws Itaniam processors no longer supported for JSOC

$JSOCROOT = $ENV{"JSOCROOT"};
$CVSLOG = "cvsupdate.log";
$CVSSTATUS = "$JSOCROOT/base/util/scripts/cvsstatus.pl";

my($aidx) = 0;
my($arg);
my($line);
my($synccmd);
my($wdupdate) = 0;
my($lwd); # local working directory
my($rwd); # remote working directory
my($mach);

my(%fs2mount);
my(%mount2fs);

my(@machines) = 
(
    "n00",
    "n02"
);

# set up mappings
InitMaps();

while ($arg = shift(@ARGV))
{
    if (-d $arg)
    {
	# Script will update working directory - ensure this is an absolute path.

	my($lfspath) = GetFSPath("local", $arg);
	my($rfspath);
	my($rsp);
	my($savedp);

	foreach $mach (@machines)
	{
	    @rsp = GetFSPath($mach, $arg);
	    $rfspath = shift(@rsp);

	    if ($lfspath ne $rfspath)
	    {
		print STDERR "Path '$lfspath' is not mounted on '$mach'; bailng.\n";
		exit(1);
	    }
	}

	# May be a relative path
	$savedp = `pwd`;
	chdir($arg);
	$lwd = `pwd`;
	chdir($savedp);
	chomp($lwd);

	$wdupdate = 1;
    }
    else
    {
	print STDERR "Invalid JSOC working directory argument; bailing.\n";
	exit(1);
    }

    $aidx++;
}

if ($wdupdate != 1)
{
    if (!defined($JSOCROOT))
    {
	print STDERR "Environment variable 'JSOCROOT' not set; bailing.\n";
	exit(1);
    }
    
    if (!(-d $JSOCROOT))
    {
	print STDERR "Invalid JSOC root directory; bailing.\n";
	exit(1);
    }

    $lwd = $JSOCROOT;
}

# First, synchronize with CVS repository
print STDOUT "####### Start cvs update ####################\n";
$synccmd = "(cd $lwd; jsoc_sync.pl -l$CVSLOG)";
print "Calling '$synccmd'.\n";
system($synccmd);

print STDOUT "##\n";
print STDOUT "## A scan of $CVSLOG for files with conflicts follows:\n";
print STDOUT "## Start scanning cvsupdate.log\n";
system("(cd $lwd; grep '^C ' $CVSLOG)");

print STDOUT "## Done scanning cvsupdate.log\n";
print STDOUT "## Any lines starting with a 'C' between the 'Start' and 'Done' lines above should be fixed.\n";
print STDOUT "## See release notes to deal with 'C' status conflicts.\n";
print STDOUT "##\n";
print STDOUT "## Now Check cvsstatus for files that should be in the release ##\n";
print STDOUT "####### Start checking status ####################\n";
    
if (-e $CVSSTATUS)
{
    system("cd $lwd; $CVSSTATUS");

    print STDOUT "####### Done checking status ####################\n";
	print STDOUT "## If no lines between the 'Start' and 'Done' lines then there are no cvsstatus problems.\n";
    print STDOUT "## Continue with 'cont' when ready.\n";

    $line = <STDIN>;
    chomp($line);
    $line = lc($line);

    if ($line =~ /.*cont.*/)
    {
	my(@rsp);
	my($lfspath);
	my($machtype);
	my($echocmd) = 'echo $JSOC_MACHINE';

	system("(cd $lwd ./configure)");

	@rsp = GetFSPath("local", $lwd);
	$lfspath = shift(@rsp);

	foreach $mach (@machines)
	{
	    $machtype = `ssh $mach '$echocmd'`;
	    chomp($machtype);

	    print STDOUT "start build on $machtype\n";
	    @rsp = GetMountPath($mach, $lfspath);
	    $rwd = shift(@rsp);	    
	    system("(ssh $mach $rwd/make_jsoc.csh) 1>make_jsoc_$machtype.log 2>&1");
	    print STDOUT "done on $machtype\n";
	}
    }
    else
    {
	print STDOUT "Bailing upon user request.\n";
	exit(0);
    }
}
else
{
    print STDERR "Required script $CVSSTATUS missing; bailing.\n";
    exit(1);
}

print STDOUT "JSOC update Finished.\n";

sub InitMaps
{
    my($first);
    my($mach);
    my($fs);
    my($mountpoint);

    # current machine
    open(DFCMD, "df |");
    $first = 1;
    while (defined($line = <DFCMD>))
    {
	if ($first == 1)
	{
	    $first = 0;
	    next;
	}
	chomp($line);
	if ($line =~ /^(\S+)\s+.+\s+(\S+)$/)
	{
	    if (defined($1) && defined($2))
	    {
		$fs = $1;
		$mountpoint = $2;
		$fs =~ s/g:/:/;
		$fs2mount{"local"}->{$fs} = $mountpoint;
		$mount2fs{"local"}->{$mountpoint} = $fs;
	    }
	}
    }
    close DFCMD;

    # remote machines
    foreach $mach (@machines)
    {
	open(DFCMD, "ssh $mach df |");
	$first = 1;
	while (defined($line = <DFCMD>))
	{
	    if ($first == 1)
	    {
		$first = 0;
		next;
	    }
	    chomp($line);
	    if ($line =~ /^(\S+)\s+.+\s+(\S+)$/)
	    {
		if (defined($1) && defined($2))
		{
		    $fs = $1;
		    $mountpoint = $2;
		    $fs =~ s/g:/:/;
		    $fs2mount{$mach}->{$fs} = $mountpoint;
		    $mount2fs{$mach}->{$mountpoint} = $fs;
		}
	    }
	}

	close DFCMD;
    }
}

# input is a mount path (could be relative to working directory)
sub GetFSPath
{
    my($mach, $path) = @_;
    my($mountpath);
    my($fs);
    my($mountpoint);
    my(@fsinfo);
    my(@ret);
    my($fspath);
    my($savedp);

    # May be a relative path
    $savedp = `pwd`;
    chdir($path);
    $mountpath = `pwd`;
    chdir($savedp);
    chomp($mountpath);

    @fsinfo = GetFS($mach, $mountpath);
    $fs = shift(@fsinfo);
    $mountpoint = shift(@fsinfo);
    $fspath = $mountpath;
    $fspath =~ s/$mountpoint/${fs}::/;

    push(@ret, $fspath);
    return @ret;
}

# input is a FS path
sub GetMountPath
{
    my($mach, $fspath) = @_;
    my($mountpath);
    my($mountpoint);
    my($fs);
    my(@ret);
    
    $fs = $fspath;
    if ($fs =~ /(.+)::/)
    {
	$fs = $1;
    }

    $mountpoint = $fs2mount{$mach}->{$fs};

    $mountpath = $fspath;
    $mountpath =~ s/${fs}::/$mountpoint/;
    push(@ret, $mountpath);
    return @ret;
}

sub GetFS
{
    my($mach, $path) = @_;
    my($fs);
    my($mountpoint);
    my(@ret);


    if ($mach eq "local")
    {
	$fs = `df $path`;
    }
    else
    {
	$fs = `ssh $mach df $path`;
    }

    if ($fs =~ /.+\n(\S+)\s+.+\s+(\S+)$/)
    {
	$fs = $1;
	$mountpoint = $2;
    }
    else
    {
	 print STDERR "Invalid 'df' response; bailing.\n";
	 exit(1);
    }

    # This is SU-specific!
    $fs =~ s/g:/:/;

    push(@ret, $fs);
    push(@ret, $mountpoint);
    return @ret;
}
