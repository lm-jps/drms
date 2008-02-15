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
my(@rsp);

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
	# May be a relative path
	my($lfspath);
	my($rfspath);
	my($savedp);

	@rsp = ResolvePath($arg);
	$lwd = shift(@rsp);

	@rsp = GetFSPath("local", $lwd);
	$lfspath = shift(@rsp);

	if ($lfspath !~ /:\S+::/)
	{
	    print STDERR "Path '$lwd' is mounted locally only; bailing.\n";
	    exit(1);
	}

	foreach $mach (@machines)
	{
	    # check for existence of mountpath on $mach that points to $lfspath
	    $rwd = GetMountPath($mach, $lfspath);

	    if ($rwd eq "")
	    {
		print STDERR "Path '$lwd' is not a network filesystem mounted on '$mach'; bailing.\n";
		exit(1);
	    }
	}

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

    @rsp = ResolvePath($JSOCROOT);
    $lwd = shift(@rsp);
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
	my($lfspath);
	my($machtype);
	my($echocmd) = 'echo $JSOC_MACHINE';

	system("(cd $lwd ./configure)");

	@rsp = GetFSPath("local", $lwd);
	$lfspath = shift(@rsp); # could be local only

	if ($lfspath !~ /:\S+::/)
	{
	    print STDERR "Path '$lwd' is mounted locally only; bailing.\n";
	    exit(1);
	}

	foreach $mach (@machines)
	{
	    $machtype = `ssh $mach '$echocmd'`;
	    chomp($machtype);

	    print STDOUT "start build on $machtype\n";
	    @rsp = GetMountPath($mach, $lfspath);
	    $rwd = shift(@rsp);	    
	    system("(ssh $mach $rwd/make_jsoc.pl) 1>make_jsoc_$machtype.log 2>&1");
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
	if ($line =~ /^(\S+:\S+)\s+.+\s+(\S+)$/)
	{
	    if (defined($1) && defined($2))
	    {
		$fs = $1;
		$mountpoint = $2;
		$fs =~ s/g:/:/;
		$fs2mount{"local"}->{$fs} = $mountpoint;
		$mount2fs{"local"}->{$mountpoint} = $fs;

		#print "$fs\t$mountpoint\n";
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
	    if ($line =~ /^(\S+:\S+)\s+.+\s+(\S+)$/)
	    {
		if (defined($1) && defined($2))
		{
		    $fs = $1;
		    $mountpoint = $2;
		    $fs =~ s/g:/:/;
		    $fs2mount{$mach}->{$fs} = $mountpoint;
		    $mount2fs{$mach}->{$mountpoint} = $fs;

		    #print "$fs\t$mountpoint\n";
		}
	    }
	}

	close DFCMD;
    }
}

sub ResolvePath
{
    my($path) = @_;
    my($savedp);
    my($rpath);
    my(@ret);

    $savedp = `pwd`;
    chdir($path);
    $rpath = `pwd`;
    chdir($savedp);
    chomp($rpath);

    push(@ret, $rpath);
    return @ret;
}

# input is an absolute path on $mach
sub GetFSPath
{
    my($mach, $mountpath) = @_;
    my($fs);
    my($mountpoint);
    my(@fsinfo);
    my(@ret);
    my($fspath);

    @fsinfo = GetFS($mach, $mountpath);
    $fs = shift(@fsinfo);
    $mountpoint = shift(@fsinfo);

    $fspath = $mountpath;
    $fspath =~ s/$mountpoint/${fs}::/;

    push(@ret, $fspath);
    return @ret;
}

# input is an FS path and a machine on which the FS is mounted
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

    if (defined($mountpoint))
    {
	$mountpath = $fspath;
	$mountpath =~ s/${fs}::/$mountpoint/;

	if ($mach eq "local")
	{
	    if (!(-d $mountpath))
	    {
		print STDERR "Directory '$mountpath' does not exist on machine '$mach'; bailing.\n";
		exit(1);
	    }
	}
	else
	{
	    open(STATCMD, "(ssh $mach stat $mountpath | sed 's/^/STDOUT:/') 2>&1 |");

	    while (defined($line = <STATCMD>))
	    {
		chomp($line);
		if ($line !~ /^STDOUT:/)
		{
		    if ($line =~ /No such file or directory/)
		    {
			print STDERR "Directory '$mountpath' does not exist on machine '$mach'; bailing.\n";
			close STATCMD;
			exit(1);
		    }
		}
	    }

	    close STATCMD;
	}

	push(@ret, $mountpath);
    }
    else
    {
	push(@ret, "");
    }

    return @ret;
}

# input is an absolute path on $mach 
sub GetFS
{
    my($mach, $path) = @_;
    my($fs);
    my($mountpoint);
    my(@ret);

    if ($mach eq "local")
    {
	if (-d $path)
	{
	    $fs = `df $path`;
	}
	else
	{
	    print STDERR "Directory '$path' does not exist on machine '$mach'; bailing.\n";
	    exit(1);
	}
    }
    else
    {
	open(STATCMD, "(ssh $mach stat $path | sed 's/^/STDOUT:/') 2>&1 |");

	while (defined($line = <STATCMD>))
	{
	    chomp($line);
	    if ($line !~ /^STDOUT:/)
	    {
		if ($line =~ /No such file or directory/)
		{
		    print STDERR "Directory '$path' does not exist on machine '$mach'; bailing.\n";
		    close STATCMD;
		    exit(1);
		}
	    }
	}

	$fs = `ssh $mach df $path`;
    }

    if ($fs =~ /.+\n(\S+)\s+.+\s+(\S+)$/)
    {
	$fs = $1;
	$mountpoint = $2;
    }
    else
    {
	 print STDERR "Invalid 'df' response '$fs'; bailing.\n";
	 exit(1);
    }

    # This is SU-specific!
    $fs =~ s/g:/:/;

    push(@ret, $fs);
    push(@ret, $mountpoint);
    return @ret;
}
