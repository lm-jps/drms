# !/usr/bin/perl -w 

# This script downloads the latest data files from the MOC Product Server (@Goddard).
# It reads in a config file (default is ___), which is specified by the -c parameter.
# This config file lists the 

#   -c - Required.  Config file.
#   -s - Required.  Status file.
#   -r - Required.  Root directory.  The root location where the local copy of the files will be
#        placed.  Paths specified in the config file are relative to this root.
#   -t - timeout.  Optional.  This script calls an expect script and passes this timeout value
#        to it
#   -f - force download of all files that failed to previously download


# Sample config file:
#   root:moc/
#   # comment-lines beginning with a '#' are ignored
#   # |r denotes recursive
#   filespec:fds/ |r                
#   filespec:lpz
#   filespec:lpz/dir1/ |r
#   filespec:lpz/dir2/
#   # wildcards ok, dlMOCDataFiles.pl uses sftp ls.
#   filespec:another/*.S01

use FileHandle;
use IPC::Open2;
use IPC::Open3;

$EXP = "/home/jsoc/sdo/sftpScript.exp";
# Use $EXP below for testing
# $EXP = "./sftpScript.exp";
$RQARG = "hmiroboto\@129.165.8.6";
$RDCMD = "scp hmiroboto\@129.165.8.6:";
%STATMAP;
$FSFILE = "fslist";

my($configFile) = "mocDlSpec.txt"; # input file; defaults to config in working directory
my($statusFile) = "mocDlStatus.txt"; # file to write status to; defaults to file in working directory
my($localRoot);
my($timeout);
my($forceDL);
my(@fileSpecs);
my(@rFileSpecs);
my(@nrFileSpecs);
my($nRSpecs) = 0;
my($nNRSpecs) = 0;

my(@successDL);
my(@unsuccessDL);

my($argc) = scalar(@ARGV);
if ($argc == 0)
{
    PrintUsage();
    exit(1);
}
else
{
    my($arg);

    while ($arg = shift(@ARGV))
    {
	if ($arg eq "-c")
	{
	    # config file
	    $configFile = shift(@ARGV);
	    if (!defined($configFile))
	    {
		PrintUsage();
		exit(1);
	    }
	}
	elsif ($arg eq "-s")
	{
	    # status file
	    $statusFile = shift(@ARGV);
	    if (!defined($configFile))
	    {
		PrintUsage();
		exit(1);
	    }
	}
	elsif ($arg eq "-r")
	{
	    # root directory
	    $localRoot = shift(@ARGV);
	    if (!defined($configFile))
	    {
		PrintUsage();
		exit(1);
	    }

	    if ($localRoot =~ /(\S+)\s*/)
	    {
		$localRoot = $1;
	    }
	    
	    if ($localRoot !~ /.+\/$/)
	    {
		$localRoot = $localRoot . "/";
	    }
	}
	elsif ($arg eq "-t")
	{
	    # timeout (for expect script)
	    $timeout = shift(@ARGV);
	    if (!defined($timeout))
	    {
		PrintUsage();
		exit(1);
	    }
	}
	elsif ($arg eq "-f")
	{
	    $forceDL = 1;
	}
	else
	{
	    print stderr $arg;
	    PrintUsage();
	    exit(1);
	}
    }

    # Read the last status file - store in an hash array %STATMAP, keyed by remote path.
    ReadStatusFile($statusFile);
    
    # Examine the remote files specified in the config file.
    # If the file does not exist in the status file, try to download it.
    @fileSpecs = ParseConfigFile($configFile, $localRoot);

    $remoteRoot = $fileSpecs[0];
    $nRSpecs = $fileSpecs[1];
    $nNRSpecs = $fileSpecs[2];

    my($nrSpecsIdx) = $nRSpecs + 3;
    @rFileSpecs = @fileSpecs[3..$nrSpecsIdx - 1];
    @nrFileSpecs = @fileSpecs[$nrSpecsIdx..@fileSpecs - 1];

    my(@dlStatus);
    if (@rFileSpecs > 0)
    {
	@dlStatus = DownloadApplicableFiles("r", $remoteRoot, $localRoot, $forceDL, @rFileSpecs);
    }
    if (@nrFileSpecs > 0)
    {
	@dlStatus = (@dlStatus, DownloadApplicableFiles("n", $remoteRoot, $localRoot, $forceDL, @nrFileSpecs));
    }

    WriteStatusFile($statusFile, @dlStatus);
}
# end main

sub ResolveSpans
{
    my($fs) = @_;
    my(@ret);

    # are there any spans in $fileSpec?
    if ($fs =~ /(.*?)\|s\[([0-9]+)\-([0-9]+)\](.*)/)
    {
	my($begin) = $2;
	my($end) = $3;
	my($pad) = 0;

	if (length($begin) == length($end))
	{
	    $pad = length($begin);
	}
	
	my($indx);
	for ($indx = $begin; $indx <= $end; $indx++)
	{
	    my($format);
	    
	    if ($pad > 0)
	    {
		$format = "%0" . $pad . "d";
	    }
	    else
	    {
		$format = "%d";
	    }
	    
	    my($intermed) = $1 .sprintf($format, $indx) . $4;
	    push(@ret, ResolveSpans($intermed));
	}	    
    }
    else
    {
	push(@ret, $fs);
    }

    return @ret;
}

sub ParseConfigFile
{
    my($filePath, $localRoot) = @_;
    my($line);
    my($remoteRoot);
    my($directive);
    my($fileSpec);
    my($modifier);
    my($destination);
    my(@specs);
    my(@rSpecs);
    my(@nrSpecs);
    my($useRecursive);
    my($nRecursive) = 0; # number of recursive file specs
    my($nNRecursive) = 0; # number of non-recursive file specs

    # specs[0] = nRecursive;
    # specs[1] = nNRecursive;

    open(CFILE, $filePath) || die "Couldn't open config file: $filePath\n";
    
    while($line = <CFILE>)
    {
	chomp($line);
	if (length($line) == 0 || $line =~ /^\#.*/)
	{
	    # strip comments
	    next;
	}

	if ($line =~ /s*(\w+):(\S+)\s+(\S)\s*/)
	{
	    $directive = $1;
	    $fileSpec = $2;
	    $destination = $3;

	}
	elsif ($line =~ /s*(\w+):(\S+)\s*/)
	{
	    # no destination, leave $destination undefined and use default destination
	    $directive = $1;
	    $fileSpec = $2;	    
	}
	else
	{
	    die "Bad config file format: $line.\n";
	}

	# are there any modifiers in $fileSpec?
	if ($fileSpec =~ /^\|(\w)(\S+)/)
	{
	    $fileSpec = $2;
	    $modifier = $1;
	}

	if ($directive eq "root")
	{
	    $remoteRoot = $fileSpec;

	    if ($remoteRoot =~ /(\S+)\s*/)
	    {
		$remoteRoot = $1;
	    }
	    
	    if ($remoteRoot !~ /.+\/$/)
	    {
		$remoteRoot = $remoteRoot . "/";
	    }
	    
	    if (defined($modifier))
	    {
		die "Modifier not allowed in $line.\n";
	    }
	}
	elsif ($directive eq "filespec")
	{
	    if (defined($modifier))
	    {
		if ($modifier eq "r")
		{
		    $useRecursive = 1;
		}
		else
		{
		    # no other modifiers defined currently
		    die "Bad fileSpec modifier $modifier.\n";
		}
	    }
	    else
	    {
		$useRecursive = 0;
	    }

	    my(@resolvedFS) = ResolveSpans($fileSpec);
	    my($oneFS);

	    if ($useRecursive != 0)
	    {
		while (defined($oneFS = shift(@resolvedFS)))
		{
		    $rSpecs[@rSpecs] = $oneFS;
		    $nRecursive++;
		}
	    }
	    else
	    { 
		while (defined($oneFS = shift(@resolvedFS)))
		{
		    $nrSpecs[@nrSpecs] = $oneFS;
		    $nNRecursive++;
		}
	    }
	}
	else
	{
	    die "Invalid directive: " . $directive;
	}
    }

    print "remote root: $remoteRoot\n";
    print "local root: $localRoot\n";
    
    if (!defined($remoteRoot) || !defined($localRoot))
    {
	die "Missing remote or local root.\n";
    }    

    @specs = ($remoteRoot, $nRecursive, $nNRecursive, @rSpecs, @nrSpecs);

    return @specs;
}

sub DownloadApplicableFiles
{
    my($recFlag, $remoteRoot, $localRoot, $forceDL, @dirsToDL) = @_;

    my(@ret);
    my(@allFiles);   
    my($disposition);

    # turn $fileSpec into actual files
    @allFiles = ProcessFileSpecs($recFlag, $remoteRoot, @dirsToDL); 

    if (@allFiles > 0)
    {
	my($fullRdCmd);
	my($filesDir);
	my($oneFile);
	
	while ($oneFile = shift(@allFiles))
	{
	    if (defined($disposition = $STATMAP{$oneFile}))
	    {
		# Can't download files that have been downloaded already.  Also
		# don't download files that failed to download previously (these
		# should be retrieved manually).
		if (!$forceDL || $disposition ne "notdownloaded")
		{
		    next;
		}
	    }
	    
	    # do the download
	    $disposition = DownloadFile($localRoot, $oneFile);
	    
	    if ($disposition eq "couldn't download")
	    {
		push(@ret, $oneFile);
		push(@ret, "notdownloaded");
	    }
	    elsif ($disposition eq "downloaded")
	    {
		push(@ret, $oneFile);
		push(@ret, "downloaded");
	    }
	    else
	    {
		die "Unknown download disposition $disposition.\n";
	    }
	}
    }

    return(@ret);
}

sub ProcessFileSpecs
{
    my(@ret);
    my($recFlag, $remoteRoot, @fileSpecs) = @_;
    my($oneFileSpec);
    my($line);
    my($expFsArg);
    my($nFS) = scalar(@fileSpecs);
    my($createFile) = 0;

    if ($nFS >= 20)
    {
	$createFile = 1;
    }

    # sort filespecs so sftpScript.exp optimally fetches.
    @fileSpecs = sort(@fileSpecs);

    if ($createFile)
    {
	open(FSFILE, ">$FSFILE") || die "Couldn't write fs file.\n";
    }
    
    # pass filespecs in file
    my($path);
    my($filename);
    my($mergedFS);
    my($mergedFSPath);
    my(@parts);
    my($disposition);
    
    while ($oneFileSpec = shift(@fileSpecs))
    {
	# Must merge specs that have the same path
	@parts = split(/::/, $oneFileSpec, -1);
	
	if (scalar(@parts) == 2)
	{
	    $path = $parts[0];
	    $filename = $parts[1];
	    
	    if ($path eq $mergedFSPath)
	    {
		$mergedFS = $mergedFS . "," . $filename;
	    }
	    else
	    {
		if (defined($mergedFS))
		{
		    if (!$createFile)
		    {
			$expFsArg = $expFsArg . $mergedFS . " ";
		    }
		    else 
		    {
			print FSFILE $mergedFS . "\n";
		    }
		}
		
		$mergedFSPath = $path;
		
		if ($recFlag eq "r")
		{
		    $mergedFS = "r=";
		}
		else
		{
		    $mergedFS = "n=";
		}
		
		$mergedFS = $mergedFS . $path . "::" . $filename;
	    }
	}
    }

    if (defined($mergedFS))
    {
	if (!$createFile)
	{
	    $expFsArg = $expFsArg . $mergedFS . " ";
	}
	else 
	{
	    print FSFILE $mergedFS . "\n";
	}
    }
    
    if ($createFile)
    {
	$expFsArg = "f=" . $FSFILE;
    }
    
    if (defined($timeout))
    {
	$timeout = "t=" . $timeout;
    }

    $expCmd = $EXP . " " . $RQARG . " " . $remoteRoot . " " . $timeout . " " . $expFsArg . " |";
    print $expCmd . "\n";

    open(EXPSESSION, $expCmd) || die "Couldn't run expect script.\n";
    
    my($emptyDir) = 0;
    my($wasSpec) = 0;
    my($stData) = 0;
    while ($line = <EXPSESSION>)
    {
	if ($line =~ /RESULTS/)
	{
	    $stData = 1;
	    next;
	}
	elsif ($line =~ /^\s*found:\s*(\S*)\s*/)
	{
	    if (defined($disposition = $STATMAP{$1}))
	    {
		if ($disposition ne "downloaded")
		{
		    print "  found " . $1 . "\n";
		    $emptyDir = 0;
		}
	    }
	    else
	    {
		print "  found " . $1 . "\n";
		$emptyDir = 0;
	    }
	}
	elsif ($line =~ /^\s*Processing/)
	{
	    if (!$wasSpec || !($line =~ /directory/))
	    {
		if ($emptyDir)
		{
		    print "  Found no files to download.\n";
		}
	    }

	    $emptyDir = 1;
	    
	    if ($line =~ /directory/)
	    {
		$wasSpec = 0;
	    }
	    else
	    {
		$wasSpec = 1;
	    }
 
	    chomp($line);
	    print $line . "...\n";
	}
	elsif ($line =~ /^\s*Couldn\'t stat/)
	{
	    print "  Path doesn't exist.\n";
	}
	
	if ($stData)
	{
	    chomp($line);
	    push(@ret, $line);
	}
    }

    if ($emptyDir)
    {
	print "  Found no files to download.\n";
    }

    close(EXPSESSION);

    return @ret;
}

sub DownloadFile
{
    my($localRoot, $theFile) = @_;
    my($ret);
    my($filesDir);
    my($fullRdCmd);
    my($cmdRet);
    my($error) = 0;

    if($theFile =~ /(.+)\/[^\/]+$/)
    {
	$filesDir = $localRoot . $1;
    }
    
    # make local directory, if necessary
    if (!(-e $filesDir))
    {
	system("mkdir -p $filesDir");
    }

    if (!(-e $filesDir))
    {
	print "Couldn't make local directory: $filesDir\n";
	$ret = "couldn't download";
	$error = 1;
    }
    
    if (!$error)
    {
	$fullRdCmd = $RDCMD . $theFile . " " . $localRoot . $theFile;
	print "Downloading $theFile: ";
	$cmdRet = system($fullRdCmd . " >& /dev/null");

	#print $fullRdCmd . "\n";

	if ($cmdRet != 0)
	{
	    $ret = "couldn't download";
	    $error = 1;
	    print "BAD\n"
	}
	else
	{
	    $ret = "downloaded";
	    print "OK\n";
	}
    }

    return $ret;
}

# Loads LATEST status.  If there is more than one entry for a file, the last one
# is the one that counts.
sub ReadStatusFile
{
    my($statusFilePath) = @_;

    my($line);

    # read
    open(STATUSFILE, "< $statusFilePath") || "Couldn't access output file $statusFilePath.\n";

    while ($line = <STATUSFILE>)
    {
	chomp($line);

	if ($line =~ /^\#.*/)
	{
	    # strip comments
	    next;
	}

	if (length($line) == 0)
	{
	    # ignore empty lines
	    next;
	}

	if ($line =~ /^\s+$/)
	{
	    # ignore empty lines
	    next;
	}

	if ($line =~ /(.+)\s+(\w+)/)
	{
	    $STATMAP{$1} = $2;
#	    print "Loading STATMAP: STATMAP{\"$1\"} = \"$2\"\n";
	}
	else
	{
	    print "Bad status file, deleting $statusFilePath\n";
	    %STATMAP = undef;
	    system("unlink $statusFilePath");
	    
	    if (-e $statusFilePath)
	    {
		die "Couldn't access status file $statusFilePath\n";
	    }
	    
	    last;
	}
    }
}

# Track which files have been downloaded
# Format:
#    <Full local path>\t<status>
#
#    <state> can be:
#       "notdownloaded" - an attempt to download was made, but failed in some way.
#       "downloaded"    - successful download
#    
#    Subsequent scripts should use this same file and update the <state> as follows:
#       "notingested"   - an attempt to ingest the file into DRMS failed in some way.
#       "ingested"      - successful ingestion.
#       "cleaned"       - successfully ingested and deleted from download location 
#                         (only in DRMS at this point)
# 
# This status file should be updated up periodically and the downloaded file
# should be deleted iff <state> == "ingested".

sub WriteStatusFile
{
    my($statusFilePath) = $_[0];
    my(@dlFiles) = @_[1..@_ - 1];

    my($date);

    # write out date first
    open(DATECMD, "date |") || die "Couldn't run date\n";
    $date = <DATECMD>;
    chomp($date);

    # append
    open(STATUSFILE, ">>$statusFilePath") || die "Couldn't access status file $statusFilePath.\n";

    print STATUSFILE "# Files downloaded on $date:\n";

    my($aFile);
    my($status);

    while ($aFile = shift(@dlFiles))
    {
	$status = shift(@dlFiles);
        print STATUSFILE $aFile . "\t" . $status . "\n";
    }
}

