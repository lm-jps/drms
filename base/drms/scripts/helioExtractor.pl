# !/usr/bin/perl -w 

# helioExtractor.pl parses the Predicted Orbital State Vectors (Heliocentric) FDS data project 
# (described in section 3.3.11 of "Solar Dynamics Observatory (SDO) 
# Interface Control Document (ICD) Between The Mission Operations Center (MOC) /
# Flight Dynamics System (FDS) And The SDO Ground System 464-GS-ICD-0068 Revision A

# For each record, it extracts the observation time, and the location and vecolicity
# vectors and stores them in DRMS.

# The usage is:
#    helioExtractor.pl <filename>
#    where <filename> is the name of the file containing the state-vector data


# DRMS series in which to place the data files
my($series) = "su_arta.TestFDSHelio";

# Primary key
my(@primaryKey);
$primaryKey[0] = "OBS_DATE";

# Other keys
my(@otherKey);
$otherKey[0] = "X";
$otherKey[1] = "Y";
$otherKey[2] = "Z";
$otherKey[3] = "Vx";
$otherKey[4] = "Vy";
$otherKey[5] = "Vz";

# ordDate->jsocTime hash
my(%ordDateHash);

my($inputfile);
my($argc) = scalar(@ARGV);
if ($argc != 1)
{
    PrintUsage();
    exit(1);
}
else
{
    $inputFile = $ARGV[0];
}

open(HELIOFILE, $inputFile) || die "Couldn't open input file $inputFile\n";

my($numRecsAdded) = 0;

my($line);
my($endOfHeader) = -1;

while (defined($line =<HELIOFILE>))
{
    chomp($line);
    
    if ($endOfHeader == -1)
    {
	if ($line =~ /\s*Time\s*\(YYYYDDD\)\s+X\s*\(km\)\s+Y*\s*\(km\)\s+Z\s*\(km\)\s+Vx\s*\(km\/sec\)\s+Vy\s*\(km\/sec\)\s+Vz\s*\(km\/sec\)\s*/)
	{
	    # end of header is next line
	    $endOfHeader = 1;
	    next;
	}
    }
    else
    {

	if ($endOfHeader == 1)
	{
	    $endOfHeader--;
	    next;
	}
	else
	{
	    # data line
	    my($time);
	    my(@pos);
	    my(@vel);

	    my($ordDate);
	    my($hours);
	    my($minutes);

	    my($skCmd);

	    if ($line =~ /\s*(\d\d\d\d\d\d\d\.\d+)\s+(-?\d+\.\d+)\s+(-?\d+\.\d+)\s+(-?\d+\.\d+)\s+(-?\d+\.\d+)\s+(-?\d+\.\d+)\s+(-?\d+\.\d+)\s*/)
	    {
		# valid data
		$time = $1;
		$pos[0] = $2;
		$pos[1] = $3;
		$pos[2] = $4;
		$vel[0] = $5;
		$vel[1] = $6;
		$vel[2] = $7;

		if ($time =~ /(\d\d\d\d)(\d\d\d)\.(\d\d)(\d\d)/)
		{
		    my($jsocTime);

		    $ordDate = $1 . "." . $2;
		    $hours = $3;
		    $minutes = $4;

		    # call time_convert to put the time into a DRMS format
		    $jsocTime = $ordDateHash{$ordDate};

		    if (!defined($jsocTime))
		    {
			my($convTime);
			my($tcCmdLine) = "time_convert ord=$ordDate" . "_UT o=cal zone=UT |";
			
			open (TIMECONV, $tcCmdLine) || die "Couldn't run time_conv: $tcCmdLine\n";
			
			if (defined($convTime = <TIMECONV>))
			{			
			    chomp($convTime);
			    $jsocTime = $convTime;
			    $ordDateHash{$ordDate} = $jsocTime;
			}
		    }

		    if (defined($jsocTime))
		    {
			# Add hours/minutes to $jsocTime
			if ($jsocTime =~ /(.+_)00:00/)
			{
			    $jsocTime = $1 . $hours . ":" . $minutes . "_UT";
			    $numRecsAdded++;
			    print(<STDOUT>, "Adding record for <$jsocTime, $pos[0], $pos[1], $pos[2], $vel[0], $vel[1], $vel[2]>\n");
			    $skCmd = "set_keys -c ds=$series $primaryKey[0]=$jsocTime $otherKey[0]=$pos[0] $otherKey[1]=$pos[1] $otherKey[2]=$pos[2] $otherKey[3]=$vel[0] $otherKey[4]=$vel[1] $otherKey[5]=$vel[2]";
			    system($skCmd) == 0 || die "Error calling set_keys: $?\n";
			}
		    }
		}
	    }
	}
    }
}

print(<STDOUT>, "Added $numRecsAdded records to $series.\n");

close(HELIOFILE);

exit(0);

sub PrintUsage
{
    print(<STDOUT>, "Usage:\n");
    print(<STDOUT>, "\thelioExtractor.pl <filename>\n");
}
