#!/home/jsoc/bin/linux_x86_64/perl5.12.2

use strict;
use warnings;
use FindBin qw($Bin);
use lib "$Bin/../../libs/perl";
use drmsLocks;
use drmsArgs;
use drmsRunProg;

# Return values
use constant kRetSuccess => 0;
use constant kRetInvalidArgs => 1;

# Required cmd-line arguments
use constant kArgOp => "op";

# Optional cmd-line arguments
use constant kOptSname => "series";
use constant kOptProc => "proc";
use constant kOptPath => "path";
use constant kOptReq => "req";
use constant kOptOpt => "opt";
use constant kOptMap => "map";
use constant kOptOut => "out";

# kArgOp values
use constant kArgValCrt => qr(create)i;
use constant kArgValDrp => qr(drop)i;
use constant kArgValAdd => qr(add)i;
use constant kArgValDel => qr(delete)i;

# Tmp path
use constant kTmpDir => "/tmp";

my($rv);
my($args);
my($opts);
my($argsinH);
my($optsinH);
my($op);

# Required arguments
$argsinH = 
{
    &kArgOp =>    's',
};

# Optional arguments, although some are required depending on which operation is being performed.
$optsinH =
{
    &kOptSname    => 's',
    &kOptProc     => 's',
    &kOptPath     => 's',
    &kOptReq      => 's',
    &kOptOpt      => 's',
    &kOptMap      => 's',
    &kOptOut      => 's'
};

$args = new drmsArgs($argsinH, 1);
$opts = new drmsArgs($optsinH, 0);

$rv = &kRetSuccess;

if (!defined($args) || !defined($opts))
{
    $rv = &kRetInvalidArgs;
}
else
{
    # Collect cmd-line args.
    $op = $args->Get(&kArgOp);
    if (!defined($op))
    {
        $rv = &kRetInvalidArgs;
    }
    
    if ($rv == &kRetSuccess)
    {
        if ($op =~ &kArgValCrt)
        {
            my($sname);
            
            # Get optional series name.
            $sname = $opts->Get(&kOptSname);
            $rv = CreateProcSeries($sname);
        }
        elsif ($op =~ &kArgValDrp)
        {
            
        }
        elsif ($op =~ &kArgValAdd)
        {
            
        }
        elsif ($op =~ &kArgValDel)
        {
            
        }
        else
        {
            print STDERR "Invalid op $op\n";
            $rv = &kRetInvalidArgs;
        }
    }
}

exit($rv);

sub CreateProcSeries
{
    my($sname) = shift; # optional
    my($rv);
    my(@jsd);
    my($jsdstr);
    my($pipe);
    my($rsp);
    
    my($rfh);
    
    $rv = 0;
    
    @jsd = <DATA>;
    $jsdstr = join("", @jsd);
    
    if (defined($sname))
    {
        $jsdstr =~ s/jsoc\.export_procs/$sname/g;
    }
    
    if (length($jsdstr) > 0)
    {
        $pipe = new drmsPipeRun("create_series -i");
        
        if (defined($pipe))
        {
            $pipe->EnableAutoflush();
            $pipe->WritePipe($jsdstr);
            $pipe->ClosePipe(1); # close write pipe
            $pipe->ReadPipe(\$rsp);
            
            print "create_series output: \n$rsp\n";
            
            # close both read and write pipes (but write was already closed)
            if ($pipe->ClosePipe())
            {
                print STDERR "Failure writing to pipe.\n";
                $rv = 1;
            }
        }
        else
        {
            print STDERR "Unable to call create_series.\n";
            $rv = 1;
        }
    }
    else
    {
        print STDERR "Empty JSD.\n";
        $rv = 1;
    }
    
    return $rv;
}

sub Usage
{
    print "manageproctbl.pl op=create --series=<series name>\n";
}

__DATA__
Seriesname:     jsoc.export_procs
Author:         "Art Amezcua"
Owner:          arta
Unitsize:       32
Archive:        0
Retention:      2
Tapegroup:      1
PrimeKeys:      proc
DBIndex:        proc
Description:    "This dataseries contains one record per available export processing step."

Keyword:        proc, string, variable, record, "BadProc", "%s", "NA", "Unique name of an export processing step."
Keyword:        path, string, variable, record, "BadPath", "%s", "NA", "Path to export processing-step program."
Keyword:        required, string, variable, record, "", "%s", "NA", "Comma-separated list of required program arguments."
Keyword:        options, string, variable, record, "", "%s", "NA", "Comma-separated list of optional program arguments."
Keyword:        map, string, variable, record, "", "%s", "NA", "Comma-separated list of key:value pairs. key is the name of a program argument, and value has the form <name>(<type>)."
Keyword:        out, string, variable, record, "", "%s", "NA", "The name of the series into which program inserts records. If the name starts with _, then the output series is formed by appending this name to the input series. Otherwise, the output series is this name."
