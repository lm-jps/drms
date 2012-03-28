#!/home/jsoc/bin/linux_x86_64/perl5.12.2

use strict;
use warnings;
use File::Basename;
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

# Proc series keywords
use constant kProcSproc     => "proc";
use constant kProcSpath     => "path";
use constant kProcSrequired => "required";
use constant kProcSoptional => "optional";
use constant kProcSmap      => "map";
use constant kProcSout      => "out";

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
    my($sname);
    
    # Get optional series name.
    $sname = $opts->Get(&kOptSname);
    
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
            $rv = CreateProcSeries($sname);
        }
        elsif ($op =~ &kArgValDrp)
        {
            $rv = DropProcSeries($sname);
        }
        elsif ($op =~ &kArgValAdd)
        {
            my(%items);
            my($proc);
            my($path);
            my($reqargs);
            my($optargs);
            my($namemap);
            my($snameout);
            
            # $reqargs and $optargs contain a comma-separated list of 
            # arguments. Each $reqargs argument has the form arg_i, 
            # where arg_i is the name of an argument. Each $optargs
            # argument has the form arg_i=val_i, where
            # arg_i is the name of an argument, and val_i is the default
            # value of that argument. The default is to be used if the 
            # option's value cannot be found in the processing column
            # of the jsoc.export_new record, the list of jsoc_export_manage
            # C variables, or the list of shell variables available for use.
            $proc = $opts->Get(&kOptProc);
            $path = $opts->Get(&kOptPath);
            $reqargs = $opts->Get(&kOptReq);
            $optargs = $opts->Get(&kOptOpt);
            $namemap = $opts->Get(&kOptMap);
            $snameout = $opts->Get(&kOptOut);
            $rv = AddProc($sname, $proc, $path, $reqargs, $optargs, $namemap, $snameout);
        }
        elsif ($op =~ &kArgValDel)
        {
            # We have no way of doing this with DRMS!
            print STDERR "Deletion of records not supported by DRMS.\n";
            $rv = &kRetInvalidArgs;
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

sub DropProcSeries
{
    my($sname) = shift; # optional
    my($pipe);
    my($rsp);
    my($line);
    my($goahead);
    my($rv);
    
    $rv = 0;
    $goahead = 0;
    
    if (!defined($sname))
    {
        $sname = "jsoc.export_procs";
    }
    
    print "Are you sure you want to drop series $sname?\n";
    $line = <STDIN>;
    chomp($line);
    if ($line =~ /yes/i)
    {
        print "Are you REALLY sure you want to drop series $sname?\n";
        $line = <STDIN>;
        chomp($line);
        if ($line =~ /yes/i)
        {
            $goahead = 1;
        }
    }
    
    if ($goahead)
    {
        $pipe = new drmsPipeRun("delete_series $sname");
        if (defined($pipe))
        {
            $pipe->EnableAutoflush();
            $pipe->WritePipe("yes\nyes\n");
            $pipe->ClosePipe(1); # close write pipe
            $pipe->ReadPipe(\$rsp);
            
            print "delete_series output: \n$rsp\n";
            
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
        print "Drop of series $sname aborted.\n";
    }
    
    return $rv;
}

sub AddProc
{
    my($procseries) = shift;
    my($proc) = shift;
    my($path) = shift;
    my($reqargs) = shift;
    my($optargs) = shift;
    my($namemap) = shift;
    my($snameout) = shift;
    my($rv);
    my($pipe);
    my($rsp);
    my($realargs);
    my($cmd);
    
    $rv = 0;
    
    # $proc - No validation necessary.
    # $path - Could ensure $path exists on a cluster machine, but this might require password, so skip.
    # $reqargs, $optargs - Ensure the program specified by $path takes arguments specified by $reqargs and $optargs.
    # $namemap - Ensure that the LHS program arguments are valid program arguments.
    # $snameout - Validation must be done in jsoc_export_manage.
    
    # Validate program arguments.
    #   1. Collect program arguments.
    if (defined($path) && length($path) > 0)
    {
        if (-e $path)
        {
            $pipe = new drmsPipeRun("$path -H");
        }
        else
        {
            # Try just the filename.
            my($filename) = fileparse($path);
            
            $pipe = new drmsPipeRun("$filename -H");
        }
        
        if (!defined($pipe))
        {
            print STDERR "Invalid path $path.\n";
            $rv = 1;
        }
        else
        {
            my(@content);
            my(@usage);
            my($line);
            my($opts);
            my($arg);
            
            $pipe->ClosePipe(1); # close write pipe
            $pipe->ReadPipe(\$rsp);
            if ($pipe->ClosePipe())
            {
                print STDERR "Failure writing to pipe.\n";
                $rv = 1;
            }
            else
            {
                @content = split(/\n/, $rsp);
                
                # Parse $rsp for all program arguments.
                @usage = grep(/=/, @content);
                
                foreach $line (@usage)
                {
                    if ($line =~ /\[-(.+)\]/)
                    {
                        $opts = $1;
                        $line =~ s/\[-$opts\]//;
                    }
                    
                    while ($line =~ /(\S+)=/)
                    {
                        $arg = $1;
                        if (!defined($realargs))
                        {
                            $realargs = {};
                        }
                        $realargs->{$arg} = 1;
                        $line =~ s/$arg=//;
                    }
                }
                
                # Now add opts to %realargs. $opts will have the form "aDcFPp". Each
                # char is an option.
                while (length($arg = substr($opts, 0, 1)) == 1)
                {
                    if (!defined($realargs))
                    {
                        $realargs = {};
                    }
                    $realargs->{$arg} = 1;
                    $opts = substr($opts, 1);
                }
            }
        } 
    }
    
    # Check all arguments in $reqargs and $optargs - ensure they exist in %realargs.
    if ($rv == 0 && defined($realargs))
    {
        # %realargs has all required and optional variable names. Loop through all
        # argument names in $reqargs and $optargs, ensuring that each is in %realargs.
        
        # Check required arguments.
        if (defined($reqargs))
        {
            $rv = CheckArgs($reqargs, qr((.+)=.+), $realargs);
        }
        
        if ($rv == 0)
        {
            # Check optional arguments.
            if (defined($optargs))
            {
                $rv = CheckArgs($optargs, qr((.+)=.+), $realargs);
            }
        }
        
        if ($rv == 0)
        {
            # Check name map (LHS of ':' has program name).
            if (defined($namemap))
            {
                $rv = CheckArgs($namemap, qr((.+):.+), $realargs);
            }
        }
    }
    
    if ($rv == 0)
    {
        # Add record to table.
        $cmd = "set_info -c ds='$procseries' " . &kProcSproc . "='$proc'";
        
        if (defined($path))
        {
            $cmd = "$cmd " . &kProcSpath . "='$path'";
        }
        
        if (defined($reqargs))
        {
            $cmd = "$cmd " . &kProcSrequired . "='$reqargs'";
        }
        
        if (defined($optargs))
        {
            $cmd = "$cmd " . &kProcSoptional . "='$optargs'";
        }
        
        if (defined($namemap))
        {
            $cmd = "$cmd " . &kProcSmap . "='$namemap'";
        }
        
        if (defined($snameout))
        {
            $cmd = "$cmd " . &kProcSout . "='$snameout'";
        }
        
        print "running cmd $cmd\n";

        $pipe = new drmsPipeRun($cmd);
        if (defined($pipe))
        {
            $pipe->EnableAutoflush();
            $pipe->ClosePipe(1); # close write pipe
            $pipe->ReadPipe(\$rsp);
            if ($pipe->ClosePipe())
            {
                print STDERR "Failure writing to pipe.\n";
                $rv = 1;
            }
            else
            {
                print "set_info output: \n$rsp\n";
            }
        }
    }

    return $rv;
}

sub CheckArgs
{
    my($arglst) = shift;
    my($regexp) = shift;
    my($realargs) = shift;
    my($arg);
    my($lhs);
    my($rv);
    
    $rv = 0;
    
    while ($arglst =~ /^\s*([^,]+)\s*(,(.+))?\s*$/)
    {
        $arg = $1;
        $arglst = $3; # may be undefined
        
        if ($arg =~ $regexp)
        {
            $lhs = $1;
        }
        else
        {
            $lhs = $arg;
        }
        
        if (!exists($realargs->{$lhs}))
        {
            print STDERR "Invalid program argument: $arg.\n";
            $rv = 1;
            last;
        }
        
        if (!defined($arglst))
        {
            last;
        }
    }
    
    return $rv;
}

sub Usage
{
    print "manageproctbl.pl op=create [ --series=<series name> ]\n";
    print "manageproctbl.pl op=drop [ --series=<series name> ]\n";
    print "manageproctbl.pl op=add --proc=<step name> --path=<path to program> --req=<required program args> --opt=<optional program args> --map=<program arg to internal name> --out=<suffix/series out> [ --series=<series name> ]\n";
    print "manageproctbl.pl op=delete --proc=<step name> [ --series=<series name> ]\n"
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
Keyword:        path, string, variable, record, "", "%s", "NA", "Path to export processing-step program."
Keyword:        required, string, variable, record, "", "%s", "NA", "Comma-separated list of required program arguments."
Keyword:        optional, string, variable, record, "", "%s", "NA", "Comma-separated list of optional program arguments."
Keyword:        map, string, variable, record, "", "%s", "NA", "Comma-separated list of key:value pairs. key is the name of a program argument, and value has the form <name>(<type>)."
Keyword:        out, string, variable, record, "", "%s", "NA", "The name of the series into which program inserts records. If the name starts with _, then the output series is formed by appending this name to the input series. Otherwise, the output series is this name."
