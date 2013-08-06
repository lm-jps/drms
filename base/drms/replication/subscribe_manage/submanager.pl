#!/home/jsoc/bin/linux_x86_64/activeperl -w

# Returns 0 on success, or non-zero error code otherwise.

use FindBin qw($Bin);
use lib "$Bin/..";
use toolbox qw(GetCfg);
use FileHandle;
use DBI;
use DBD::Pg;
use Data::Dumper;

use constant kArgConfig => "config";
use constant kArgOp => "op";
use constant kArgNode => "node";
use constant kArgAllSeries => "allseries";


# return codes
use constant kSuccess => 0;
use constant kInvalidArg => 1;

my($argsin);
my($argsH);
my($conf);
my($op);
my($node);
my($lstfile);

# Arguments specification
$argsin = 
{
 &kArgConfig => 's',
 &kArgOp => 's',
 &kArgNode => 's',
 &kArgAllSeries => 's'
};

$argsH = {};

# Get required cmd-line arguments. This function ensures that all these arguments are
# present, or it bombs out.
unless (toolbox::GetArgs($argsin, $argsH))
{
   exit(kInvalidArg);
}

# Collect arguments.
$conf = $argsH->{&kArgConfig};
$op = $argsH->{&kArgOp};
$node = $argsH->{&kArgNode};
$lstfile = $argsH->{&kArgAllSeries};

# Switch on op
if ($op eq "updatelst")
{
   
}
else 
{
   print STDERR "Invalid operation $op.\n";
   exit(kInvalidArg);
}
