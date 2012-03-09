#!/home/jsoc/bin/linux_x86_64/perl5.12.2 -w 

# This script prints series publication / subscription information. By default, a list of published series
# is generated, but if the caller specifies the "-subscriptions" flag, then the list of series
# to which NetDRMS institutions are subscribed is generated. The caller can control which institutions for
# which output is printed with the "-institutions" option. By default, output is json, but the "-nojson"
# flag can be used to generated non-json, formatted-text output.
#
# All arguments are options can be abbreviated as long as the abbreviation uniquely identifies
# the parameter/option.
#
# Cmd-line parameters:
#   config (required) - The server-side configuration file containing publication parameters.
# Cmd-line options:
#   -subscriptions (no value) - If this flag is specified, then instead of publication information,
#                               subscription information will be printed, organized by institution.
#   -institutions (mandatory value) - A comma-separated list of DRMS sites for which subscription information 
#                                     is requested. If this option is not specified, then information for 
#                                     all sites is generated. This option is ignored if the "-subscriptions" 
#                                     flag is not specified.
#   -nojson (no value) - If this flag is specified, then output is not json, but formatted text. Otherwise, 
#                        the output is json.

use DBI;
use DBD::Pg;
use FindBin qw($Bin);
use lib "$Bin/..";
use toolbox qw(GetCfg GetArgs);

use constant kArgConfig => 'config';
use constant kOptSubs => 'subscriptions';
use constant kOptInsts => 'institutions';
use constant kOptNoJSON => 'nojson';

# return status codes
use constant kSuccess => 0;
use constant kInvalidArg => 1;

my($argsin);
my($optsin);
my($argsH); # Hash of cmd-line arguments, keyed by argument name
my($optsH); # Hash of cmd-line options, keyed by option name
my($conf);
my($dosub);
my($instsA);
my($dojson);
my(%cfg);
my(@publist);
my($lstH);


# Arguments specification
$argsin = 
{
 &kArgConfig => 's'
};

# Options specification
$optsin =
{
 &kOptSubs => 'noval',
 &kOptInsts => 's',
 &kOptNoJSON => 'noval'
}

$argsH = {};
$optsH = {};

# Get required cmd-line arguments.
unless (toolbox::GetArgs($argsin, $argsH))
{
   Usage();
   exit(kInvalidArg);
}

# Get options cmd-line options
unless (toolbox::GetOpts($optsin, $optsH))
{
   Usage();
   exit(kInvalidArg);
}

# Collect arguments
$conf = $argsH->{&kArgConfig};
$dosub = defined($optsH->{&kOptSubs});
if (defined($dosub))
{
   $instsA = ParseInstsList($optsH->{&kOptInsts});
}
$dojson = !defined($optsH->{&kOptNoJSON});

# Read server configuration file
if (-e $conf)
{
   # fetch needed parameter values from configuration file
   if (toolbox::GetCfg($conf, \%cfg))
   {
      print "Unable to read configuration file $conf\n";
      $rv = kInvalidArg;
   }
} 
else
{
   print "Configuration file $conf does not exist.\n";
   $rv = kInvalidArg;
}


# Get list of published series (requires connecting to hmidb2)
@publist = GetPubList($cfg{'SLAVEHOST'}, $cfg{'SLAVEPORT'}, $cfg{'SLAVEDBNAME'});

# Get .lst files (map node --> series)
$lstH = GetLstFiles($cfg{'tables_dir'});

sub ParseInstsList
{
   my($list) = $_[0];

   my($rv);

   if (defined($lists))
   {
      $rv = [];
      @$rv = split(/,/, $lists);
   }

   return $rv;
}

sub GetPubList
{
   my($dbhost) = $_[0];
   my($dbport) = $_[1];
   my($dbname) = $_[2];

   my(@rv);
   my($dbuser);
   my($dsn);
   my($dbh);

   $dbuser = getlogin();

   $dsn = "dbi:Pg:dbname=$dbname;host=$dbhost;port=$dbport";
   $dbh = DBI->connect($dsn, $dbuser, '');

   if (defined($dbh))
   {

   }

   return @rv;
}

sub GetLstFiles
{
   my($rv);

   $rv = {};

   return $rv;
}

sub Usage
{
   print "Usage: publist.pl config=<server configuration file> [ -subscriptions ] [ -institutions ] [ -nojson ]\n";
}
