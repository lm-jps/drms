#!/home/jsoc/bin/linux_x86_64/perl5.12.2

# Use this script to generate the tables that hold information needed by the log parser
# to generate site-specific logs.

# /home/arta/jsoctrees/JSOC/base/drms/replication/subscribe_manage/gentables.pl conf=/home/arta/jsoctrees/JSOC/proj/replication/etc/repserver.cfg --node=artatest op=replace --lst=/c/pgsql/slon_logs/live/etc/artatest.lst
# /home/arta/jsoctrees/JSOC/base/drms/replication/subscribe_manage/gentables.pl conf=/home/arta/jsoctrees/JSOC/proj/replication/etc/repserver.cfg --node=artatest op=add --sitedir=/solarport/pgsql/slon_logs/live/site_logs/artatest --lst=/c/pgsql/slon_logs/live/etc/artatest.lst
# /home/arta/jsoctrees/JSOC/base/drms/replication/subscribe_manage/gentables.pl conf=/home/arta/jsoctrees/JSOC/proj/replication/etc/repserver.cfg op=create


use strict;
use warnings;

use FindBin qw($Bin);
use lib "$Bin/..";
use lib "$Bin";
use toolbox qw(GetCfg);
use subtablemgr;

use constant kLockFile => "gentables.txt";

# Arguments
use constant kArgConfig     => "config";   # Server configuration file
use constant kArgOp         => "op";       # Operation to perform
use constant kOptNode       => "node";     # Operating on this node's records 
use constant kOptSDataFile  => "lst";      # File containing series data
use constant kOptSiteDir    => "sitedir";  # Directory containing site-specific slony log files
use constant kOptPop        => "populate"; # Populate both tables from data files

# Op values
use constant kOpValRep  => qr(replace)i;
use constant kOpValAdd  => qr(add)i;
use constant kOpValCrt  => qr(create)i;
use constant kOpValPop  => qr(populate)i;

# Return codes
use constant kSuccess     => 0;
use constant kInvalidArg  => 1;
use constant kCantLock    => 2;
use constant kDBConn      => 3;
use constant kBadQuery    => 4;
use constant kTable       => 5;
use constant kCfgTable    => 6;
use constant kLstTable    => 7;

my($rv);
my($argsin);
my($optsin);
my($argsH);
my($optsH);
my($conf);
my($op);
my($tblmgr);
my($node);
my($lstfile);
my($sitedir);
my($dopop); 
my(%cfg);

# Required arguments
$argsin = 
{
    &kArgConfig =>    's',
    &kArgOp     =>    's'
};

# Optional arguments, although some are required depending on which operation is being performed.
$optsin =
{
    &kOptNode      => 's',
    &kOptSDataFile => 's',
    &kOptSiteDir   => 's',
    &kOptPop       => 'noval'
};

$argsH = {};
$optsH = {};

unless (toolbox::GetArgs($argsin, $argsH))
{
    Usage();
    exit(kInvalidArg);
}

unless (toolbox::GetOpts($optsin, $optsH))
{
    Usage();
    exit(kInvalidArg);
}

# Collect arguments/options
$conf = $argsH->{&kArgConfig};
$op = $argsH->{&kArgOp};

# Read server configuration file.
if (defined($conf) && -e $conf)
{
    # fetch needed parameter values from configuration file                                               
    if (toolbox::GetCfg($conf, \%cfg))
    {
        print "Unable to read configuration file $conf\n";
        $rv = &kInvalidArg;
    }
}
else
{
    print "Configuration file $conf does not exist.\n";
    $rv = &kInvalidArg;
}

if (!$rv)
{
    # Create a new SubTableMgr object. A lock will be acquired to 
    # prevent multiple instances.
    $tblmgr = new SubTableMgr($cfg{kServerLockDir} . "/" . &kLockFile, $cfg{kCfgTable}, $cfg{kLstTable}, $cfg{'MASTERDBNAME'}, $cfg{'MASTERHOST'}, $cfg{'MASTERPORT'}, $cfg{'REPUSER'});
    
    $rv = GetScrptErr($tblmgr->GetErr());
}

if (!$rv)
{
    if ($op =~ &kOpValRep)
    {
        # Replace the node's existing lst table records with the data in
        # the provided lst file.
        $node = $optsH->{&kOptNode};
        $lstfile = $optsH->{&kOptSDataFile};
        
        if (!defined($node) || !defined($lstfile))
        {
            Usage();
            exit(kInvalidArg);
        }
        
        $rv = $tblmgr->Replace($node, $lstfile);
    }
    elsif ($op =~ &kOpValAdd)
    {
        $node = $optsH->{&kOptNode};
        $sitedir = $optsH->{&kOptSiteDir};
        $lstfile = $optsH->{&kOptSDataFile};
        
        if (!defined($node))
        {
            Usage();
            exit(kInvalidArg);
        }
        
        if (!defined($sitedir))
        {
            # Assume site dir is node name if no sitedir name provided.
            $sitedir = $node;
        }
        
        $tblmgr->Add($node, $sitedir, $lstfile);
    }
    elsif ($op =~ &kOpValCrt)
    {
        $dopop = $optsH->{&kOptPop};
        
        if (!defined($dopop))
        {
            $dopop = 0;
        }
        
        if ($dopop)
        {
            $tblmgr->Create($cfg{parser_config});
        }
        else
        {
            $tblmgr->Create();
        }
    }
    elsif ($op =~ &kOpValPop)
    {
        $tblmgr->Populate($cfg{parser_config});
    }
    else
    {
        print "Invalid operation $op.\n";
        Usage();
        exit(kInvalidArg);
    }
    
    $rv = GetScrptErr($tblmgr->GetErr());
}

exit $rv;

sub GetScrptErr
{
    my($mgrerr) = @_;
    my($screrr);
    
    if ($mgrerr == &SubTableMgr::kRetSuccess)
    {
        $screrr = &kSuccess;
    }
    elsif ($mgrerr == &SubTableMgr::kRetLock)
    {
        $screrr = &kCantLock;
    }
    elsif ($mgrerr == &SubTableMgr::kRetInvalidArg)
    {
        $screrr = &kInvalidArg;
    }
    elsif ($mgrerr == &SubTableMgr::kRetDBConn)
    {
        $screrr = &kDBConn;
    }
    elsif ($mgrerr == &SubTableMgr::kRetBadQuery)
    {
        $screrr = &kBadQuery;
    }
    elsif ($mgrerr == &SubTableMgr::kRetIO)
    {
        $screrr = &kIO;
    }
    elsif ($mgrerr == &SubTableMgr::kRetTable ||
        $mgrerr == &SubTableMgr::kRetTableRepl ||
        $mgrerr == &SubTableMgr::kRetTableAdd ||
        $mgrerr == &SubTableMgr::kRetTableCreate)
    {
        $screrr = &kTable;
    }
    elsif ($mgrerr == &SubTableMgr::kRetAlreadyExists)
    {
        $screrr = &kInvalidArg;
    }
    else
    {
        print STDERR "Undefined error code $mgrerr.\n";
    }
    
    return $screrr;
}

sub Usage
{
    # REPLACE existing rows in the lsts table with data from lstfile.
    print "Usage:\n";
    print "gentables.pl config=<server config file> op=replace --node=<node> --lstfile=<file with series list>\n";
    print "REPLACE existing rows in the lst table with data from lstfile.\n\n";
    
    # ADD a new node to the configuration table.
    print "gentables.pl config=<server config file> op=add --node=<node> --sitedir=<site dir> [ --lstfile=<file with series list> ]\n";
    print "ADD a new node to the configuration table. Optionally populate the node's lst-table records with the series list in <file with series list>.\n\n";
    
    # CREATE both configuration and lsts tables. Optionally populate both tables from data in the
    #   configuration and lst FILES.
    print "gentables.pl config=<server config file> op=create [ --populate ]\n";
    print "CREATE both configuration and lsts tables. Optionally populate both tables \nfrom data in the configuration and lst FILES.\n\n";
    
    # POPULATE both configuration and lsts tables from data in the configuration and lst FILES.
    print "gentables.pl config=<server config file> op=populate\n";
    print "POPULATE both configuration and lsts tables from data in the configuration and lst files.\n";
}
