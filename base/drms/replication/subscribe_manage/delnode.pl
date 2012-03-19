#!/home/jsoc/bin/linux_x86_64/perl5.12.2

use strict;
use warnings;

use FindBin qw($Bin);
use File::Find qw(finddepth);
use File::Copy;
use File::Basename;
use lib "$Bin";
use lib "$Bin/../../../libs/perl";
use lib "$Bin/..";
use toolbox qw(GetCfg);
use subtablemgr;
use drmsLocks;
use drmsArgs;

use constant kArgConfig => "config";
use constant kArgNode   => "node";

use constant kLockFile => "gentables.txt";

# Return values
use constant kSuccess     => 0;
use constant kInvalidArg  => 1;
use constant kLock        => 2;
use constant kFileIO      => 3;
use constant kAbort       => 4;
use constant kTblMgr      => 5;
use constant kUnknownNode => 6;

my($argsinH);
my($args);
my($conf);
my($node);
my(%cfg);
my($instlck);
my($subscribelockpath);
my($tblmgr);
my($sitedir);
my($sitedirbak);
my($ans);
my($tmret);
my($rv);

$rv = &kSuccess;

# Get arguments
$argsinH = 
{
    &kArgConfig =>    's',
    &kArgNode   =>    's'    
};

$args = new drmsArgs($argsinH, 1);
if (!defined($args))
{
    $rv = &kInvalidArg;
}
else
{
    $conf = $args->Get(&kArgConfig);
    $node = $args->Get(&kArgNode);

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
}

if ($rv == &kSuccess)
{
    # The lock will lock the cfg and lst tables and files (legacy).
    $tblmgr = new SubTableMgr($cfg{kServerLockDir} . "/" . &kLockFile, $cfg{kCfgTable}, $cfg{kLstTable}, $cfg{MASTERDBNAME}, $cfg{MASTERHOST}, $cfg{MASTERPORT}, $cfg{REPUSER});

    $tmret = $tblmgr->GetErr();

    if ($tmret ==  &SubTableMgr::kRetLock)
    {
        print STDERR "Unable to acquire table-manager lock.\n";
        $rv = &kLock;
    }
    elsif ($tblmgr->GetErr() != &SubTableMgr::kRetSuccess)
    {
        print STDERR "There was a problem initializing the table manager.\b";
        $rv = &kTbleMgr;
    }
    else
    {
        # Remove node's site-specific logs and containing directory.
        # Get node's site-dir from cfg table. This could fail (for example, if the caller is attempting to get the site dir
        # for a node that doesn't exist), in which case $sitedir == "".
        $sitedir = $tblmgr->GetSiteDir($node);

        if (!defined($sitedir) || length($sitedir) <= 0)
        {
            print STDERR "Uknown node $node.\n";
            $rv = &kUnknownNode;
        }
        else
        {
            # Do a rm -rf on $sitedir (extremely dangerous - prompt).
            print "Time to remove all site-specific logs. Please confirm - shall I delete $sitedir? (y/n)\n";
            $ans = <STDIN>;
            chomp($ans);

            if ($ans =~ /^\s*y/i)
            {
                my($filename);
                my($path);

                # Acquire subscription lock (will modify site-specific slony log files).
                $subscribelockpath = "$cfg{kServerLockDir}/subscribelock.txt";
                system("(set -o noclobber; echo $$ > $subscribelockpath) 2> /dev/null");
                
                if ($? == 0)
                {
                    $SIG{INT} = "ReleaseLock";
                    $SIG{TERM} = "ReleaseLock";
                    $SIG{HUP} = "ReleaseLock";
             
                    # Critical region
                    # $sitedir = "/tmp/arta/testsitedir"; # ART
                    ($filename, $path) = fileparse($sitedir);
                    $sitedirbak = "$path/\.$filename\.tmp";
                    move($sitedir, $sitedirbak);
                    
                    if (-e $sitedir)
                    {
                        print STDERR "Failure moving folder $sitedir to $sitedirbak.\n";
                        $rv = &kFileIO;
                    }
                    
                    # End critical region       
                    unlink "$subscribelockpath";
                    
                    $SIG{INT} = 'DEFAULT';
                    $SIG{TERM} = 'DEFAULT';
                    $SIG{HUP} = 'DEFAULT';
                }
                else
                {
                    print STDERR "Unable to acquire subscription lock.\n";
                    $rv = &kLock;
                }
            }
            else
            {
                print "User canceled removal of folder $sitedir; aborting.\n";
                $rv = &kAbort;
            }
        }

        if ($rv == &kSuccess)
        {
            # Remove node's cfg-table record, the node's lst-table records, the node's cfg-file entry(ies), and the node's lst file.
            $tblmgr->Remove($node, $cfg{parser_config});
            if ($tblmgr->GetErr() != &SubTableMgr::kRetSuccess)
            {
                print STDERR "Unable to successfully remove the cfg- and lst-table entries for node $node.\n";
                $rv = &kTblMgr;
            }
        }

        # If everything looks good, commit, else rollback.
        if ($rv == &kSuccess)
        {
            # Disconnects from db too.
            RmDir($sitedirbak);
            $tblmgr->Commit();
        }
        else
        {
            # Disconnects from db too.
            move($sitedirbak, $sitedir); # restore original sitedir.
            $tblmgr->Rollback();
        }
    }
}

exit($rv);

sub Zapper
{
    if (!-l && -d _)
    {
        rmdir($File::Find::name);
    }
    else
    {
        unlink($File::Find::name);
    }
}

sub RmDir
{
    my($dir) = shift;

    # Depth-first search, so we can call rmdir on the current directory (because we will have already called 
    # unlink on all files in the directory.)
    finddepth(\&Zapper, $dir);
}


