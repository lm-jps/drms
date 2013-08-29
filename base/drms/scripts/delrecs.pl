#!/home/jsoc/bin/linux_x86_64/activeperl

# Delete DRMS records from the DRMS database without touching retention time, or 
# otherwise communicating with SUMS.

# Run like:
#   delrecs.pl spec=su_arta.test[2013.2.4][! 1=1 !] dbname=jsoc dbhost=hmidb dbport=5432 

use warnings;
use strict;
use Data::Dumper;

use FindBin qw($Bin);
use lib "$Bin/../../libs/perl";
use DBI;
use DBD::Pg;
use drmsArgs;
use drmsRunProg;

# Arguments
use constant kArgRecSetSpec      => "spec";
use constant kArgDbname          => "dbname";
use constant kArgDbhost          => "dbhost";
use constant kArgDbport          => "dbport";

# Return values
use constant kRetSuccess         => 0;
use constant kRetInvalidArgs     => 1;
use constant kRetShowInfo        => 2;
use constant kRetPipeRead        => 3;

use constant kDBSuperUser        => "postgres"; # Only db user postgres can delete records.
use constant kChunkSize          => 128;

my($argsinH);
my($args);
my($rv);

$rv = &kRetSuccess;

$argsinH =
{
    &kArgRecSetSpec              => 's',
    &kArgDbname                  => 's',
    &kArgDbhost                  => 's',
    &kArgDbport                  => 'i'
};

$args = new drmsArgs($argsinH, 1);
if (!defined($args))
{
    print STDERR "One or more invalid arguments.\n";
    $rv = &kRetInvalidArgs;
}
else
{
    my($spec);
    my($series);
    my($cmd);
    my($rsp);
    my($stmnt);
    my($pipe);
    my($nbytes);
    my(@recs);
    my($reclist);
    my($nitems);
    my($dbname);
    my($dbhost);
    my($dbport);
    my($dbuser);
    my($dsn);
    my($dbh);

    $spec = $args->Get(&kArgRecSetSpec);
    $dbname = $args->Get(&kArgDbname);
    $dbhost = $args->Get(&kArgDbhost);
    $dbport = $args->Get(&kArgDbport);
    $dbuser = &kDBSuperUser;
    
    # Call show_info -e to parse the record-set specification into constituent parts.
    $cmd = "show_info -eq '$spec'";
    $pipe = new drmsPipeRun($cmd, 0);
    
    if (defined($pipe))
    {
        $pipe->ReadPipe(\$rsp);
        
        # close the read pipe
        if ($pipe->ClosePipe())
        {
            print STDERR "Failure reading from pipe.\n";
            $rv = &kRetPipeRead;
        }
        else
        {
            if ($rsp =~ /\s*(\S+)/)
            {
                $series = $1;
            }
            else
            {
                print STDERR "Unexpected response from show_info: $rsp.\n";
                $rv = &kRetShowInfo;
            }
        }
    }
    else
    {
        print STDERR "Unable to call show_info.\n";
        $rv = &kRetShowInfo;
    }

    if ($rv == &kRetSuccess)
    {
        # Call show_info to get the list of records on which to operate. Also fetch the record-set specifications
        # for each individual record. These specifications will be printed to stdout so we know exactly which 
        # records were deleted.
        $cmd = "show_info -qri '$spec'";
        $pipe = new drmsPipeRun($cmd, 0);
        
        if (defined($pipe))
        {
            # Going to be communicating with the database.
            $dsn = "dbi:Pg:dbname=$dbname;host=$dbhost;port=$dbport;";
            $dbh = DBI->connect($dsn, $dbuser, '', {AutoCommit => 0}); # will need to put pass in .pg_pass
            
            if (defined($dbh))
            {
                my($recnum);
                my($rspec);
                
                $nitems = 0;
                while (($nbytes = $pipe->ReadLine(\$rsp)) > 0)
                {
                    # $rsp is a string, and may have funky formatting.
                    if ($rsp =~ /\s*(\S+)\s+(\S+)/)
                    {
                        $recnum = sprintf("%d", $1);
                        $rspec = $2;
                    }
                    else
                    {
                        print STDERR "Unexpected response from show_info: $rsp.\n";
                        $rv = &kRetShowInfo;
                        last;
                    }
                    
                    if ($#recs + 1 >= &kChunkSize)
                    {
                        # Time to delete more records.
                        $reclist = join(',',@recs);
                        $nitems += $#recs + 1;
                        @recs = ();
                        $stmnt = "DELETE FROM $series WHERE recnum IN ($reclist)";

                        $rv = ExeStmnt($dbh, $stmnt, 1, "Deleting records from $series: $stmnt\n");
                        if ($rv != &kRetSuccess)
                        {
                            last;
                        }
                    }
                    
                    # one recnum - add it to the list of recnums
                    push(@recs, $recnum);
                    print "$recnum:$rspec\n";
                }
                
                if ($rv == &kRetSuccess)
                {
                    if ($#recs >= 0)
                    {
                        $reclist = join(',',@recs);
                        $nitems += $#recs + 1;
                        @recs = ();
                        $stmnt = "DELETE FROM $series WHERE recnum IN ($reclist)";
                        
                        $rv = ExeStmnt($dbh, $stmnt, 1, "Deleting records from $series: $stmnt\n");
                    }
                }
                
                if ($rv == &kRetSuccess)
                {
                    $dbh->commit();
                    print "Total number of records deleted: $nitems.\n";                    
                }
                else
                {
                    $dbh->rollback();
                    print "No records deleted, error %rv.\n";
                }
            }
            
            # close the read pipe
            if ($pipe->ClosePipe())
            {
                print STDERR "Failure reading from pipe.\n";
                $rv = &kRetPipeRead;
            }
        }
        else
        {
            print STDERR "Unable to call show_info.\n";
            $rv = &kRetShowInfo;
        }
    }
}

exit($rv);


sub NoErr
{
    my($rv, $dbh, $stmnt) = @_;
    my($ok) = 1;
    
    if (!defined($rv) || !$rv)
    {
        if (defined($$dbh) && defined($$dbh->err))
        {
            print STDERR "Error " . $$dbh->errstr . ": Statement '$stmnt' failed.\n";
        }
        
        $ok = 0;
    }
    
    return $ok;
}

sub ExeStmnt
{
    my($dbh, $stmnt, $doit, $diag) = @_;
    my($rsp);
    my($res);
    my($rv);
    
    $rv = &kRetSuccess;
    
    if ($doit)
    {
        $res = $dbh->do($stmnt);
        if (!NoErr($res, $dbh, $stmnt))
        {
            $rv = &kRetDbQuery;
        }
    }
    else
    {
        print $diag;
    }
    
    return $rv;
}
