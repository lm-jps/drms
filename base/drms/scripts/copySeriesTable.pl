#!/home/jsoc/bin/linux_x86_64/activeperl
#
# copySeriesTable.pl dbname=jsoc dbhost=hmidb dbport=5432 dbuser=postgres dfile=recList.txt newtable=test_table oldtable=hmi.cosmic_rays -suffix=copied

use strict;
use warnings;
use Data::Dumper;
use DBI;
use DBD::Pg;
use File::Copy;
use FindBin qw($Bin);
use lib "$Bin/../../../base/libs/perl";
use drmsArgs;

use constant kRetSuccess     => 0;
use constant kRetInvalidArgs => 1;
use constant kRetDbQuery     => 2;

use constant kArgDbname      => "dbname";
use constant kArgDbhost      => "dbhost";
use constant kArgDbport      => "dbport";
use constant kArgDbuser      => "dbuser";
use constant kArgDfile       => "dfile";
use constant kArgNewtable    => "newtable";
use constant kArgOldtable    => "oldtable";
use constant kArgSuffix      => "suffix"; # The suffix to add to processed input files.

use constant kNumArgs        => 32;


my($args);
my($argsinH);
my($opts);
my($optsinH);
my($dbname);
my($dbhost);
my($dbport);
my($dbuser);
my($dfile);
my($newtable);
my($oldtable);
my($suffix);
my($fh);
my(@lines);
my($dsn);
my($dbh);
my($sth);
my($ns);
my($rel);
my($stmnt);
my($rrows);
my($rv);

$rv = &kRetSuccess;

$argsinH =
{
    &kArgDbname   => 's',
    &kArgDbhost   => 's',
    &kArgDbport   => 'i',
    &kArgDbuser   => 's',
    &kArgDfile    => 's',
    &kArgNewtable => 's',
    &kArgOldtable => 's'
};

$optsinH =
{
    &kArgSuffix   => 's'
};

$args = new drmsArgs($argsinH, 1);
$opts = new drmsArgs($optsinH, 0);

if (!defined($args) || !defined($opts))
{
    $rv = &kRetInvalidArgs;
}
else
{
    $dbname = $args->Get(&kArgDbname);
    $dbhost = $args->Get(&kArgDbhost);
    $dbport = $args->Get(&kArgDbport);
    $dbuser = $args->Get(&kArgDbuser);
    $dfile = $args->Get(&kArgDfile);
    $newtable = $args->Get(&kArgNewtable);
    $oldtable = $args->Get(&kArgOldtable);
    $suffix = $opts->Get(&kArgSuffix);

    if (defined($newtable))
    {
        $newtable = lc($newtable);
    }

    if (defined($oldtable))
    {
        $oldtable = lc($oldtable);
    }
    
    if (defined(open($fh, "<$dfile")))
    {
        @lines = <$fh>;
        $fh->close();
        
        # Make a prepared statement that will be used repeatedly to delete records from the series table.
        # Connect to the database. Hard-code the essential information (not my best script in the world).
        $dsn = "dbi:Pg:dbname=$dbname;host=$dbhost;port=$dbport";
        $dbh = DBI->connect($dsn, $dbuser, '',  { AutoCommit => 0 }); # will need to put pass in .pg_pass
        
        if (defined($dbh))
        {
            # Test for the existence of $newtable. Don't attempt to create it if it already exists.
            if ($newtable =~ /(\S+)\.(\S+)/)
            {
                $ns = $1;
                $rel = $2;
            }
            else
            {
                $ns = "public";
                $rel = $newtable;
            }
            
            $stmnt = "SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = '$ns' AND c.relname = '$rel'";
            
            $rrows = $dbh->selectall_arrayref($stmnt, undef);
            if (!NoErr($rrows, \$dbh, $stmnt))
            {
                $rv = &kRetDbQuery;
            }
            else
            {
                my(@rows) = @$rrows;
                if (@rows)
                {
                    # At least one row - $newtable exists.
                }
                else
                {
                    # The output table does not exist. Create it.
                    $stmnt = "CREATE TABLE $newtable(LIKE $oldtable INCLUDING INDEXES)";
                    $rv = ExeStmnt($dbh, $stmnt, 1, "Creating new table: $stmnt.\n");                    
                }
            }
            
            if ($rv == &kRetSuccess)
            {
                $sth = $dbh->prepare("INSERT INTO $newtable SELECT * FROM $oldtable WHERE recnum in (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            }
            
            if (defined($sth))
            {
                my($recnum);
                my($reclist);
                my($reclistStr);
                my($iExe);
                my($nExe);
                
                $reclist = ();
                $iExe = 0;
                foreach my $line (@lines)
                {
                    chomp($line);
                    if ($line =~ /\s*(\d+)\s*/)
                    {
                        $recnum = $1;
                    }
                    else
                    {
                        # This is an invalid line, skip, but report.
                        print STDERR "Invalid recnum: $recnum; skipping.\n";
                    }
                    
                    push(@$reclist, $recnum);
                    
                    if (($iExe + 1) % &kNumArgs == 0)
                    {
                        $reclistStr = join(', ', @$reclist);
                        print "Executing statement " . $sth->{Statement} . "; args ==> $reclistStr.\n";
                        if (!defined($sth->execute(@$reclist)))
                        {
                            # error - rollback
                            $rv = &kRetDbQuery;
                            last;
                        }
                        
                        $reclist = ();
                    }
                    
                    $iExe++;
                }
                
                # There might be some left-over recnums not yet processed.
                if (@$reclist)
                {
                    $reclistStr = join(', ', @$reclist);
                    $stmnt = "INSERT INTO $newtable SELECT * FROM $oldtable WHERE recnum in ($reclistStr)";
                    print "Executing statement $stmnt.\n";
                    $rv = ExeStmnt($dbh, $stmnt, 1, "Copying left-over rows: $stmnt.\n");
                }
            }
            else
            {
                print STDERR "Failed to create prepared statement.\n";
                $rv = &kRetDbQuery;
            }
            
            if ($rv == &kRetSuccess)
            {
                # commit
                $dbh->commit();

                if (defined($suffix) && length($suffix) > 0)
                {
                    my($newFileName) = "$dfile\.$suffix";

                    move($dfile, $newFileName);
                }
            }
            else
            {
                # rollback
                $dbh->rollback();
            }
            
            $dbh->disconnect();
        }
    }
}

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
        if (!NoErr($res, \$dbh, $stmnt))
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
