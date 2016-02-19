#!/home/jsoc/bin/linux_x86_64/activeperl

# This script creates the SQL dump of the series tables to which a node is subscribing. It ensures that there are no duplicate data
# that exist in both the dump file and the slony logs. After performing the dump, it adds $node.new.lst to slony_parser.cfg and
# to su_production.slonycfg. It has to navigate a critical region shared by other scripts.


# Run like this:

# config=/home/jsoc/cvs/Development/JSOC/proj/replication/etc/repserver.cfg client=cora sublist=hmi.v_avg120 idlist=89 newcl=1 filectr=/solarport/pgsql/slon_logs/live/triggers/slon_counter.cora.txt

use warnings;
use strict;
use Data::Dumper;

use FindBin qw($Bin);
use lib "$Bin/../../../libs/perl";
use lib "$Bin/..";
use lib $Bin; # Location of subtablemgr.
use toolbox qw(GetCfg);
use subtablemgr;
use DBI;
use DBD::Pg;
use drmsArgs;
use drmsRunProg;

# Arguments
use constant kArgConfig      => "config";
use constant kArgClient      => "client";
use constant kArgSublist     => "sublist";
use constant kArgIdlist      => "idlist";
use constant kArgNewClient   => "newcl";
use constant kArgFileCtr     => "filectr";

# Return values
use constant kRetSuccess           => 0;
use constant kRetInvalidArgs       => 1;
use constant kRetParseSlonyLogs    => 2;
use constant kRetDbQuery           => 3;
use constant kRetLock              => 4;
use constant kRetFileIO            => 5;
use constant kRetSubTable          => 6;

# Other
use constant kLockFile             => "dumprep.txt"; # Not really needed, a higher-level lock will suffice, but
                                                     # SubTableMgr constructor requires a lock file.

my($argsinH);
my($args);
my($config);
my($client);
my($sublist);
my($idlist);
my($newclient);
my($filectr);
my(%cfg);
my($clnsp);
my($dbname);
my($dbhost);
my($dbport);
my($dbuser);
my($dsn);
my($dbh);
my(@subtables);
my($idsH);
my($stmnt);
my($cmd);
my($rrows);
my($fields);
my($subscribelockpath);
my($newlst);
my($fh);
my($rv);

$rv = &kRetSuccess;

$argsinH =
{
    &kArgConfig    => 's',
    &kArgClient    => 's',
    &kArgSublist   => 's',
    &kArgIdlist    => 's',
    &kArgNewClient => 's',
    &kArgFileCtr   => 's'
};


$args = new drmsArgs($argsinH, 1);
if (!defined($args))
{
    print STDERR "One or more invalid arguments.\n";
    $rv = &kRetInvalidArgs;
}
else
{
    # fetch needed parameter values from configuration file
    $config = $args->Get(&kArgConfig);
    if (toolbox::GetCfg($config, \%cfg))
    {
        $rv = &kRetInvalidArgs;
        print STDERR "Cannot open configuration file '$config'.\n";
    }
    else
    {
        my(@tmpA);
        my(@cptables);
        my($tabname);
        my($tabid);
        
        $client = $args->Get(&kArgClient);
        $clnsp = "_" . $cfg{CLUSTERNAME};
        $dbname = $cfg{SLAVEDBNAME};
        $dbhost = $cfg{SLAVEHOST};
        $dbport = $cfg{SLAVEPORT};
        $sublist = $args->Get(&kArgSublist);
        $idlist = $args->Get(&kArgIdlist);        
        $newclient = $args->Get(&kArgNewClient);
        $filectr = $args->Get(&kArgFileCtr);
        $dbuser = $ENV{USER};
        @subtables = split(/,/, $sublist);
        @tmpA = split(/,/, $idlist);
        $idsH = {};

        @cptables = @subtables;
        while (1)
        {
            $tabname = shift(@cptables);
            $tabid = shift(@tmpA);
            
            if (!defined($tabname) || !defined($tabid))
            {
                if (defined($tabname) || defined($tabid))
                {
                    print STDERR "The number of elements in " . &kArgSublist . " and " . &kArgIdlist . " do not match.\n";
                    $rv = &kRetInvalidArgs;
                }
                
                last;
            }
            
            $idsH->{$tabname} = $tabid;
        }
        
        if ($rv == &kRetSuccess)
        {
            $dsn = "dbi:Pg:dbname=$dbname;host=$dbhost;port=$dbport;";
            $dbh = DBI->connect($dsn, $dbuser, '', {AutoCommit => 0}); # will need to put pass in .pg_pass
        }

        if (defined($dbh))
        {
            # Use the serializable isolation level so that we have a consistent view of the 
            # database throughout the transaction.
            $stmnt = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;";
            
            $rv = ExeStmnt($dbh, $stmnt, 1, "Starting db transaction: $stmnt\n");
            if ($rv == &kRetSuccess)
            {
                # Lock each table that is being subscribed to. We do not want the creation of slony logs that
                # modify any of these tables. We want to perform a dump such that no changes are made, otherwise
                # the slony logs generated during the dump could contain insert statements for rows that the
                # dump already has. 
                
                # The dump will freeze the view of series tables at the time the copy command is issued, not at the 
                # time the transaction was started. So, by locking now, the dump will contain all inserts into series
                # tables that happened between the start of the transaction and the issuance of the LOCK statements.
                foreach my $table (@subtables)
                {
                    # LOCK <table> IN SHARE MODE
                    $stmnt = "LOCK $table IN SHARE MODE;";
                    $rv = ExeStmnt($dbh, $stmnt, 1, "Locking a table: $stmnt\n");
                    if ($rv != &kRetSuccess)
                    {
                        last;
                    }
                }
            }
            
            if ($rv == &kRetSuccess)
            {
                # Run the slony-log parser. This will ensure that there are no slony logs that contain insert statements
                # for any of the tables being subscribed to. If we don't do this, then after we finish the dump, the log-parser
                # could process a slony log with insert statement involving subscribed-to series. However, those inserts will
                # be for records that are already in the dump.
                #
                # ENSURE THAT THE log-parser LOCK IS NOT BEING HELD!! Otherwise, deadlock will ensue.
                $cmd = $cfg{kJSOCRoot} . "/base/drms/replication/parselogs/parse_slon_logs.pl $config parselock.txt subscribelock.txt > /usr/local/pgsql/replication/live/log/cron.parse_slony_logs.log 2>&1";
                
                if (drmsSysRun::RunCmd($cmd) != 0)
                {
                    # Error calling parse_slon_logs.pl.
                    print STDERR "Unable to run parse_slon_logs.pl";
                    $rv = &kRetParseSlonyLogs;
                }
            }

            # This might be useless. The slony logs will continue to be generated during a subscription, so 
            # I'm not sure what use it is to dump the tracking number.
            if ($rv == &kRetSuccess)
            {
                $stmnt = "CREATE TEMP TABLE subscriber_slon_counter AS SELECT ac_num FROM $clnsp.sl_archive_counter;";
                $stmnt = $stmnt . "COPY subscriber_slon_counter TO '$filectr';";
                $rv = ExeStmnt($dbh, $stmnt, 1, "Dumping the current log number to $filectr: $stmnt\n");   
            }
                
            if ($rv == &kRetSuccess)
            {
                if ($newclient)
                {
                    if ($rv == &kRetSuccess)
                    {
                        # ----
                        # Fill the setsync tracking table with the current status
                        # ----
                        # Returns rows.
                        $stmnt = "SELECT 'INSERT INTO $clnsp.sl_archive_tracking values (' || ac_num::text || ', ''' || ac_timestamp::text || ''', CURRENT_TIMESTAMP);' FROM $clnsp.sl_archive_counter;";
                        $rrows = $dbh->selectall_arrayref($stmnt, undef);
                        
                        if (NoErr($rrows, \$dbh, $stmnt))
                        {
                            my(@rows) = @$rrows;
                            
                            foreach my $row (@rows)
                            {
                                # Print each row to stdout (there should be only one row).
                                print "$row->[0]\n";
                            }
                        }
                        else
                        {
                            $rv = &kRetDbQuery;
                        }
                    }
                
                    # Do the COPY command after the INSERT INTO command so that all COPY commands are at the end of the SQL dump file.
                    # This way the client can split the SQL dump file and execute the non-COPY commands first in one transaction, 
                    # and then do the potentially large COPY commands in a second transaction. If the first set of commands succeeds,
                    # and the second set fails, then we end up with an empty series on the client host. An error message will
                    # indicate this, and the client can then re-subscribe to try again.
                    if ($rv == &kRetSuccess)
                    {
                        # ----
                        # Fill the sl_sequence_offline table and provide initial 
                        # values for all sequences.
                        # ----
                    
                        # This goes into the .sql file. Everything that goes into the .sql file is sent to stdout.
                        print "COPY $clnsp.sl_sequence_offline FROM stdin;\n";
                        # This will provide the values that go into the $clnsp.sl_sequence_offline table at the client site. This command
                        # returns rows, unlike previous commands. Must call selectall_arrayref().
                        $stmnt = "SELECT seq_id::text || '\t' || seq_relname  || '\t' || seq_nspname FROM $clnsp.sl_sequence;";
                        $rrows = $dbh->selectall_arrayref($stmnt, undef);
                    
                        if (NoErr($rrows, \$dbh, $stmnt))
                        {
                            my(@rows) = @$rrows;
                        
                            foreach my $row (@rows)
                            {
                                # Print each row to stdout.
                                print "$row->[0]\n";
                            }
                        
                            # Send an EOF to the COPY command.
                            print "\\\.\n";
                        }
                        else
                        {
                            $rv = &kRetDbQuery;
                        }
                    }
                } # newsite
            }

            if ($rv == &kRetSuccess)
            {
                # ----
                # Now dump all the user table data
                # ----
                foreach my $table (@subtables)
                {                        
                    # For each replicated-table ID in $tables (the set of tables being subscribed to), dump the table.
                    # Returns rows.
                    # Get fieldnames...These are just all column names in the table being subscribed to (like "recnum","sunum","slotnum", ...).
                    $stmnt = "SELECT $clnsp.copyfields(" . $idsH->{$table} . ");";
                    
                    $rrows = $dbh->selectall_arrayref($stmnt, undef);
                    if (NoErr($rrows, \$dbh, $stmnt))
                    {
                        my(@rows) = @$rrows;
                        if ($#rows == 0)
                        {
                            # Must be only one row.
                            $fields = join(',', @{$rows[0]});
                        }
                        else
                        {
                            $rv = &kRetDbQuery;
                        }
                    }
                    else
                    {
                        $rv = &kRetDbQuery;
                    }
                    
                    if ($rv == &kRetSuccess)
                    {
                        my($cpbuf);
                        
                        # Do the actual dump - I think this will create a copy statement in stdout like:
                        #   COPY hmi.m_45s ("recnum","sunum","slotnum", ...) FROM stdin;
                        #   1       27804102        0     ...
                        #   2       27804102        1     ...
                        #   3       27804102        2     ...
                        print "COPY $table $fields FROM stdin;\n";
                        $stmnt = "COPY $table $fields TO stdout;"; # Acts like a print statement - goes to stdout. Does not return rows.
                        
                        $rv = ExeStmnt($dbh, $stmnt, 1, "Dumping table $table: $stmnt\n");
                        
                        # Need to do more to get the COPY data out of DBD::Pg.
                        $cpbuf = "";
                        while ($dbh->pg_getcopydata(\$cpbuf) >= 0)
                        {
                            print "$cpbuf";
                        }

                        # Send an EOF to the COPY command.
                        print "\\\.\n";
                    }
                } # dump each table
            }
            
            if ($rv == &kRetSuccess)
            {
                #-------------------------------------------------------------------------
                # Adding the new entry to the slon log parser config file
                #-------------------------------------------------------------------------
                
                # Must lock parser so we can modify slony_parser.cfg. We can release parser lock
                # before we know if the subscription succeeded or not, because the parser will actually operate on two different
                # lst files for this series: the original lst file (the parser will continue to write logs based on this lst file
                # into the $client site directory), and the new lst file (the parser will write logs based upon this lst file
                # into the $client.new site directory). If the subscription is bad, then we just delete the files in $client.new. Otherwise,
                # we overwrite the ones in $client with the ones in $client.new.
                
                ###########################
                ## Edit slony_parser.cfg
                ###########################
                $subscribelockpath = $cfg{kServerLockDir} . "/subscribelock.txt";
                system("(set -o noclobber; echo $$ > $subscribelockpath) 2> /dev/null");
                
                if ($? == 0)
                {
                    my($tblmgr);

                    $SIG{INT} = "DropLock";
                    $SIG{TERM} = "DropLock";
                    $SIG{HUP} = "DropLock";
                    
                    # Critical region
                    $newlst = $cfg{tables_dir} . "/$client.new.lst";
                    if (defined(open($fh, ">>" . $cfg{parser_config})))
                    {
                        print $fh $cfg{subscribers_dir} . "/$client.new        $newlst";
                        $fh->close();

                        # ART LST - Add $node.new.lst to su_production.slonycfg.
                        $tblmgr = new SubTableMgr($cfg{'kServerLockDir'} . "/" . &kLockFile, $cfg{'kCfgTable'}, $cfg{'kLstTable'}, $cfg{'tables_dir'}, $cfg{'MASTERDBNAME'}, $cfg{'MASTERHOST'}, $cfg{'MASTERPORT'}, $cfg{'REPUSER'});

                        unless (&SubTableMgr::kRetSuccess == ($tblmgr->GetErr()))
                        {
                            $rv = &kRetSubTable;
                        }
                        else
                        {
                            # Must not print to stdout! All stdout gets redirected to the sql dump file.
                            $tblmgr->Silent();
                            $tblmgr->Add("$client.new", $cfg{subscribers_dir} . "/$client.new"); # Don't provide lst-file argument.
                                                                                           # This argument is used for
                                                                                           # populating su_production.slonylst
                                                                                           # which is something we don't want to
                                                                                           # do. That table gets populated 
                                                                                           # in subscription_update. In fact, at
                                                                                           # this point, subscription_update will
                                                                                           # have already updated this db table.

                            unless (&SubTableMgr::kRetSuccess == ($tblmgr->GetErr()))
                            {
                                $rv = &kRetSubTable;
                            }
                        }
                    }
                    else
                    {
                        print STDERR "Unable to open " . $cfg{parser_config} . " for appending to.\n";
                        $rv = &kRetFileIO;
                    }
                    # End critical region
                    
                    # OK TO RELEASE PARSE LOCK here. No inserts to the series can happen just yet (the series tables are locked).
                    # release subscribe lock
                    unlink "$subscribelockpath";
                    
                    $SIG{INT} = 'DEFAULT';
                    $SIG{TERM} = 'DEFAULT';
                    $SIG{HUP} = 'DEFAULT';
                }
                else
                {
                    print STDERR "[dumpreptables.pl] Unable to acquire subscription lock.\n";
                    $rv = &kRetLock;
                }    
            }
            
            ($rv == &kRetSuccess) ? $dbh->commit() : $dbh->rollback();
        } # defined($dbh)
        else
        {
            print STDERR "Unable to connect to database using '$dsn'.\n";
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

sub DropLock
{
    unlink "$subscribelockpath";
    exit(1);
}
