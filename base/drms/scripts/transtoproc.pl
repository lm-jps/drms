#!/home/jsoc/bin/linux_x86_64/activeperl

# Must run this on db host (i.e., hmidb or hmidb2). This script needs to ssh to the client machines, many
# of which will be cluster nodes. The cluster nodes are all accessible directly from the db machines. 
# The SQL that collects information about PG clients must be evaluated as a db superuser, otherwise
# the sql will not return useful information.

# The way it works:
# 1. The script first sshs to a database-server machine as user postgres. It is necessary
#    to run this script on a database-server machine because those machines have direct
#    access to all client machines. 
# 2. The script then connects to the requested database instance running on the server, as 
#    db user postgres, a db superuser. The connection must be made by a db superuser to get 
#    useful information. The script then runs a query on the pg_stat_activity table to obtain a list 
#    of all clients currently running a db transaction.
# 3. For each transaction, this list provides a client machine address and client port number. The
#    script then sshs to this client machine, as root, and runs lsof with the client port number as an argument.
#    The user of this script will need to provide the root password for each client machine.
#    lsof will return a pid for the process connected to the port.
# 4. This script will print the machine's address and pid of the process that has started the 
#    transaction.

use strict;
use warnings;

use DBI;
use DBD::Pg;
# ART - This module may not be installed. DO NOT USE CPAN to install modules. Use the ActiveState ppm. Unfortunately, 
# ActiveState does not provide this module.
use Net::SSH::Perl;
use Date::Parse;
use Socket;
use FindBin qw($Bin);
use lib "$Bin/../replication";
use toolbox qw(GetCfg AcquireLock ReleaseLock);

use constant kArgDbname => "dbname";
use constant kArgDbhost => "dbhost";
use constant kArgDbport => "dbport";
use constant kOptWhere  => "where";
use constant kOptLog    => "log";

my($argsin);
my($optsin);
my($argsH);
my($optsH);
my($dbname);
my($dbhost);
my($dbport);
my($where);
my($log);
my($dsn);       # database connection arguments
my($dbh);       # database handle
my($err);

local $| = 1;

$argsin =
{
    &kArgDbname => 's',
    &kArgDbhost => 's',
    &kArgDbport => 'i'
};

$optsin =
{
    &kOptWhere => 's',
    &kOptLog   => 's'
};

$argsH = {};
$optsH = {};

unless (toolbox::GetArgs($argsin, $argsH))
{
    Usage();
    exit(&kInvalidArg);
}

unless (toolbox::GetOpts($optsin, $optsH))
{
    Usage();
    exit(&kInvalidArg);
}

# Collect all arguments
$dbname = $argsH->{&kArgDbname};
$dbhost = $argsH->{&kArgDbhost};
$dbport = $argsH->{&kArgDbport};
$where = $optsH->{&kOptWhere};
$log = $optsH->{&kOptLog};

if (!defined($where))
{
    $where = "";
}

if (!defined($log))
{
    $log = "/dev/null";
}

$dsn = "dbi:Pg:dbname=$dbname;host=$dbhost;port=$dbport";

# Despite ALL documentation saying otherwise, it looks like the error codes/string
# provided by DBI are all UNDEFINED, unless there is some kind of failure. So,
# never try to look at $dbh->err or $dbh->errstr if the call succeeded.                                                                                                         
$dbh = DBI->connect($dsn, "postgres", ''); # will need to put pass in .pg_pass

if (defined($dbh))
{
    my($addr);
    my($port);
    my($xactT);
    my($eltime);
    my($eltimestr);
    my($days);
    my($hours);
    my($mins);
    my($secs);
    my($machinfo);
    my($breaker);
    my($stmnt);
    my($rows);
    my($row);
    my($timev);
    my($name);
    my(@sortedmach);
    my(@group1);
    my(@group2);
    my(@group3);
    
    # Set up log
    if (!open(LOG, ">$log"))
    {
        print STDERR "Unable to open log file '$log' for writing.\n";
        $err = 1;
    }
    else
    {
        my($defout) = select(LOG);
        $| = 1;
        select($defout);
    }
    
    if (!$err)
    {
        $stmnt = "SELECT client_addr,client_port,xact_start FROM pg_stat_activity $where ORDER BY xact_start";
        
        $rows = $dbh->selectall_arrayref($stmnt, undef);
        $err = !(NoErr($rows, \$dbh, $stmnt));
    }
    
    if (!$err)
    {
        foreach $row (@$rows)
        {
            # These addresses are visible on $dbhost, but might not be visible on all hosts.
            $addr = $row->[0];
            $port = $row->[1];
            $xactT = $row->[2];
            
            # Calculate elapsed time
            $eltimestr = "???";
            if (defined($xactT) && length($xactT) > 0)
            {
                $timev = str2time($xactT); 
                
                if (defined($timev))
                {
                    $eltime = int(time() - $timev);
                    if (defined($eltime))
                    {
                        $days = int($eltime / (60 * 60 * 24));
                        $eltime = $eltime % (60 * 60 * 24);
                        $hours = int($eltime / (60 * 60));
                        $eltime = $eltime % (60 * 60);
                        $mins = int($eltime / 60);
                        $secs = $eltime % 60;
                        
                        # Put into a string.
                        $eltimestr = "$days days, $hours hours, $mins mins, $secs secs";
                    }
                }
            }
            
            # Group by machine so we don't have to constantly switch between machines.
            if (!defined($machinfo))
            {
                $machinfo = {};
            }

            $name = gethostbyaddr(inet_aton($addr), AF_INET);

            if (!defined($name))
            {
                $name = $addr;
            }

            if (!exists($machinfo->{$name}))
            {
                # The address might be private, but the db server will have a mapping
                # in /etc/hosts to the actual machine.
                $machinfo->{$name} = {};
            }
            
            if (!exists($machinfo->{$name}->{$port}))
            {
                $machinfo->{$name}->{$port} = {};
            }
            
            $machinfo->{$name}->{$port}->{'eltime'} = $eltimestr;
        }
        
        # Process by each machine
        
        # sort by machine so we can minimize the need for the user to enter passwords.
        @sortedmach = sort(keys(%$machinfo));
        
        foreach my $mach (@sortedmach)
        {
            if ($mach =~ /^\s*10\.0\.1/ || $mach =~ /^\s*cl1n/)
            {
                # cl1nXXX cluster
                PrintToFHs("pushing $mach into group 1.\n", *LOG);
                push(@group1, $mach);
            }
            elsif ($mach =~ /^s*172\.24\.103\.73/ || $mach =~ /^s*172\.24\.103\.76/ ||
                   $mach =~ /^s*hmidb/)
            {
                # Don't know the root password for hmidb or hmidb2.
                PrintToFHs("skipping $mach.\n", *LOG);
            }
            elsif ($mach =~ /^\s*172\.24\.103/ || $mach =~ /^\s*n\d\d/)
            {
                # nXX cluster
                PrintToFHs("pushing $mach into group 2.\n", *LOG);
                push(@group2, $mach);
            }
            elsif ($mach =~ /^\s*10\.100/)
            {
                # Don't do anything. This is probably a lockheed connection, and if we try 
                # to connect to this machine, this script's process will die without allowing a 
                # way to continue.
            }
            else
            {
                # this sucks - just try to convert from the IP address to a machine name, 
                # and use this so the user can at least know what machine the script
                # is attempting to log-in to.
                PrintToFHs("pushing $mach into group 3.\n", *LOG);
                push(@group3, $mach);
            }
        }

        if ($#group1 >= 0)
        {
            # First ssh into a machine that will require the password, then ssh'ing from
            # that machine to the cient machines will not require a password.
            my($ssh) = Net::SSH::Perl->new('j1', protocol => '2,1', interactive => 1);
            
            PrintToFHs("Logging into j1.\n", *STDOUT, *LOG);
            $ssh->login("root");
            
            $breaker = 0;
            map ({ProcessMach($_, $machinfo, \$breaker, $ssh, 0)} @group1);
            $err = ($breaker);
        }
        
        if (!$err && $#group2 >= 0)
        {
            # First ssh into a machine that will require the password, then ssh'ing from
            # that machine to the cient machines will not require a password.
            my($ssh) = Net::SSH::Perl->new('n02', protocol => '2,1', interactive => 1);
            
            $ssh->login("root");
            
            $breaker = 0;
            map ({ProcessMach($_, $machinfo, \$breaker, $ssh, 0)} @group2);
            $err = ($breaker);
        }
        
        if (!$err && $#group3 >= 0)
        {
            $breaker = 0;
            map ({ProcessMach($_, $machinfo, \$breaker, undef, 1)} @group3);
            $err = ($breaker);
        }
        
        if (!$err)
        {
            # Now print results (in original order)
            if (scalar(@$rows) > 0)
            {
                # Print header.
                print "machine\tport\tpid\tppid\telapsedT\tcmd\n";
            }

            map({ProcessRecord($_, $machinfo)} @$rows);
        }
    }
    else
    {
        PrintToFHs("Bad query: $stmnt\n", *STDERR, *LOG);
    }
}
else
{
    PrintToFHs("Unable to connect to db.\n", *STDERR, *LOG);
    $err = 1;
}

exit($err);

sub NoErr
{
    my($rv) = $_[0];
    my($dbh) = $_[1];
    my($stmnt) = $_[2];
    my($msg);
    my($ok) = 1;
    
    if (!defined($rv) || !$rv)
    {
        if (defined($$dbh) && defined($$dbh->err))
        {
            $msg = "Error " . $$dbh->errstr . ": Statement '$stmnt' failed.\n";
            PrintToFHs($msg, *STDERR, *LOG);
        }
        
        $ok = 0;
    }
    
    return $ok;
}

sub ProcessMach
{
    my($mach) = shift;
    my($infoH) = shift;
    my($breaker) = shift;
    my($conn) = shift;
    my($allowskip) = shift;
    my($port);
    
    if ($$breaker == 0)
    {
        # Use SSH-2 protocol - allows multiple commands per connection. If SSH-2 is not available, 
        # then use SSH-1.
        my($ssh);
        my($cmd);
        my($stdout);
        my($stderr);
        my($exitcode);
        my($pid);
        my($abuf);
        
        # Will login as root and prompt for password. root is needed so that process information
        # can be collected.
        
        
        if ($allowskip)
        {
            PrintToFHs("***** Logging into machine $mach.\n", *STDOUT, *LOG);
            print "You must know the root password to collect data from this machine.\n";
            print "Do you want to skip this machine instead? (y/n) ";
            
            # Allow the user to skip this machine, since they might not know the root password.
            $abuf = <STDIN>;        
            print "\n";
            
            if ($abuf =~ /^y/i)
            {
                PrintToFHs("Skipping machine $mach.\n", *STDOUT, *LOG);
                return;
            }
        }
        else
        {
            PrintToFHs("***** Logging into machine $mach.\n", *LOG);
        }
        
        # BEWARE! $ssh->cmd() can call croak(), which will kill the process without providing a way
        # to catch an error and continue!! What is wrong with people?
        
        if (!defined($conn))
        {
            $ssh = Net::SSH::Perl->new($mach, protocol => '2,1', interactive => 1);
            $ssh->login("root");
        }
        
        foreach $port (keys(%{$infoH->{$mach}}))
        {
            # SSH into machines specified by $addr, and then run lsof -i:$port
            $cmd = "lsof -i :$port -F \'p\'";
            PrintToFHs("Running $cmd on $mach.\n", *LOG);
            
            if (defined($conn))
            {
                ($stdout, $stderr, $exitcode) = $conn->cmd("ssh -Y root\@" . $mach . " $cmd");
            }
            else
            {
                ($stdout, $stderr, $exitcode) = $ssh->cmd($cmd);
            }
            
            # Check $exitcode. The lsof cmd could fail for various reason, but the most likely reason
            # would be that by the time the script got this far, the process running the transaction
            # could have ended. In this case, the $exitcode will be 1. Skip all cases where lsof 
            # failed to run properly.
            
            if ($exitcode == 0)
            {
                # $pid is the pid of the client process that is connected to the db
                $pid = ($stdout =~ /^p(\d+)/)[0];
                
                # Run ps cmd to fetch command.
                $cmd = "ps -p $pid -o ppid,command --no-headers";
                PrintToFHs("Running $cmd on $mach.\n", *LOG);
                
                if (defined($conn))
                {
                    ($stdout, $stderr, $exitcode) = $conn->cmd("ssh -Y root\@" . $mach . " $cmd");
                }
                else
                {
                    ($stdout, $stderr, $exitcode) = $ssh->cmd($cmd);
                }
                
                # Similarly, the process could have completed between the lsof comannd and a ps command.
                if ($exitcode == 0)
                {
                    if ($stdout =~ /^\s*(\S+)\s+(.+)$/)
                    {
                        $infoH->{$mach}->{$port}->{'pid'} = $pid;

                        # The PID of the parent of the command that made the db connection.
                        $infoH->{$mach}->{$port}->{'ppid'} = $1;
                        
                        # The command that made the db connection.
                        $infoH->{$mach}->{$port}->{'cmd'} = $2;
                    }
                    else
                    {
                        PrintToFHs("Unexpected ps-command output: $stdout.\n", *STDERR, *LOG);
                        $$breaker = 1;
                    }
                }
                else
                {
                    PrintToFHs("Unable to locate a process with pid $pid, continuing.\n", *LOG);
                }
            }
            else
            {
                PrintToFHs("Unable to locate a process on $mach using port $port, continuing.\n", *LOG);
            }
        }
    }
    
    # I'm guessing that as $ssh exits scope, the ssh connection will be closed.
}

sub ProcessRecord
{
    my($row) = shift;
    my($infoH) = shift;
    my($addr);
    my($port);
    my($pid);
    my($ppid);
    my($eltime);
    my($cmd);
    my($name);
    
    $addr = $row->[0];
    $port = $row->[1];

    $name = gethostbyaddr(inet_aton($addr), AF_INET);

    if (!defined($name))
    {
        $name = $addr;
    }

    $pid = $infoH->{$name}->{$port}->{'pid'};
    $ppid = $infoH->{$name}->{$port}->{'ppid'}; 
    $eltime = $infoH->{$name}->{$port}->{'eltime'};
    $cmd = $infoH->{$name}->{$port}->{'cmd'};

    if (defined($pid))
    {
        print "$name\t$port\t$pid\t$ppid\t$eltime\t$cmd\n";
    }
}

sub PrintToFHs
{
    my($msg) = shift;
    my(@fhs) = @_;
    
    for my $fh (@fhs)
    {
        print $fh $msg;
    }
}
