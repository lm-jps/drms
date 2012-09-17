#!/home/jsoc/bin/linux_x86_64/perl5.12.2 

# This script takes two arguments:
#   series - the DRMS dataseries to which we want to add keywords.
#   spec - 

use warnings;
use strict;

use FindBin qw($Bin);
use Cwd qw(realpath);
use Net::SSH::Perl;
use lib ("$Bin/../../libs/perl", "$Bin/../base/libs/perl");
use lib ("$Bin/../replication", "$Bin/../base/drms/replication");
use drmsLocks;
use drmsArgs;
use drmsRunProg;
use toolbox qw(GetCfg);

# Return values
use constant kRetSuccess     => 0;
use constant kRetInvalidArgs => 1;
use constant kRetModuleCall  => 2;
use constant kRetPipeWrite   => 3;
use constant kRetConfig      => 4;
use constant kRetAddKey      => 5;
use constant kRetSlonik      => 6;
use constant kRetFileIO      => 7;

# Parameters
use constant kArgSeries  => "series";
use constant kArgKwSpec  => "spec";
use constant kArgConfig  => "config";

# SQL file - must be a path that is accessible from hmidb2. Put it in the caller's homedir.
use constant kSqlFile     => ".addcols.sql";

$| = 1;

my($rv);
my($argsinH);
my($optsinH);
my($args);
my($opts);
my($series);
my($spec);
my($config);
my(%cfg);

my($keystoadd);
my($nkeys);
my($pipe);
my($rsp);
my(@rspA);
my(@sql);
my(@keys);
my($keystr);
my($sqlstr);
my($sqlfile);
my($isrepl) = 0;

# Fetch arguments.
# Required arguments
$argsinH = 
{
    &kArgSeries  =>    's',
    &kArgKwSpec  =>    's',
    &kArgConfig  =>    's'
};

# Optional arguments.
$optsinH =
{

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
    $series = $args->Get(&kArgSeries);
    $spec = $args->Get(&kArgKwSpec);
    $config = $args->Get(&kArgConfig);
}

if ($rv == &kRetSuccess)
{
    if (-e $config)
    {
        # fetch needed parameter values from configuration file
        if (toolbox::GetCfg($config, \%cfg))
        {
            print STDERR "Error cannot open and parse configuration file $config.\n";
            $rv = &kRetConfig;
        }
    } 
    else
    {
        print STDERR "Configuration file $config cannot be found.\n";
        $rv = &kRetConfig;
    }
}

if ($rv == &kRetSuccess)
{
    my(@keys);
    
    # Parse out the keyword names that we're adding (so we can check later to ensure that they have been added).
    # Ideally, we'd have DRMS do this, but since there isn't a good API to do this from Perl, we're just
    # going to have to parse with Perl.
    foreach (split(/\n/, $spec)) 
    {
        $_ =~ /keyword:(\w+)/i || next;
        if (!defined($keystoadd))
        {
            $keystoadd = {};
        }
        
        $keystoadd->{$1} = 0;
    }
    
    @keys = keys(%$keystoadd);
    $nkeys = $#keys + 1;
    if ($nkeys < 1)
    {
        print STDERR "Invalid spec argument .\n";
        $rv = &kRetInvalidArgs;
    }
}

if ($rv == &kRetSuccess)
{
    # Determine if the series to be modified in under Slony replication. If so, then we have to add columns
    # to the table via slonik. The slonik command will contain SQL to not only add columns to 
    # the replicated table, but it will also contain SQL to insert record(s) into the ns.drms_keyword table.
    # If the series it NOT under replication, then we can use a DRMS module to modify both the table and the ns.drms_keyword table.
    
    # Call a DRMS module to parse the keyword specification. If the series is not a member of a Slony replication-set, 
    # then the keywords will be added immediately by the DRMS module. Otherwise, the module will 
    # return SQL that needs to go into the slonik SQL command. This SQL will both add columns to the series table, 
    # and it will add rows to the ns.drms_keyword table. This script will place the SQL in the slonik command. The slonik
    # runs asynchronously, so this script has to poll waiting for the slonik to complete before it can terminate.
    
    # Open a read pipe to drms_addkeys.
    print "Attempting to add $nkeys columns to series $series.\n";
    $pipe = new drmsPipeRun("drms_addkeys series=$series spec='$spec'", 0);
    
    if (defined($pipe))
    {
        $pipe->ReadPipe(\$rsp);
        # The drms_addkeys module returns 3 when the series is under replication. stderr will then have the SQL needed to add the columns
        # to the series table and the <ns>.drms_keyword table.
        
        # close the read pipe
        if ($pipe->ClosePipe(0))
        {
            print STDERR "Failure reading from pipe.\n";
            $rv = &kRetPipeWrite;
        }
        else
        {
            $isrepl = ($pipe->GetStatus(0) == 3);            
        }
    }
    else
    {
        print STDERR "Unable to call drms_addkeys.\n";
        $rv = &kRetModuleCall;
    }
}

if ($rv == &kRetSuccess)
{
    @rspA = split(/\n/, $rsp);
    
    if ($isrepl)
    {
        # We were not able to add keywords to the series. Instead, we have to let Slony do this.
        # This will be an asynchronous process. Must poll waiting for the database to propagate the
        # column adds / row inserts.
        
        # Extract the SQL from the $rsp.
        @sql = @rspA[3..$#rspA];
        $sqlstr = join('', map({"$_;\n"} @sql));
        # print "$sqlstr\n";
        print "Cannot use DRMS to add columns to a slony-replicated series.\n";
        print "Attempting to use Slony to add the columns.\n";
        
        # Create and execute slonik () script. The slonik script has to run on hmidb2.
        unless (($rv = ExeSlonik(\%cfg, \$sqlstr, \$sqlfile)))
        {
            print "Slonik cmd succeeded. Waiting for propagation of column addition to slave db.\n";
        }
        else
        {
            print STDERR "Slonik cmd failed. Columns not added.\n";
        }
    }
}

# Ensure that keywords got added to series in master database.
if ($rv == &kRetSuccess)
{
    print "Checking series $series for successful addition of columns.\n";
    if ($isrepl)
    {
        # Must poll, looking for successful addition of added columns. The slonik cmd will 
        # operate asynchronously.
        while(1)
        {
            print ". ";
            if (NewKeysExist($keystoadd))
            {
                print "\nSuccess!\n";
                unlink();
                last;
            }
            
            sleep(2);
        }
    }
    else
    {
        # No need to poll. drms_addkeys operates synchronously.
        if (NewKeysExist($keystoadd))
        {
            print "Success!\n";
        }
        else
        {
            print "Failure!\n";
        }
    }
}

sub ExeSlonik
{
    my($cfg) = $_[0];
    my($sql) = $_[1];
    my($sqlfile) = $_[2];
    my($slonikcmd);
    my($ssh);
    my($stdout);
    my($stderr);
    my($exitcode);
    my($rv) = &kRetSuccess;
    
    $$sqlfile = $ENV{HOME} . "/" . &kSqlFile;
    if (open(SQLFILE, ">$$sqlfile"))
    {
        # Write SQL to SQL file.
        print SQLFILE $$sql;
        close(SQLFILE);
        
        # Create slonik cmd.
        $slonikcmd = "cluster name = $cfg->{CLUSTERNAME};\n" .
        "node 1 admin conninfo = 'dbname=$cfg->{MASTERDBNAME} host=$cfg->{MASTERHOST} port=$cfg->{MASTERPORT} user=$cfg->{REPUSER}';\n" .
        "node 2 admin conninfo = 'dbname=$cfg->{SLAVEDBNAME} host=$cfg->{SLAVEHOST} port=$cfg->{SLAVEPORT} user=$cfg->{REPUSER}';\n\n" .
        "EXECUTE SCRIPT (SET ID = 1, FILENAME = '$$sqlfile', EVENT NODE = 1);";
        
        print "Executing slonik ==>\n$slonikcmd\n";
        
        # Log-in to hmidb2.
        print "Logging into " . $cfg->{kModOnSlaveHost} . "\n";
        $ssh = Net::SSH::Perl->new($cfg->{kModOnSlaveHost}, protocol => '2,1', interactive => 1);
        $ssh->login("postgres");
        
        print "Logged-in!\n";
        
        # Run slonik cmd on hmidb2.
        ($stdout, $stderr, $exitcode) = $ssh->cmd("echo \"$slonikcmd\" | $cfg->{kSlonikCmd}"); # un-remove
        # $slonikcmd = "publish.hmi.M_720s_nrt.log"; # remove
        # ($stdout, $stderr, $exitcode) = $ssh->cmd("cat $slonikcmd | grep CODEVER1"); # remove
        # print "rsp is $stdout\n"; # remove

        if ($exitcode != 0)
        {
            $rv = &kRetSlonik;
            print STDERR "Encountered an error when running slonik cmd.\n";
            print STDERR "STDERR is $stderr.\n";
        }
    }
    else
    {
        $rv = &kRetFileIO;
        print STDERR "Unable to open sql file $$sqlfile for writing.\n";
    }
    
    return $rv;
}

# NewKeysExist is quick and dirty, and inefficient. It runs show_info. This function is called in a loop, so
# it will continually start/stop a process and create/destroy a db connection and transaction. 
# It would be more efficient to query the db directly for the keywords.
sub NewKeysExist
{
    my($keystoadd) = $_[0];
    my($istat) = $_[1];
    
    my($pipe);
    my($jsd);
    my(@jsdA);
    my(@keys);
    my($rv) = &kRetSuccess;
    my($ans) = 0;
    
    # We should make sure that drms_addkeys successfully added the keys. I guess we can use show_info -j to
    # do that.
    $pipe = new drmsPipeRun("show_info -j $series JSOC_DBHOST=hmidb2", 0);
    
    if (defined($pipe))
    {
        $pipe->ReadPipe(\$jsd);
        
        # close both read and write pipes (but write was already closed)
        if ($pipe->ClosePipe(0))
        {
            print STDERR "Failure reading from pipe.\n";
            $rv = &kRetPipeWrite;
        }
        else
        {
            # Parse the $rsp, looking for the keywords added. drms_addkeys should return the 
            # keyword names, so extract those names from drms_addkeys output, and then look for
            # them in the response.
            my($akey);
            
            @jsdA = split(/\n/, $jsd);
            foreach my $line (@jsdA)
            {
                $line =~ /keyword:(\w+)/i || next;
                $akey = $1;
                
                if (exists($keystoadd->{$akey}))
                {
                    $keystoadd->{$akey} = 1;
                }
            }
        }
        
        # Ensure all keys got added.
        @keys = keys(%$keystoadd);
        foreach (@keys)
        {
            if ($keystoadd->{$_} == 0)
            {
                $rv = &kRetAddKey;
                last;
            }
        }
    }
    else
    {
        print STDERR "Unable to call show_info.\n";
        $rv = &kRetModuleCall;
    }
    
    $$istat = $rv;
    
    return $ans;
}

__DATA__
