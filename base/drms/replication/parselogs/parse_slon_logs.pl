#!/usr/bin/perl

#    26-Mar-2010    |  This script joins parse_slony_logs and prep_slon_logs
#                   |   into a sigle script. I also makes the parsing a lot more
#                   |   efficient by loading the configuration file into a perl hash and
#                   |   opening slony_logs files for the sites only once! and not as the 
#                   |   current logic do which is 
#                   |   (number of sites x number of series on that site) times.
#                   |   It also parses the slony sql only once per site.
#                   |   In addition to this it deals with the full series name.
#                   |   i.e. namespace.series_name    --igor

#    04-Apr-2010    |   I adjusted some security stuff; used glob() where I could
#                   |   to get lists of files, rather than shelling out; changed run()
#                   |   to use "open '-|',@cmd" rather than `$cmd`; changed calls to
#                   |   open to three args: "open $handle, '>', $filename"
#                   |   changed shell calls to mkdir/mv/cp/rm/ls to use the perl equiv.
#                   |   (mkdir / File::Copy (move, copy) / unlink / glob

#    20-Aug-2010    |   Test end of line in sql inserts for \);$ in parseLog subroutine

# Run this:
#  parse_slon_logs.pl /b/devtest/JSOC/proj/replication/etc/repserver.dev.cfg parselock.txt subscribelock.txt


###################################
#### pseudo code documentation ####
###################################


#    -> read argument options (options are: a config file (required) and then  h or H or help for usage and in the event
#                                            of reprocessing the following arguments need to be passed in: repro begin and end )
#
#    -> read repconfig file parse_slon_log.pl parameters
#    -> (config_hash, node_hash) =loadConfig: Loads the contents of kPSLprepCfg file in two perl hash tables
#                kPSLprepCfg has the form of:
#                   /path/to/node/slon_log/directory/node_name  /path/to/node/subscription/list/file/node_name.lst
#                The node_name.lst file has a list of drms series of the form <name_space>.<table_name>. 
#                   i.e.
#                       su_arta.lev0
#                       hmi.doptest
#                       aia.lev1 
#           Return values:
#             .- config_hash: has its keys of the form <name_space>.<table_name.
#                  and the contents for each keys is an array with the nodes that currently subscribe to that series.
#                  See the loadConfig function for more info.
#             .- node_hash: it's keys are the nodes names and the values are anonymous hashes of the form
#                        { 
#                          path=>(site specific slony log directory),
#                          tar_repro=>[array of tar file to be reprocessed] },
#                          filename=>"name to the node temp slon log the parser writes to",
#                          FD => "An open file descriptor to the above filename"
#                        }
#                        Please note that:
#                               .- filename and FD get dynamically set in parseLog function
#                               .- tar_repro only is used in reprocessing.
#                
#
#    -> lock this file for processing. note that there will be one lock for normal processing and another for reprocessing.
#    -> readCounter: takes its counter from the last .sql.parsed slon log file it finds in the slon log directory.
#                     It also saves that counter into a file for missmatch checking. If the counter found in the
#                     slon log directory doesn't match that of the file it will issue a warning. Then it resets the 
#                     counter in the parser counter file.
#
#    -> readSlonCounter: reads the current slonlogready counter.
#
#    -> while ( (slon_log_file = nextCounter()) and (current_processing_counter (less or equal to) slonlogready counter))
#          ## nextCounter() function returns the next slon log file to process
#         -> parseLog()
#              -> openSlonLog()
#                 given a slon_log_file to process it opens the output file descriptor in node_hash for each node.
#
#              -> open slon_log_file
#              -> while reading the slon_log file 
#                  do
#                      parse insert commands and extracts the <name_space>.<table_name> from the line.
#                  -> dumpSlonLogs()
#                      it maches the nodes that subscribe to that series using the config_hash structure and writes 
#                      the sql line to its log file.
#              -> end_while_loop
#              -> closeSlonLogs(). This function closes all the nodes open file descriptors and move the new files 
#                                  from .tmp to its final name.
#         ->  saveCounter() saves the current counter
#         ->  move (slon_log_file to slon_log_file.parsed)
#    -> end_of_while_loop            
#              

use FindBin qw($Bin);
use lib "$Bin/";
use File::Basename;
use Log;
use Data::Dumper;
use Getopt::Long;
use Fcntl ':flock';
use Carp;
use File::Copy;
use File::Basename;
use FileHandle;
use IPC::Open3;

# config-file parser
use FindBin qw($Bin);
use lib "$Bin/..";
use toolbox qw(GetCfg);

my($lockfh);
my($insubcrit);
my($subscribelockpath);
my($anode);

my ($repro,$begin,$end,$repronodes,@repronodelst,%repronodeH);
my $opts = GetOptions ("help|h|H" => \&usage,
                       "repro"    => \$repro,
                       "begin|beg=i"    => \$begin,
                       "end=i"    => \$end,
                       "nodes=s"  => \$repronodes);

my $config_file=$ARGV[0] or die ("No config_file specified");
my $parselock=$ARGV[1] or die ("No parse lock file specified");
my $subscribelock=$ARGV[2] or die ("No subscribe lock file specified");

###################################################################################################################

#### Get CONFIGURATION variables from $config_file
my %config;

if (-e $config_file)
   {
      # fetch needed parameter values from configuration file
      if (toolbox::GetCfg($config_file, \%config))
      {
         die "Error cannot open and parse config file [$config_file].\n";
      }
   } 
   else
   {
      die "Config file [$config_file] does not exist.\n";
   }

# This script must share a lock with managelogs.pl and archivelogs.pl because
# they all modify the logs files, and if the reads and writes aren't synchonized
# then there could be read/write race conditions.
# Must open file handle with write intent to use LOCK_EX


my @missing_config = grep { ! defined($config{$_}) } qw(
     kPSLlogsSourceDir
     kPSLprepCfg
     kPSLprepLog
     kPSLlogReady
     kPSLparseCounter
     kPSLreproPath
     kPSLaccessRepro
     kServerLockDir
);
if ( @missing_config ) {
        die "Missing values from the config file : @missing_config ";
}


## providing some backward compatibility should be removed.
my $complete = {
	'lev0'          => 'su_arta',
	'doptest'       => 'hmi', 
	'vw_v_lev18'    => 'su_arta', 
	'lev1_test4k10s'=> 'lm_jps'
};

################
## MAIN ROUTINE
################

## check lock, only one version of this program should run

## setting log file and targets
logThreshold("info");

map { logTarget($_,$config{'kPSLprepLog'}) } qw(notice info error emergency warning debug);

debug "$_=$config{$_}\n" foreach ( sort keys %config );
debug("Checkpoint A.");

## Normal operation (no reprocessing)
unless ($repro) {
	##
	## for normal processing use the passed in lock file name
  my($lckFH);

  info("locking $config{'kServerLockDir'}/$parselock");

  # parselock facilitates coordination between this process and the processes
  # instantiated via invocations of manage_logs.pl and archivelogs.pl.
  # These three processes are accessing the original log and parsed logs, and
  # can attempt to do so simultaneously.
  thisLock("$config{'kServerLockDir'}/$parselock", \$lckFH);  ## only one instance of this process running

  # $subscribelock facilitates coordination between this process and the processes
  # instantiated via invocations of sql_gen, subscription_cleanup, and sdo_slony1_dump.sh.
  # These four processes access slon_parser.cfg, the nodes' .lst files, and the nodes' site-specific-
  # log directories, and can do so simultaneously.
  $subscribelockpath = "$config{'kServerLockDir'}/$subscribelock";
  system("(set -o noclobber; echo $$ > $subscribelockpath) 2> /dev/null");

  if ($? == 0)
  {
     $SIG{INT} = "ReleaseLock";
     $SIG{TERM} = "ReleaseLock";
     $SIG{HUP} = "ReleaseLock";

     $insubcrit = 1;

     ## normal processing
     debug("load config " . $config{'kPSLprepCfg'});

     ## load configuration in a hash structure
     my ($cfgH, $nodeH) = loadConfig($config{'kPSLprepCfg'});

     my $counter      = readCounter($config{'kPSLparseCounter'});
     my $slon_counter = readSlonCounter($config{'kPSLlogReady'});

     info ("Current counter [$counter]");

     debug("b4 while loop [$counter] [$slon_counter]");
     while ((my $srcFile = nextCounter(\$counter,$config{'kPSLlogsSourceDir'})) && ($counter <= $slon_counter) )
     {
        debug("counter is [$counter]");

        # parseLog will take one source log, and it will parse it N times, once for each node 
        # that is subscribed to a series in that source log. The parsed content will be 
        # written to each node's site-specific log directory.
        if (parseLog($cfgH,$nodeH,$srcFile))
        {
           # ART - If an error occurred in parseLog, should bail here before updating the counter file and 
           # renaming the log file.
           error("Problem parsing a file; bailing out.");
           last;
        }

        $counter = saveCounter($config{'kPSLparseCounter'},$srcFile);

        debug("moving $srcFile to $srcFile.parsed");

        move( $srcFile, "$srcFile.parsed" );
     }

     # release subscribelock.
     # Need to remove this type of lock file, since the other scripts that
     # access it assume the lock is held if the file exists.
     unlink "$subscribelockpath";

     $insubcrit = 0;

     $SIG{INT} = 'DEFAULT';
     $SIG{TERM} = 'DEFAULT';
     $SIG{HUP} = 'DEFAULT';
  }
  else
  {
     print "Warning:: couldn't obtain parse lock; bailing.\n";
     exit(1);
  }

} else { ## user wants reprocessing ###

	##
	## WARNING : using basename($0) *is* better than $0 for the lock, but
	## you risk someone making a copy of the script to test, and running it
	## while the production copy is running -- I'd hard code the lock file.
	##
  my($lckFH);
  my($bail) = 0;

  info("Acquiring single-instance lock.");

  ## ART - Don't use an NFS-mounted file as the lock file!!!! flock was hanging for me on such a file.
  thisLock($config{'kServerLockDir'} . "/" . basename($0) . "repro", \$lckFH);  ## only one instance of reprocessing running
  info("  Lock acquired!");

  # Don't start with junk in the repro directory (a previous failure could leave junk).
  info("Cleaning up repro directory $config{'kPSLreproPath'} before reprocessing.");
  unlink glob("$config{'kPSLreproPath'}/*");

  # Fetch array of nodes for which reprocessing should be performed.
  debug("Regenerating logs for $repronodes.");
  @repronodelst = split(/,/, $repronodes);

  foreach $anode (@repronodelst)
  {
     # skip previously defined nodes; use case-insensitive comparisons
     if (!defined($repronodeH{lc($anode)}))
     {
        info("Re-generating site-specific log files $anode.");
        $repronodeH{lc($anode)} = 1;
     }
  }

  info("load config " . $config{'kPSLprepCfg'});

  ## load configuration in a hash structure
  my ($cfgH, $nodeH);

  if ($#repronodelst >= 0)
  {
     ($cfgH, $nodeH) = loadConfigFilt($config{'kPSLprepCfg'}, \%repronodeH);
  }
  else
  {
     info("Re-generating site-specific log files for all nodes.");
     ($cfgH, $nodeH) = loadConfig($config{'kPSLprepCfg'});
  }

  unless (($begin =~ /\d+/) && ($end =~ /\d+/)) {
     die(usage());
  }

  $nodeH->{'suffix'}="repro"; ## This assumes a repro directory exists on the slony_logs/sites directories. If it doesn't exist it tries to create it and generate all the logs for that site in that location.

  info ("reprocessing range [$begin] - [$end]");
  ## run access repro to download data.

  info("Running accessreplogs to fetch archived slony-log tar files.");
  run( $config{'kPSLaccessRepro'}, 'logs=su_production.slonylogs', "path=$config{'kPSLreproPath'}", "beg=$begin", "end=$end", 'action=ret' );

  ## should do this in another way. The access repro won't have todays data. For that we'll need to copy from the current slon logs directory

  map { my $base = basename($_); copy($_,"$config{'kPSLreproPath'}/$base"); } glob ("$config{'kPSLlogsSourceDir'}/*.sql*");


  ## untar files
  info("Extracting slony-log tar files into $config{'kPSLreproPath'}.");
  run( 'find ', $config{'kPSLreproPath'}, " -name \"*.tar.gz\" -exec tar zxf {} -C $config{'kPSLreproPath'} \\;" );

  ## check if in the sites directory exists any slony_logs tar file in the range specified in $begin - $end.  if so extract the tar files to the repro directory and let the sql logs be overwritten by the new ones.
  ## This function looks for tar files in the site-specific directories that contain log files that 
  ## lie within the requested reprocessing range. If it finds any, it extract the contained files into
  ## the site-specific repro directory. And it pushes the tar-file path (in then site-specific log-file
  ## directory, not the repro subdirectory) into the 'tar_repro' array.
  info("Extracting slony-log tar files from any relevant pre-archived tar files into $config{'kPSLreproPath'}.");
  expandOldTars($nodeH, $begin, $end);

  ## change kPSLlogsSourceDir to temp location
  $config{'kPSLlogsSourceDir'} = $config{'kPSLreproPath'};

  ## make in memory counter
  my $counter = $begin - 1;
  ##

  info("Entering log-file parse loop.");
  while ((my $srcFile = nextCounter(\$counter,$config{'kPSLlogsSourceDir'})) && ($counter <= $end))
  {
     info("Parsing file $srcFile");
     if (parseLog($cfgH,$nodeH,$srcFile))
     {
        # ART - If an error occurred in parseLog, should bail here.
        error("Problem parsing a file; bailing out.");
        $bail = 1;
        last;
     }

     $counter = saveCounter(undef,$srcFile);
  }

  info("Exiting log-file parse loop.");

  if (!$bail)
  {
     ## recreate tars for sites if needed.
     ## This will create tar files in the site-specific log-file directory (not in the repro subdirectory).
     info("Tar'ing log files in the site-specific log-file directories.");
     my ($tar_begin, $tar_end) = recreateNewTars($nodeH, $begin, $end);

     ## clean up main repro directory
     info("Cleaning up repro directory $config{'kPSLreproPath'}.");
     unlink glob("$config{'kPSLreproPath'}/*");
  }

  # If we bailed out, then all re-processing rolls back, but the repro directory contains a partial parsing.

  thisUnlock(\$lckFH);
}
### END OF MAIN ROUTINE ####


#--|----------------------------------------------------------
#--| ################## Subroutines ######################
#--|----------------------------------------------------------
sub CallDie {
   my $msg = shift;

   # This should only be called by code in the subscribe critical region
   if ($insubcrit)
   {
      # Need to remove this type of lock file, since the other scripts that
      # access it assume the lock is held if the file exists.
      unlink "$subscribelockpath";
   }

   die $msg;
}

sub ReleaseLock
{
   unlink "$subscribelockpath";
   exit(1);
}

sub  expandOldTars {
  my ($nodeH, $begin, $end) = @_;

  for my $node (keys %{$nodeH->{nodes}}) {
    my $repro_path=undef;
    my $path = $nodeH->{nodes}->{$node}->{'path'};

    if (exists $nodeH->{'suffix'}) {
      $repro_path = sprintf("%s/%s",$path, $nodeH->{'suffix'});
    } else {
      $repro_path = sprintf("%s/%s",$path, "repro");
    }

    unless (-d $repro_path) {
      info("Create $repro_path");
      mkdir $repro_path; ## don't want to add the -p option just in case the path falls in the wrong disk partition and fills it.
    }
    my @list = glob("$path/slony_logs_*.tar.gz");
    my @wlist= (); # work list
    ## go through the list of tar files finding those that are withing the repro range
    for my $tfile (@list) {
#                    slony_logs_799371-800809.tar.gz
      if ($tfile =~ /slony_logs_(\d+)-(\d+).tar.gz/) {
        if ($begin <= $2 && $end >= $1) {
          ## extract tar into repro dir

          debug "in expandOldTars begin=$1 and end=$2 expand $tfile";
          run('tar', 'zxf', $tfile, '-C', $repro_path);
          debug "in expandOldTars after expanding $tfile";

          push @wlist, $tfile;
        }
      } 
    }

    ## keep tar list for clean up
    if ($#wlist>=0) {
      #print Dumper [@wlist];
      $nodeH->{nodes}->{$node}->{'tar_repro'}=[@wlist];
    }

  }

}

# I think the point of this function is to make tar files out of just-parsed slony log files
# that were created during a reprocessing request. So...
#   1. One or more tar files of unparsed slony logs is extracted.
#   2. The extracted log files are parsed and made site-specific.
#   3. The site-specific log files are tar'd by this function. Presumably this makes
#      for a smaller download by remote sites.
sub recreateNewTars {
  my ($nodeH, $begin, $end) = @_;

  for my $node (keys %{$nodeH->{nodes}}) {
    my $repro_path=undef;
    my $path = $nodeH->{'nodes'}->{$node}->{'path'};

    if (exists $nodeH->{'suffix'}) {
      $repro_path = sprintf("%s/%s",$path, $nodeH->{'suffix'});
    } else {
      $repro_path = sprintf("%s/%s",$path, "repro");
    } 

    ## go through the list of tar files finding those that are withing the repro range
    ## $tfile is the tar file found
#    print Dumper $nodeH;

    ## find the lower and upper range in the tar files
    ## THIS LOOP FINDS THE BEGINNING COUNTER NUMBER OF THE OLDEST LOG FILE IN
    ## THE SET OF TAR FILES IN THE SITE-SPECIFIC LOG DIRECTORY THAT WERE EXPANDED.
    ## THESE TAR FILES WERE USED BECAUSE THEY CONTAINED LOGS IN THE REPROCESSING
    ## RANGE. THIS IS AN OPTIMIZATION - THIS SCRIPT USES THOSE LOGS, WHICH ARE
    ## ALREADY PARSED, RATHER THAN PARSING NEWLY EXPANDED TAR FILES THAT WERE
    ## FETCHED FROM ARCHIVE.
    ##
    ## IT ALSO FINDS THE
    ## COUNTER NUMBER OF THE NEWEST LOG FILE IN THIS SET OF TAR FILES. $tar_begin
    ## holds the oldest counter number, and $tar_end holds the newest counter number.
    my ($tar_begin, $tar_end)=(undef,undef);
    @tarlist=();
    for my $tfile (@{$nodeH->{'nodes'}->{$node}->{'tar_repro'}}) {
      ## get the beggining and end counter range from the tar files. 
       my ($_begin,$_end) =(undef,undef);
       unless (($_begin,$_end) = ($tfile =~ /slony_logs_([0-9]+)\-([0-9]+).tar.gz/)) {
         my $msg = "Could not find ranges in [$tfile]";
         emergency($msg);
         die($msg);
       }

       push @tarlist, $tfile;
       $tar_begin = $_begin
         if (!defined $tar_begin || $tar_begin > $_begin);

       $tar_end = $_end
         if (!defined $tar_end || $tar_end < $_end );

       debug("working out ranges on [$tfile] [$tar_begin] [$tar_end]\n");
    }

## Now match new tar ranges with reprocessing begin and end ranges
    ## SELECT AS THE OLDEST LOG FILE TO TAR the log file with the counter $tar_begin, unless
    ## that log file is newer than the oldest log file requested for reprocessing. If the latter
    ## is true, use the oldest log file requested for reprocessing instead.
    $tar_begin = $begin
      if (!defined $tar_begin || $tar_begin > $begin);

    my $tar_name = undef;
    if (defined $tar_end) { ## means there is a tar file to be generated
       ## If $tar_end is defined, then there was at least one tar file that was extracted and
       ## used for reprocessing.
      info("working out final ranges on [$tar_begin] [$tar_end]\n");
      ## generateTar generates an aggregate tar file
      $tar_name = generateTar($repro_path,$tar_begin, $tar_end);
    }

    ## Moves the parsed log files from the repro subdirectory to the site-specific log-file
    ## directory.
    cleanUpNode($nodeH->{'nodes'}->{$node}, $path, $repro_path, $tar_name, $tar_begin, $tar_end);

  }
}

## 1. Replace any tar files in the site-specific log-file directory with newly created
##    tar files (in the repro subdirectory) that contain all the files in the original 
##    tar files. The new tar files will contain files not in the original tar files,
##    and the reprocessing requestor will want the log files that were not in the original.
## 2. Move the remaining (not-tar'd) files in the repro directory up one level to the
##    site-specific log-file directory. 
##
## After 1 and 2 have been completed, then the site-specific log-file directory will have
## a combination of tar files and slony log files that contain the complete set of slony
## log files requested.
sub cleanUpNode {
  my ($node, $path, $repro_path, $tar_name, $tar_begin, $tar_end) = @_;


  ## clean up whatever is left from the repro directory

  ## The following code tests the new tar file if any.
  ## Check if any existing tar file exists that is in the range of the new tar file
  ## and deletes them. 
  if (defined $tar_name) {
    if ( -f "$repro_path/$tar_name") {
      if (my ($bcounter,$ecounter) = ($tar_name =~ /slony_logs_(\d+)\-(\d+).tar.gz/)) {
        my ($out,$err,$status) = run3("tar", "-ztf", "$repro_path/$tar_name");
         if ($status != 0 || defined $err) {
           die("command failed with status [$status] and error [$err]\n");
         }
  
         my @sql=map { /.sql$/ } split("\n",$out);

         my $tarred_counter = $ecounter - $bcounter;
   
         if ($#sql != $tarred_counter) {
           die("Validation on tar file $tar_name failed. Number of files tarred doesn't match stated counter");
         } else { 
           info "First validation pass: tar file [$tar_name] sql number [$#sql] and tar stated number is [$tarred_counter]";
         }

        ## delete old tars
        unlink foreach @{$node->{'tar_repro'}};
  
      }
      info ("moving $repro_path/$tar_name to $path/$tar_name.tmp");
      move "$repro_path/$tar_name", "$path/$tar_name.tmp";
      info ("moving $path/$tar_name.tmp to $path/$tar_name");
      move "$path/$tar_name.tmp", "$path/$tar_name";
    }

  }

  info ("Moving rest of sql files");

  moveLogsRest($path, $repro_path, $tar_begin, $tar_end);

  unlink foreach glob("$repro_path/slony1_log_2_*.sql*");
  unlink foreach glob("$repro_path/slogs_*tar.gz");
}

sub moveLogsRest {
  my ($path, $repro_path, $tar_begin, $tar_end) = @_;

  map {
        my ($file, $counter);

        if (($file, $counter) = ($_ =~ /(slony1_log_2_(\d+).sql)/)) {
          ## move over the list of remaining sql files
          ## move all those that are on the range but not tarred
          ## to the site log directory
          $counter*=1;
          if ($tar_end < $counter) {
            info ("move [$_] to [$path/$file]");
            move ($_, "$path/$file");
          }
        }
      } glob("$repro_path/*.sql");

}

sub moveLogsRest2 {
  my ($nodeH, $begin, $end) = @_;

  for my $node (keys %{$nodeH->{nodes}}) {
    my $repro_path=undef;
    my $path = $nodeH->{'nodes'}->{$node}->{'path'};

    if (exists $nodeH->{'suffix'}) {
      $repro_path = sprintf("%s/%s",$path, $nodeH->{'suffix'});
    } else {
      $repro_path = sprintf("%s/%s",$path, "repro");
    }
 
    map {
           my ($file, $counter);

           if (($file, $counter) = ($_ =~ /(slony1_log_2_(\d+).sql)/)) {
           ## move over the list of remaining sql files
           ## move all those that are on the range to the 
           ## site log directory
             $counter*=1;
             if ($end < $counter) {
               info ("move [$_] to [$path/$file]");
               move ($_, "$path/$file");
             }
           }
        } glob("$repro_path/*.sql");

    ## clean up
    unlink foreach glob("$repro_path/slony1_log_2_*.sql*");
    unlink foreach glob("$repro_path/slogs_*tar.gz");


  }
}

sub generateTar {
  my ($repro_path, $begin, $end) = @_;

  my $range_fn = sprintf("%s/%d-%d.lst",$repro_path,$begin,$end);

  info("generateTar create list file is $range_fn");
  open RANGE, '>', $range_fn or die ("could not open file [$range_fn], [$!]");

  ## create list of slony_logs that should be in the tar file
  my @sql_list=();
  for my $log_counter ( $begin .. $end ) {
    my $slony_log = sprintf("slony1_log_2_%020d.sql", $log_counter);
    unless (-f "$repro_path/$slony_log" ) {
      my $msg="Error [$repro_path/$slony_log] doesn't exists";
      emergency($msg);
      die $msg;
    }
    print RANGE $slony_log, "\n";
    push @sql_list, $slony_log;
  }
  close RANGE;

  my $tname= sprintf("slony_logs_%d-%d.tar.gz",$begin, $end);

  run("tar zcf $repro_path/$tname -C $repro_path -T $range_fn");

  info ("remove sql files from [$repro_path] directory");
  map { unlink "$repro_path/$_" } @sql_list; 

  info ("remove list file [$range_fn]");
  unlink $range_fn;

  return $tname;
}

sub thisLock {
  my $this_lock=shift;
  my($fh) = shift; # reference to FH

  debug "trying to get $this_lock\n";

  # open $lockfh, '>', "$this_lock" or die ("error opening file [$this_lock] [$!]\n");
  if (!defined($$fh = FileHandle->new(">$this_lock")))
  {
     die ("error opening file [$this_lock] [$!]\n");
  }

  my($natt) = 0;
  while (1)
  {
    if (flock($$fh, LOCK_EX|LOCK_NB)) 
    {
      debug "Created parse-lock file $this_lock.\n";
      last;
    }
    else
    {
      debug("Didn't acquire lock.");
      if ($natt < 10)
      {
        info("Another process is currently modifying files - waiting for completion.");
        sleep 1;
      }
      else
      {
        info("Warning:: couldn't obtain parse lock; bailing.\n");
        exit(1);
      }
   }

   $natt++;
  }
}

sub thisUnlock {
   my $this_lock_fh=shift; #reference to FH

   flock($$this_lock_fh, LOCK_UN);
   $$this_lock_fh->close;
}

sub run {
  my @cmd = @_;

  my $cmd = join(" ", @cmd);  
  debug ("run cmd=$cmd");
  unless ( open my $fh_cmd, "| $cmd 2>&1" ) {
    my $msg = "Cannot run command [$cmd] : $!";
    emergency($msg);
    croak($msg);
  }
  my $res=""; 
  while (<$fh_cmd>) { 
    $res .= $_;
    error $_;
  }
  close $fh_cmd;
  
  return $res;
}

sub run3 {
  my @cmd = @_;

  my $cmd = join(" ", @cmd);

  info("run cmd=$cmd");
  
  my ($in, $out, $err)=(undef, FileHandle->new,FileHandle->new);

  $out->autoflush(1);
  $err->autoflush(1);

  my $pid = open3($in, $out, $err, $cmd);

  debug ("pid is $pid");
  my $res_error=undef;
  my $res="";
  
  while (<$out>) {
    $res .= $_;
  }

  while (<$err>) {
    $res_error .= $_;
  }

  waitpid($pid, 0);

  my $child_exit_status = $? >> 8;

  $out->close;
  $err->close;
 
  return ($res, $res_error, $child_exit_status);
}


sub nextCounter {
  my ($counter_ref, $log_dir) = @_;

  ## sql files might come under to names
  ## normal slony1_log_2_\d+.sql
  ## or
  ## slon1_log_2_\d+.sql.parsed

  my $file1 = sprintf ("%s/slony1_log_2_%020d.sql",$log_dir,++${$counter_ref});

  debug("next file to look at [$file1]");

  $file1 = (-f $file1)? $file1: (-f $file1.".parsed")? $file1.".parsed":undef;

  if ( defined($file1) ) {
    debug("next file to process [$file1]");
    return $file1;
  } else {
    info ("No file to process");
    return undef;
  }
}

sub list {
  my $pattern = shift;

  @out = glob($pattern);

  if (@out) {
    return $out[0];
  }

  return undef;

}

sub saveCounter {

  my ($counter_file, $log_file) = @_;

  my ($counter)= ($log_file =~ m/slony1_log_2_0+(\d+).sql/ );
  
  if (defined $counter_file) {
    open my $fhW, '>', $counter_file or CallDie "Can't Open $counter_file:$!";
    print $fhW $counter;
    close $fhW;
  }
  return $counter;
}


sub readSlonCounter {
  my $counter_file = shift;

  unless ( -f $counter_file ) {
    CallDie("Could not find counter file");
  }

  my $slon_counter=undef;
  my $retry = 0;
  while ($retry <=10) {
    local $/;
    open my $fhR, '<', $counter_file or CallDie "Can't Open $counter_file:$!";
    my $cur_counter=<$fhR>;

    close $fhR;

    if ($cur_counter =~ /^(\d+)\n__EOF__$/) {
      return $1;
    }
    $retry++;
    sleep 1;
  }

  CallDie("Could not read counter file");
}

## readCounter takes its counter from the last .sql.parsed slon log file found in the slon log directory.
##  it also saves that counter into a file for missmatch checking. if the counter found in the directory system doesn't
##  match that of the file it will issue a warning. Then it will reset the counter in the parser counter file.
sub readCounter {
  my $internal_counter_file = shift;

  my $source_dir=$config{'kPSLlogsSourceDir'};

  my $slon_dir_counter = 0;

  my $last_log_file = (glob("$source_dir/slony1_log_2_0*.sql.parsed"))[-1]; ## get the last element in the array
  if (defined $last_log_file) {
    ($slon_dir_counter) = ($last_log_file =~ /slony1_log_2_0+(\d+).sql.parsed$/);
  } else {
    $last_log_file = (glob("$source_dir/slony1_log_2_0*.sql"))[0]; ## get the first element in the array
    if (defined $last_log_file) {
      ($slon_dir_counter) = ($last_log_file =~ /slony1_log_2_0+(\d+).sql$/);
       --$slon_dir_counter;  ## since there wasn't any .sql.parsed file and we drawn the counter from the sql log file
                             ## discount one.
    }
  }

  unless ( defined($slon_dir_counter) && $slon_dir_counter > 0 ) {
    my $msg = "Could not find last counter for slon1_log_2_0*.sql.parsed or slony1_log_2_0*.sql logs in source directory [$source_dir]";
    emergency($msg);
    CallDie($msg);
  }

  ## if the counter file doesn't exists create one.
  unless ( -f $internal_counter_file ) {
    open my $fhR, '>', $internal_counter_file or CallDie "Can't Open $internal_counter_file for write:$!";
    print $fhR $slon_dir_counter;
    close $fhR;
  }

  open my $fhR, '<', $internal_counter_file or CallDie "Can't Open $internal_counter_filei for read:$!";
  my $internal_file_counter=<$fhR>;
  chomp $internal_file_counter;
  close $fhR;

  if ($internal_file_counter != $slon_dir_counter) { 
    notice("counter in file [$internal_file_counter] and in slon directory [$slon_dir_counter] differ!!");
    my $source_dir=$config{'kPSLlogsSourceDir'};
    open my $fhR, '>', $internal_counter_file or CallDie "Can't Open $internal_counter_file:$!";
    print $fhR $slon_dir_counter;
    close $fhR;
    return $slon_dir_counter;
  } else {
    info("using counter [$internal_file_counter]");
    return $internal_file_counter;
  }

}
## Function : loadConfig
## Arguments: parse_slon_logs config file
## Returns : Array of hash references
## The first hash reference is to an array of hashes. Where the hash key is series name 
## and the array elements the node names
## get all the configuration into a perl hash of the form
## key1 => [ array of node names ]
##  ...
## keyN => [ array of node names ]
## e.g.
##{
#    'hmi.doptest' => [
#                      'sao',
#                      'sdac',
#                      'nso',
#                      'mps'
#                     ],
#    'aia.lev0' => [
#                   'sao',
#                   'sdac',
#                   'nso'
#                  ],
#    'aia.lev1_test4k10s' => [
#                             'sao',
#                             'sdac',
#                             'nso'
#                            ],
#    'aia.vw_v_lev18' => [
#                         'sao'
#                        ]
#        }
#
## where key has the form of namespace.series_name
##
## The second hash reference is a reference to a hash (A) with two elements. The first element, 
## 'nodes' of hash A, is a reference to a hash reference and it is keyed by the node name. The hash
## reference to which it refers contains four elements: 'path', 'tar_repro', 'filename, and 'FD'.
## The first two are defined in this function, the last two are defined in other functions.
## 'path' is the path to the site-specific log directory. tar_repro is a reference to an array containing a 
## list of tar files that need to be extracted during reprocessing. 'filename' is the full path to
## the parsed log file being currently written, and 'FD' is the perl filehandle to that open 
## file.  
##
## The second element of hash A is 'suffix', which is used to form the path into which re-extracted tar
## files will be placed ( $A->{nodes}->{rob}->{path} . "/" . $A->{suffix} ).
##
## e.g.
## {
#      'nodes' => { 'rob' => { 'path' => '/solarport/pgsql/slon_logs/live/site_logs//rob',
#                              'tar_repro' => [ 'slony_logs_1604324-1605763.tar.gz',
#                                               'slony_logs_1605764-1607203.tar.gz' ],
#                              'filename' => '/solarport/pgsql/slon_logs/live/site_logs/rob/repro/slony1_log_2_00000000000001607634.sql',
#                              'FD' => <filehandle to filename> },
#                   'mps' => { 'path' => '/solarport/pgsql/slon_logs/live/site_logs//mps',
#                              'tar_repro' => [ 'slony_logs_1204324-1205763.tar.gz' ], 
#                              'filename' => '/solarport/pgsql/slon_logs/live/site_logs/mps/repro/slony1_log_2_00000000000001607642.sql',
#                              'FD' => <filehandle to filename> }
#                  },
#
#      'suffix' => 'repro'
#
## }

sub loadConfigInt {
  my $cfg_file = shift;
  my $nodelstH = shift; # reference to hash of nodes whose slony logs we want to reprocess

  open CFG, '<', $cfg_file or CallDie ("Couldn't open $cfg_file [$!]");

  ## AN HASH KEYED BY seriesname. EACH ELEMENT IS A REFERENCE TO AN ARRAY OF NODE NAMES.
  ## This hash tells you, for each series, which nodes are subscribed to that series.
  my $cfgH = {};

  ## 
  my $npath= {};

  while (<CFG>) {
    chomp;
 
    ## a config file row looks like
    ## /solarport/pgsql/slon_logs/live/site_logs//rob        /solarport/pgsql/slon_logs/live/etc//rob.lst


    ## the format of the configuration file is:
    ## node_name_path path_to_node_series_list_file
    my @a = split;

    if ($#a != 1) { ## check if it has two elements
      error("Error in parse_log config file");
      next;
    }

    ## get node name
    ## /solarport/pgsql/slon_logs/site_logs//ROB 
    my @b = split('/',$a[0]);
    my $node = pop @b; ## get last element only
    my @nodelst = keys(%$nodelstH);
    
    if ($#nodelst >= 0)
    {
       if (!defined($nodelstH->{lc($node)}))
       {
          # skip nodes we don't care about
          next;
       }
    }

    # 'path' is the path to the site's site-specific log directory
    # 'tar_repro' is an array of tar files that need to be extracted for 
    #    reprocessing.
    $npath->{nodes}->{$node} = { 'path'=>$a[0], 'tar_repro'=>[] };

    ## opening node list file 
    open LST, '<', $a[1] or CallDie ("Couldn't open list file for $a[0] $a[1]: [$!]");
    while (<LST>) {
      ##format of series list file is 
      ## namespace.series_name
      ## or 
      ## "namespace"."series_name"
      ## weed out empty lines
      chomp;

      ## Right now we don't consider namespace but we want to introduce it, 
      ## while begin backward compatible.
      
      #"name_space"."series_hello"
      #name_space.series_hello
      #name_space."series_hello"
      #"series_hello"
      #series_hello
      # the followin regex matches all of the above 
      my ($namespace, $series) = (undef,undef);

      ## EXTRACT THE namespace AND specific name FROM THE SERIES NAME IN THE 
      ##   CURRENT LINE OF THE LST FILE.
      my $pstring = $_;
      if ($pstring=~m/"?(([^\."'\s]+)"?\.)?"?([^\."'\s]+)"?/) {
        unless (defined $2) {
          if (exists $complete->{$3}) {
            ($namespace, $series) = ($complete->{$3},$3);
          } else {
            error("node [$node] doesn't have a valid namespace for series [$3]");
            next;
          }
        } else {
          ($namespace, $series) = ($2,$3);

        }
      } else {
#        info("no match for [$pstring]");
        next;
      }

      ## $key IS A SERIESNAME OBTAINED FROM THE CURRENT LST FILE
      my $key = sprintf(qq("%s"."%s"),lc($namespace),lc($series));
      if ( exists $cfgH->{$key} ) {
        if (grep { m/$node/ } @{$cfgH->{$key}}) {
          error("Duplicate entry for [$key] in node [$node]");
        } else {
          push @{$cfgH->{$key}},$node;
        }
      } else {
        $cfgH->{$key} = [$node];
      }
    }
  }

  close CFG;
  close LST;

  return ($cfgH, $npath);
}

sub loadConfig
{
   return loadConfigInt(@_);
}

sub loadConfigFilt
{
   return loadConfigInt(@_);
}

# This function opens, in the site-specific log directory, new slony logs for writing. The parser will
# process each original slony log and dump the resulting site-specific SQL into the site-specific log 
# directory. If this function is called as part of a reprocessing request, the new log files will
# be created in a "repro" subdirectory in the site-specific log directory.
sub openSlonLogs {
  my ($nodeH, $slon_name) = @_;

  ## remove any parsed suffix
  $slon_name=~s/\.parsed$//;

  for $node (keys %{$nodeH->{nodes}}) {
    if (exists $nodeH->{'suffix'}) {
      $nodeH->{nodes}->{$node}->{'filename'} = sprintf("%s/%s/%s",$nodeH->{nodes}->{$node}->{'path'}, $nodeH->{'suffix'}, $slon_name);
    } else {
      $nodeH->{nodes}->{$node}->{'filename'} = sprintf("%s/%s",$nodeH->{nodes}->{$node}->{'path'}, $slon_name);
    }
    my $filename = $nodeH->{nodes}->{$node}->{'filename'} . ".tmp";
    my $fh = undef;

    debug ("open file [$filename] for node [$node]");
    #open $fh, '>', $filename or emergency("Can NOT open slonlog for node [$node], file [$filename]");
    open $fh, '>', $filename or CallDie ("Can NOT open slonlog for node [$node], file [$filename]");
    $nodeH->{nodes}->{$node}->{'FD'} = $fh;
  }
  
}

# This function closes the file handle of the current site-specific log being written. It does
# so for all nodes' current log files. It then renames the log by removing the ".tmp" suffix.
#
# Returns 0 upon success, 1 upon failure.
sub closeSlonLogs {
  my ($nodeH) = @_;
  my($rv) = 0;

  for $node (keys %{$nodeH->{nodes}}) {
    ## close and rename file from .tmp to final name
     if (defined $nodeH->{nodes}->{$node}->{'FD'})
     {
        unless(close $nodeH->{nodes}->{$node}->{'FD'})
        {
           my $fpath = $nodeH->{nodes}->{$node}->{'path'}; 
           error("Problem writing file $fpath"); 
           $rv = 1; 
           last; 
        }
     }

     rename $nodeH->{nodes}->{$node}->{'filename'}.".tmp", $nodeH->{nodes}->{$node}->{'filename'}
       if defined $nodeH->{nodes}->{$node}->{'filename'};
  }
  
  return $rv;
}

# Writes a single line to each node's site-specific log file. If the line being 
# written contains an insert statement, then that line is written to a nodes'
# site-specific logs only if the insert is into a series to which that the node is 
# subscribed.
sub dumpSlonLog {
  my ($cfgH, $nodeH, $series, $line) = @_;

  if (defined $series) {
    for $node (@{$cfgH->{$series}}) {
      my $fh=$nodeH->{nodes}->{$node}->{'FD'};
      debug("dump line [$line] for node [$node]");
      print $fh $line if defined $fh
    }
  } else {
    for $node (keys %{$nodeH->{nodes}}) {
      my $fh=$nodeH->{nodes}->{$node}->{'FD'};
      debug("dump line [$line] for node [$node]");
      print $fh $line if defined $fh
    }
  }

  # These file writes could fail! Check for failure at file-close time.
  # Do not check the file stream status after every write. If the file is
  # NFS-mounted, and NFS is set to write asynchronously (a performance
  # enhancement, then you can't rely upon the return value of the 
  # fh->close() call. 

  # ART - 2012.02.01 - Keh-Cheng and I moved the slony logs to local disk, so we don't have to worry about
  # NFS reporting a false negative. Now we need to check all calls on close($fh) [returns true if succeeds].
  # This script currently closes all open output log file handles en masse. These open file descriptors
  # correspond to a single open (for reading) original log file. As the descriptors are closed, we
  # should check the close() return values, and bail if there is a problem. And log that problem.
}

## test end of line in insert sql  - ISS 2010/Aug/20
sub dumpErrorLog {
  my ($cfgH, $series, $line) = @_;
  if (defined $series) {
    for my $node (@{$cfgH->{$series}}) {
      error("bad log line [$line] for node [$node]");
    }
  }
}

# Parse a single original slony log file into N site-specific log files, one for each node.
# In the closeSlonLogs() function, check for problems writing files, the return a status
# code to indicate problems. Callers of parseLog should bail without continuing if there
# is a problem.
#
# Returns 0 upon success, 1 upon failure.
sub parseLog {
  my ($cfgH, $nodeH, $srcFile) = @_;

  my $srcFileBName = basename($srcFile);
  my($rv);

  $rv = 0;

  debug("Parsing log [$srcFile]");

  openSlonLogs($nodeH, $srcFileBName);

  open SRC, '<', $srcFile or CallDie ("Could not open source file [$srcFile]\n");

  while (<SRC>) {
 
    if ($_ =~ /select "_jsoc"\.archiveTracking_offline\('.*'\);/i ||
        $_ =~ /^--/ ||
        $_ =~ /^start tran/i ||
        $_ =~ /^commit/i ||
        $_ =~ /^vacuum/i ) {
      dumpSlonLog($cfgH, $nodeH, undef, $_);
      next;
    }

  #e.g.
  #insert into "lm_jps"."lev1_test4k10s"
    ## test end of line in insert sql  - ISS 2010/Aug/20
    if ($_ =~ /^insert\s+into\s+("\S+"\."\S+")/i) {
      my $series = $1;
      if ($_ =~ /.*\);$/ ) {
        dumpSlonLog($cfgH, $nodeH, $series, $_);
      } else {
        dumpErrorLog($cfgH, $series, $_);
      }
    }
  }

  close SRC;

  if (($rv = closeSlonLogs($nodeH)))
  {
     error("Problem parsing source log file $srcFile.") 
  }

  return $rv;
}

sub serverLock {
  my (%config) = @_;

  my $port=$config{'SLAVEPORT'};
  my $dbname=$config{'SLAVEDBNAME'};

  my $lock_action='LOCK';
  my $lock_id = "PARSER";
  my $pid = $$;
  my $process_name = basename($0);

  my $count=0;
  while ( $count < 120 ) {
    my $sql = "start transaction; select ps_serverlock('$lock_action','$lock_id', $pid, '$process_name'); commit;"; 

    my ($out, $err, $status) = run3("echo \"$sql\" | $config{'kPsqlCmd'} -p $port -q -At -d $dbname");
    if ($status != 0 || defined $err) {
      $count++;
      sleep 1;
    } elsif ($status == 0 && !defined $err) {
      chomp $out;
      return $out eq 'locked';
    }
  }
}

sub serverUnLock {
  my (%config) = @_;
  my $port=$config{'SLAVEPORT'};
  my $dbname=$config{'SLAVEDBNAME'};
  my $lock_action='UNLOCK';
  my $lock_id = "PARSER";
  my $pid = $$;
  my $process_name = basename($0);

  my $count=0;
  while ( $count < 120 ) {
    my $sql = "start transaction; select ps_serverlock('$lock_action','$lock_id', $pid, '$process_name'); commit;"; 

    my ($out, $err, $status) = run3("echo \"$sql\" | $config{'kPsqlCmd'}  -p $port -q -At -d $dbname");
    if ($status != 0 || defined $err) {
      $count++;
      sleep 1;
    } elsif ($status == 0 && !defined $err) {
      print "out=$out\n";
      print "err=$err\n";
      last;
    }
  }
}

sub checkOldLocks {
  my (%config) = @_;
  my $lock_id = "PARSER";
  my $port=$config{'SLAVEPORT'};
  my $dbname=$config{'SLAVEDBNAME'};

  my $sql = "select process_pid, process_name from subscribe_lock where request_lock_id = '$lock_id' or lock_id = '$lock_id'";

  my ($out, $err, $status) = run3("echo \"$sql\" | $config{'kPsqlCmd'}  -p $port -q -At -d $dbname");
  if ($status != 0 || defined $err) {
    print "OUT=$out\n";
    print "ERROR=$err\n";
  } elsif ($status == 0 && !defined $err) {
#    print "out=$out\n";
    @lines = map { [$1,$2] if /(\d+)\|(\S+)/ }split ("\n",$out);
    print Dumper [@lines];
    for $proc_arr (@lines) {
      my ($pid, $proc_name) = @{$proc_arr};
      unless (pidExists($pid, $proc_name))  {
        my $del_sql="delete from subscribe_lock where request_lock_id = '$lock_id' and process_pid=$pid and process_name = '$proc_name'";
        my ($out, $err, $status) = run3("echo \"$del_sql\" |  $config{'kPsqlCmd'}  -p $port -q -At -d $dbname");
        if (defined $err) {
          emergency("SQL [$del_sql] failed ... exiting");
          exit 1;
        }

      }
    ##TODO: check if one of the processes to delete is self
    }
    print "err=$err\n";
  }
}

sub pidExists {
  my ($pid, $proc_name) = @_;
  my ($out, $err, $status) = run3("ps -p $pid -o comm=");
  chomp $out;
  if ($status == 1) {
    info("Pid=$pid and proc = $proc_name does't exists: status=$status");
    return 1==0;
  } elsif ($status ==0 && $out eq $proc_name) {
    info("out=[$out] Pid=$pid and proc = $proc_name exists: status=$status");
    return 1==1;
  } else {
    error("Couldn't resolve output [$out] and err=[$err]; status = [$status]\n");
    return 1==1;
  }
}

sub usage {
  print <<EOF;

Usage: parse_slon_logs.pl <config file> <lock file> [-hH --help] [--repro --[beg|begin]=<number> --end=<number>]

where 'beg' and 'end' are the beginning and end counters of the slony logs.
EOF
exit;
}


