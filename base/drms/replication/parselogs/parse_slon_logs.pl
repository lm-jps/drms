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

use File::Basename;
use Log;
use Data::Dumper;
use Getopt::Long;
use Fcntl ':flock';
use Carp;
use File::Copy;

my ($repro,$begin,$end);
my $opts = GetOptions ("help|h|H" => \&usage,
                       "repro"    => \$repro,
                       "begin|beg=i"    => \$begin,
                       "end=i"    => \$end);

my $config_file=$ARGV[0] or die ("No config_file specified");

###################################################################################################################

#### Get CONFIGURATION variables from $config_file
open (SETTINGS, '<', $config_file)
        or die "Error cannot open config file [$config_file] : $!";

my %config = map {
        chomp;
        my ( $key, $val ) = m/^\s*(\w+)\s*=\s*([^#]*?)\s*(?:$|#)/;
        $val =~ s/^"(.*)"$/$1/;
        $val =~ s/^'(.*)'$/$1/;
        defined($val)
                ? ($key,$val)
                : ()
} grep {
        # must have an equals sign, must not start with a '#';
        m/=/ and !( m/^#/)
} (<SETTINGS>);
close(SETTINGS);

my @missing_config = grep { ! defined($config{$_}) } qw(
     kPSLlogsSourceDir
     kPSLprepCfg
     kPSLprepLog
     kPSLlogReady
     kPSLparseCounter
     kPSLreproPath
     kPSLaccessRepro
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

map { debug "$_=$config{$_}\n"; } qw(
     kPSLlogsSourceDir
     kPSLprepCfg
     kPSLprepLog
     kPSLlogReady
     kPSLparseCounter
     kPSLreproPath
     kPSLaccessRepro
);

## Normal operation (no reprocessing)
unless ($repro) {

	##
	## WARNING : using basename($0) *is* better than $0 for the lock, but
	## you risk someone making a copy of the script to test, and running it
	## while the production copy is running -- I'd hard code the lock file.
	##

  thisLock(basename($0));  ## only one instance of this process running
  ## normal processing
  debug("load config " . $config{'kPSLprepCfg'});

  ## load configuration in a hash structure
  my ($cfgH, $nodeH) = loadConfig($config{'kPSLprepCfg'});

  my $counter      = readCounter($config{'kPSLparseCounter'});
  my $slon_counter = readSlonCounter($config{'kPSLlogReady'});
  info ("Current counter [$counter]");

  debug("b4 while loop [$counter] [$slon_counter]");
  while ((my $srcFile = nextCounter(\$counter,$config{'kPSLlogsSourceDir'})) && ($counter <= $slon_counter) ) {
    debug("counter is [$counter]");

    parseLog($cfgH,$nodeH,$srcFile);
    $counter = saveCounter($config{'kPSLparseCounter'},$srcFile);
    debug("moving $srcFile to $srcFile.parsed");
    move( $srcFile, "$srcFile.parsed" );
  }

} else { ## user wants reprocessing ###

	##
	## WARNING : using basename($0) *is* better than $0 for the lock, but
	## you risk someone making a copy of the script to test, and running it
	## while the production copy is running -- I'd hard code the lock file.
	##
  thisLock(basename($0)."repro");  ## only one instance of reprocessing running

  debug("load config " . $config{'kPSLprepCfg'});

  ## load configuration in a hash structure
  my ($cfgH, $nodeH) = loadConfig($config{'kPSLprepCfg'});

	##
	## SUGGESTION : if you added these options to the earlier 'GetOptions' call,
	## you wouldn't have to do the strange calling syntax:
	## 
	##     command (first options) -- (second options)
	##

  unless (($begin =~ /\d+/) && ($end =~ /\d+/)) {
    die(usage());
  }

  $nodeH->{'suffix'}="repro";  ## This assumes a repro directory exists on the slony_logs/sites directories. If it doesn't exist it tries to create it and generate all the logs for that site in that location.

  ## run access repro to download data.
  run( $config{'kPSLaccessRepro'}, 'logs=su_production.slonylogs', "path=$config{'kPSLreproPath'}", "beg=$begin", "end=$end", 'action=ret' );


  ## untar files
  run( 'find', $config{'kPSLreproPath'}, qw( -name *.tar.gz -exec tar xfz {} -C ), $config{'kPSLreproPath'}, ';' );
  

  ## check if in the sites directory exists any slony_logs tar file in the range specified in $begin - $end.  if so extract the tar files to the repro directory and let the sql logs be overwritten by the new ones.

  expandOldTars($nodeH, $begin, $end);

  ## change kPSLlogsSourceDir to temp location
  $config{'kPSLlogsSourceDir'} = $config{'kPSLreproPath'};

  ## make in memory counter
  my $counter = $begin;
  ##
  while ((my $srcFile = nextCounter(\$counter,$config{'kPSLlogsSourceDir'})) && ($counter <= $end)) {
    parseLog($cfgH,$nodeH,$srcFile);
    $counter = saveCounter(undef,$srcFile);
  }

  ## recreate tars for sites if needed.
  recreateOldTars($nodeH);

  move( $_, $path ) foreach glob("$config{'kPSLreproPath'}/*.sql");

  ## clean up
  unlink foreach glob("$config{'kPSLreproPath'}/slony1_log_2_*.sql*");
  unlink foreach glob("$config{'kPSLreproPath'}/slogs_*tar.gz");

}
### END OF MAIN ROUTINE ####


#--|----------------------------------------------------------
#--| ################## Subroutines ######################
#--|----------------------------------------------------------
sub  expandOldTars {
  my ($nodeH, $begin, $end) = @_;

  for my $node (keys %{$nodeH}) {
    my $repro_path=undef;
    my $path = $nodeH->{$node}->{'path'};

    if (exists $nodeH->{'suffix'}) {
      $repro_path = sprintf("%s/%s",$path, $nodeH->{'suffix'});
    } else {
      $repro_path = sprintf("%s/%s",$path, "repro");
    }

    unless (-d $repro_path) {
      mkdir $repro_path; ## don't want to add the -p option just in case the path falls in the wrong disk partition and fills it.
    }

    my @list = glob("$path/slony_logs_*.tar.gz");
    my @wlist= (); # work list
    ## go through the list of tar files finding those that are withing the repro range
    for my $tfile (@list) {
      if ($tfile =~ /slony_logs_(\d+)-(\d+).tar.gz/) {
        if ($begin <= $2 && $end >= $1) {
          ## extract tar into repro dir
          run('tar', 'xfs', $tfile, '-C', $repro_path);
          push @wlist, $tfile;
        }
      } 
    }
    ## keep tar list for clean up
    if ($#wlist>=0) {
      $nodeH->{$node}->{'tar_repro'}=[@wlist];
    }

  }
}


sub recreateOldTars {
  my ($nodeH) = @_;

  for my $node (keys %{$nodeH}) {
    my $repro_path=undef;
    my $path = $nodeH->{$node}->{'path'};

    if (exists $nodeH->{'suffix'}) {
      $repro_path = sprintf("%s/%s",$path, $nodeH->{'suffix'});
    } else {
      $repro_path = sprintf("%s/%s",$path, "repro");
    } 

    my $wlist= []; # work list
    ## go through the list of tar files finding those that are withing the repro range
    ## $tfile is the tar file found
    for my $tfile (@{$nodeH->{$node}->{'tar_repro'}}) {
      my $tname = basename($tfile);
      generateTar($repro_path,$tname);
      move "$repro_path/$tname", "$path/$tname.tmp";
      unlink "$path/$tname";
      move "$path/$tname.tmp", "$path/$tname";
    }
  }
}

sub generateTar {
  my ($repro_path, $tname) = @_;
  unless (($begin,$end) = ($tname =~ /slony_logs_(\d+)-(\d+).tar.gz/)) {
    my $msg = "Could not find ranges in [$tname]";
    emergency($msg);
    die($msg);
  }

  my $range_fn = sprintf("%s/%d-%d.lst",$repro_path,$begin,$end);
  open RANGE, '>', $range_fn or die ("could not open file [$range_fn], [$!]");

  ## create list of slony_logs that should be in the tar file
  my @sql_list=();
  for my $log_counter ( $begin .. $end ) {
    my $slony_log = sprintf("slony1_logs_2_%020d.sql", $log_counter);
    printf RANGE $slony_log, "\n";
    push @sql_list, $slony_log;
  }
  close RANGE;

  run('tar', '-T', $range_fn, 'cfz', $tname, '-C', $repro_path);

  map { unlink $_ } @sql_list; 
}

sub thisLock {
  my $myname=shift;

  open SLOCK, '>', "/tmp/$myname.lock" or die ("error opening file /tmp/$myname.lock [$!]\n");

  ## only one version of this program running
  unless (flock(SLOCK, LOCK_EX|LOCK_NB)) {
    warning "$myname is already running. Exiting.\n";
    exit(1);
  }
}

sub run {
  my @cmd = @_;
  
  unless ( open my $cmd, '-|', @cmd ) {
    my $msg = "Cannot run command [@cmd] : $!";
    emergency($msg);
    croak($msg);
  }
  
  my $res = join '', <$cmd>;
  close $cmd;
  
  if ($? != 0) {
    my $msg="The command [@cmd] failed";
    emergency($msg);
    croak($msg);
  }
  return $res;
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
    open my $fhW, '>', $counter_file or die "Can't Open $counter_file:$!";
    print $fhW $counter;
    close $fhW;
  }
  return $counter;
}


sub readSlonCounter {
  my $counter_file = shift;

  unless ( -f $counter_file ) {
    die("Could not find counter file");
  }

  my $slon_counter=undef;
  my $retry = 0;
  while ($retry <=10) {
    local $/;
    open my $fhR, '<', $counter_file or die "Can't Open $counter_file:$!";
    my $cur_counter=<$fhR>;

    close $fhR;

    if ($cur_counter =~ /^(\d+)\n__EOF__$/) {
      return $1;
    }
    $retry++;
    sleep 1;
  }

  die("Could not read counter file");
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
    die($msg);
  }

  ## if the counter file doesn't exists create one.
  unless ( -f $internal_counter_file ) {
    open my $fhR, '>', $internal_counter_file or die "Can't Open $internal_counter_file for write:$!";
    print $fhR $slon_dir_counter;
    close $fhR;
  }

  open my $fhR, '<', $internal_counter_file or die "Can't Open $internal_counter_filei for read:$!";
  my $internal_file_counter=<$fhR>;
  chomp $internal_file_counter;
  close $fhR;

  if ($internal_file_counter != $slon_dir_counter) {
    notice("counter in file [$internal_file_counter] and in slon directory [$slon_dir_counter] differ!!");
    my $source_dir=$config{'kPSLlogsSourceDir'};
    open my $fhR, '>', $internal_counter_file or die "Can't Open $internal_counter_file:$!";
    print $fhR $slon_dir_counter;
    close $fhR;
    return $slon_dir_counter;
  } else {
    return $internal_file_counter;
  }

}
## Function : loadConfig
## Arguments: parse_slon_logs config file
## Returns : Array of hashes
## The first hash is an array of hashes. Where the hash key is series name 
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
## The second argument is a simple hash of
## node => path to slony log
sub loadConfig {
  my $cfg_file = shift;

  open CFG, '<', $cfg_file or die ("Couldn't open $cfg_file [$!]");

  my $cfgH = {};
  my $npath= {};

  while (<CFG>) {
    chomp;
 
    ## a config file row looks like
    ## /solarport/pgsql/slon_logs/site_logs//ROB    /solarport/pgsql/slon_logs/etc//ROB.lst 


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

    $npath->{$node} = { 'path'=>$a[0], 'tar_repro'=>[] };

    ## opening node list file 
    open LST, '<', $a[1] or die ("Couldn't open list file for $a[0] $a[1]: [$!]");
    while (<LST>) {
      ##format of series list file is 
      ## namespace.series_name
      ## or 
      ## "namespace"."series_name"
      ## weed out empty lines
      chomp;

      ## Right now we don't consider namespace but we want to introduce it, 
      ## while being backward compatible.
      
      #"name_space"."series_hello"
      #name_space.series_hello
      #name_space."series_hello"
      #"series_hello"
      #series_hello
      # the followin regex matches all of the above 
      my ($namespace, $series) = (undef,undef);

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

      my $key = sprintf(qq("%s"."%s"),$namespace,$series);
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

sub openSlonLogs {
  my ($nodeH, $slon_name) = @_;

  ## remove any parsed suffix
  $slon_name=~s/\.parsed$//;
  for $node (keys %{$nodeH}) {
    if (exists $nodeH->{'suffix'}) {
      $nodeH->{$node}->{'filename'} = sprintf("%s/%s/%s",$nodeH->{$node}->{'path'}, $nodeH->{'suffix'}, $slon_name);
    } else {
      $nodeH->{$node}->{'filename'} = sprintf("%s/%s",$nodeH->{$node}->{'path'}, $slon_name);
    }
    my $filename = $nodeH->{$node}->{'filename'} . ".tmp";
    my $fh = undef;
    debug ("open file [$filename] for node [$node]");
    open $fh, '>', $filename or emergency("Can NOT open slonlog for node [$node], file [$filename]");
    $nodeH->{$node}->{'FD'} = $fh;
  }
  
}

sub closeSlonLogs {
  my ($nodeH) = @_;

  for $node (keys %{$nodeH}) {
    ## close and rename file from .tmp to final name
    close $nodeH->{$node}->{'FD'} if defined $nodeH->{$node}->{'FD'};
    rename $nodeH->{$node}->{'filename'}.".tmp", $nodeH->{$node}->{'filename'}
      if defined $nodeH->{$node}->{'filename'};
  }
  
}
sub dumpSlonLog {
  my ($cfgH, $nodeH, $series, $line) = @_;

  if (defined $series) {
    for $node (@{$cfgH->{$series}}) {
      my $fh=$nodeH->{$node}->{'FD'};
      debug("dump line [$line] for node [$node]");
      print $fh $line if defined $fh
    }
  } else {
    for $node (keys %{$nodeH}) {
      my $fh=$nodeH->{$node}->{'FD'};
      debug("dump line [$line] for node [$node]");
      print $fh $line if defined $fh
    }
  }
}


sub parseLog {
  my ($cfgH, $nodeH, $srcFile) = @_;

  my $srcFileBName = basename($srcFile);

  openSlonLogs($nodeH, $srcFileBName);

  open SRC, '<', $srcFile or die ("Could not open source file [$srcFile]\n");
  info("Parsing log [$srcFile]");
  while (<SRC>) {
 
    if ($_ =~ /select "_jsoc"\.archiveTracking_offline\('.*'\);/ ||
        $_ =~ /^--/ ||
        $_ =~ /^start tran/ ||
        $_ =~ /^commit/ ||
        $_ =~ /^vacuum/ ) {
      dumpSlonLog($cfgH, $nodeH, undef, $_);
      next;
    }

  #e.g.
  #insert into "lm_jps"."lev1_test4k10s"
    if ($_ =~ /^insert\s+into\s+("\S+"\."\S+")/) {
      dumpSlonLog($cfgH, $nodeH, $1, $_);
    }
  }

  close SRC;
  closeSlonLogs($nodeH);
}


sub usage {
  print <<EOF;

Usage: parse_slon_logs.pl <config file> [-hH --help] [--repro --[beg|begin]=<number> --end=<number>]

where 'beg' and 'end' are the beginning and end counters of the slony logs.
EOF
exit;
}
