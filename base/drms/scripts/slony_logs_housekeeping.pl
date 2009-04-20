#!/usr/bin/perl -w

##############################################################################################
##  SLONY logs directory housekeeping
##  =================================
##
##  Modification History:
##  ======================
##
##  08-Dec-08:  Igor     : Slony logs housekeeping script
##  16-Dec-08:  Igor     : Delete slony sql files that have
##                         been tarred already.
##  22-Jan-09:  Igor     : Allow the script to only do deletes or archives
##                         by default the script will only do deletes
##                         if archives are required the argument "archive"
##                         should be passed in.
##  14-Apr-09:  Igor     : Cater for unplanned interruptions. Make the script rerunnable.
##  16-Apr-09:  Igor     : Improve rerunnable logic. created validate_tar function
##
##  ------------------------------------------------------
##  This script tars slony files in batches of n*1000 files, where n is interger > 0.
##  The tar files will always finish in a rounded thousand.
##   E.g. 1000, 2000, 3000 etc 
##  The logic keeps a minimum of $KEEP_NO files untar at any time, up to a maximum of $GAP_NO.
##  The number of files per tar file will be ($GAP_NO - $KEEP_NO).
##     E.g.
##        GAP_NO=3000;
##        KEEP_NO=1000;
##        GAP_NO - KEEP_NO =  2000 files in each tar file.
##  Note: In this version the tarred sql logs are moved into a temp directory called ".backup".
##        The move operation within the same file system normally involves a change in the inode table
##        for that file. If someone is downloading while the file gets moved the download operation should 
##        be able to continue.
##        We could improved the script by deleting the .backup directory on the next pass of the script.
##
## Note: Please check the variables labeled "STATIC VARIABLES" and change them according 
##       to your system
##############################################################################################


use IO::Dir;
use Fcntl ':flock';

#########################################
##          STATIC VARIABLES           ##
##
my $DEL_ONLY  = "N";
my $MV_CMD    = "/bin/mv";
my $MKDIR_CMD = "/bin/mkdir";
my $TAR_CMD   = "/bin/tar";
my $RM_CMD    = "/bin/rm";
my $LOGS_DIR  = "/c/slony_logs/igor";
my $GAP_NO    = 3000;  ## Number of sql logs that will trigger tarring.
my $KEEP_NO   = 1000;  ## Minimum number of sql logs that stay untarred.
my $TRANSFER_CMD = undef;
##
#########################################

## only one version of this program running
unless (flock(DATA, LOCK_EX|LOCK_NB)) {
    print "$0 is already running. Exiting.\n";
    exit(1);
}

if ( defined $ARGV[0] && $ARGV[0] =~ /archive/i) {
  $DEL_ONLY="N";
}

## make things easy. Work in specify directory
chdir $LOGS_DIR;

tie (my %rootDir, "IO::Dir", ".");
##Note that the $sql_list contains the slony counter number only.
my @sql_list =  sort {$a<=>$b} map {$_=~/slony1_log_2_0+(\d+)\.sql/?$1:()} keys(%rootDir);

## sort tar files based on their slony numbers
my @tar_files=  sort {($a=~/(\d+)-\d+/)[0]<=> ($b=~/(\d+)-\d+/)[0]} map {$_=~/(slony.*_\d+-\d+\.tar.*)/?$1:()} keys(%rootDir);
##my @tar_list =  sort {($a=~/(\d+)-\d+/)[0]<=> ($b=~/(\d+)-\d+/)[0]} map {$_=~/slony.*_(\d+-\d+)\.tar.*/?$1:()} keys(%rootDir);

# Exit if no slony sql log files are present.
if ($#sql_list < 0) {
  print "No files to process .... exiting\n";
  exit;
}

my $last_tar_file = undef;                 ## file name of the last tar file
my $sql_counter =  $sql_list[$#sql_list];  ## counter of last slony sql file in directory
my $tar_counter =  undef;                  ## couter of last slony sql file tarred

## if no tar files are present set it to the lowest of the sql counter.
if ($#tar_files < 0 ) {
  $tar_counter = $sql_list[0] - 1;
} else {
  $last_tar_file = $tar_files[$#tar_files];
  ($tar_counter) = ($last_tar_file =~ /-(\d+)/);
}


print "last sql elem:", $sql_counter, "\n";
print "last tar elem:", $tar_counter, "\n";
print "###################\n";
################################################
## Deal with slony files that has been tarred. #
## but still remain in the directory           #
################################################

if ($sql_list[0] <= $tar_counter) {
  print "Deal with untarred files from $sql_list[0] to $tar_counter\n";
  for my $_counter ($sql_list[0] .. $tar_counter) {
    my $sql_2_del = sprintf("slony1_log_2_%020d.sql", $_counter);
    if ($DEL_ONLY eq "Y") {
      `$RM_CMD $sql_2_del`;
    } else {
      `$MKDIR_CMD .backup` unless -d ".backup";
      `$MV_CMD $sql_2_del ./.backup`;
    }
  }
}

exit if $DEL_ONLY eq "Y";

my $diff=$sql_counter - $tar_counter;

if ($diff > $GAP_NO) {
  my $mod=$sql_counter%1000;
  my $keep_no = $mod + $KEEP_NO;
  my $tar_no  = $diff - $keep_no;
  print "keep :$keep_no and tar :$tar_no\n";

  my $sql_start_no = $tar_counter+1;
  my $sql_end_no   = $sql_counter - $keep_no;

  print "tar sql from $sql_start_no to $sql_end_no\n";


  open TARLIST, ">.tar_list.txt" or die "Cannot open file .tar_list.txt. [$!]";

  my $tar_cmd = "$TAR_CMD cfz .tar.tmp -T .tar_list.txt";

  for my $_counter ($sql_start_no .. $sql_end_no) {
    printf TARLIST "slony1_log_2_%020d.sql\n", $_counter;
  }
  close TARLIST;

  print "Tar cmd: $tar_cmd\n";
  my $exec_output=system($tar_cmd);

  print "exec_output=[$exec_output]\n";

  if ($exec_output == 0 ) {
    ## clean up
    `$RM_CMD .tar_list.txt`;
    my $tar_file_name = sprintf("slony_logs_%d-%d.tar.gz", $sql_start_no, $sql_end_no);

    my $error = validate_tar(".tar.tmp", $sql_start_no, $sql_end_no);

    if ($error == 0) {
      system("$MV_CMD .tar.tmp $tar_file_name");
    }


    ## move files to backup dir
    `$MKDIR_CMD .backup` unless -d ".backup";
    map {`$MV_CMD $_ ./.backup`} split ("\n", `$TAR_CMD tfz $tar_file_name`) if -f $tar_file_name;

  } else {
    die ("tar cmd [$tar_cmd] failed to execute\n");
  }


}

if (defined $TRANSFER_CMD) {
  my $status = system("$TRANSFER_CMD");
}

## 1.- checks that the tar file untars and uncompresses correctly.
## 2.- make sure the contents of the tar file matches the numeric values.
##     i.e. The upper and lower limits are ok
##          The number of files is ok
sub validate_tar {
  my ($tar_name, $first_tar_counter, $last_tar_counter) = @_;

  ## untar file
  my $error = system("$TAR_CMD tfz $tar_name > .dump.tar");
  
  if ($error !=0 ) {
    print "ERROR executing [$TAR_CMD tfz $tar_name > .dump.tar] error [$error]\n";
    return $error;
  } else {
    my $tar=".dump.tar";
    open TARF, "<$tar" or die ("could not open $tar file");

    my $first_sql_in_tar=undef;
    my $last_sql_in_tar=undef;
    my $total_count    = 0;
    while (<TARF>) {
      chomp $_;
      $total_count++;
      $first_sql_in_tar=$_ unless defined $first_sql_in_tar;
      $last_sql_in_tar=$_;
    }
    close TARF;

    ## remove dump
    `$RM_CMD $tar`;

    my ($first_sql_counter) = ($first_sql_in_tar =~ /slony1_log_2_0+(\d+)\.sql/);
    my ($last_sql_counter)  = ($last_sql_in_tar  =~ /slony1_log_2_0+(\d+)\.sql/);

    if ($first_sql_counter != $first_tar_counter) {
      print "Counter of first sql in tar [$first_sql_counter] doesn't match with tar file lower limit counter [$first_tar_counter]\n";
      return -1;
    }

    if ($last_sql_counter != $last_tar_counter) {
      print "Counter of last sql in tar [$last_sql_counter] doesn't match with tar file upper limit counter [$last_tar_counter]\n";
      return -2;
    }

    if ($total_count != ($last_sql_counter - $first_sql_counter + 1)) {
      print "Missing sql files in tar file: total count [$total_count], real count [", $last_sql_counter - $first_sql_counter, "]\n";
      return -3;
    }
  }
  return 0;
}  
__DATA__
