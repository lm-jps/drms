#!/usr/bin/perl -w

## 
##  2009/05/05  :: igor    : Make sure one script is running per cluster name
##                         : Keep going to the slony source directory till all
##                         : log files are dealt with.
##  2009/07/10  :: igor    : Default slony counter from database
##
use lib qw(/home/slony/Scripts);

use Net::FTP;
use Data::Dumper;
use Net::SCP;
use Net::SSH qw(ssh ssh_cmd issh sshopen2 sshopen3);
use Fcntl ':flock';

use strict;

my $cluster_name=$ARGV[0] or die ("No cluster name specified");
my $site_name=$ARGV[1] or die ("No site_name specified");


open SLOCK, ">/tmp/$cluster_name.lock" or die ("error opening file /tmp/$cluster_name.lock [$!]\n");

#### exit if die file is found
sub diecheck {	

  my $diecheck = `ls | grep get_slony_logs.$cluster_name.die`;
  if ( $diecheck ne "" ) {
    print "Die file found. Exiting.\n";
	exit;
  }
}

diecheck();

## only one version of this program running
unless (flock(SLOCK, LOCK_EX|LOCK_NB)) {
    print "$0 is already running. Exiting.\n";
    exit(1);
}

#### CONFIGURATION variables
$Net::SCP::scp="/usr/bin/scp";                           ## path to fast scp instance.
my $rmt_hostname="solarport.stanford.edu";                   ## remote machine name
my $rmt_port="55000";                                    ## port to fast scp server in remote machine
my $user="jsocexp";                                      ## user login in remote machine
my $rmt_slony_dir="/data/pgsql/slon_logs/site_logs";     ## directory in remote machine where slony logs get stage
my $slony_logs="/home/production/subscribe_series_5502/slon_logs";           ## local slony cluster directory area
my $PSQL="/usr/bin/psql -Uslony -p 5432 mydb"; 		             ## psql path
my $email_list='';  			                         ## list of people to email in case some problem occurs.
#### END of CONFIGURATION variables
###################################################################################################################
# my $rmt_dir="$rmt_slony_dir/$cluster_name";              ## directory in remote machine
# my $work_dir="$slony_logs/$cluster_name";                ## local working directory
my $rmt_dir="$rmt_slony_dir/$site_name";              ## directory in remote machine
my $work_dir="$slony_logs/$site_name";                ## local working directory
my $counter_file="$work_dir/slony_counter.txt";          ## path to file keeping the slony counter.

sub get_log_list {
  my $file_name_regex=shift;
  my $ls_string="$rmt_dir/$file_name_regex";
  print "$ls_string\n";
  sshopen3("$user\@$rmt_hostname", *WRITER, *READER, *ERROR, "ls -1 $ls_string");

  my $output=undef;
  while(<READER>) {
    $output .= $_ if $_ =~ /\S+/;
  }

  my @list = defined $output? split("\n",$output) :();
  my $error=0;
  while (<ERROR>) {
    next if $_ =~ /No match/;
    if ($_ !~ /^\s*$/) {
      print "ERROR: $_";
      $error=1;
    }
  }

  close(READER);
  close(WRITER);
  close(ERROR);

  if ($error ==1) {
    print "get_log_list failed ... exiting\n";
  }

  return @list;
}

sub save_current_counter {
  my ($counter_file, $log_file) = @_;
  my ($counter)= ($log_file=~/slony1_log_2_0+(\d+).sql/);
  open my $fhW, ">$counter_file" or die "Can't Open $counter_file:$!";
  print $fhW $counter;
  close $fhW;
}

sub ingest_log {
  my ($log, $scp) = @_;

  my ($log_name) = ($log=~/(slony1_log_2_.*sql)$/);
  print "file name = $log_name\n";

  $scp->get($log_name) or die "Cannot get file $log_name ", $scp->{errstr} if defined $scp;


  open SQLOUT, "$PSQL -f $work_dir/$log_name 2>&1 |" or die ("Could not run SQL [$work_dir/$log_name] [$!]");

  while (<SQLOUT>) {
    if ($_ =~ /slony1_log_2_0+(\d+)\.sql:\d+:\s+ERROR:(.*)/) {
      print "SQL out [$_]\n";
      send_error($cluster_name, $1, $2);
      exit;
    }
  }
  close SQLOUT;

  #clean up
  `rm $log_name`;

}

sub send_error {
  my ($cluster_name, $log_file, $error_msg) = @_;

  `echo "$error_msg" | mail -s "ERROR:: Slony log cluster [$cluster_name] [$log_file]" $email_list`;
}

chdir $work_dir;

my $scp = Net::SCP->new($rmt_hostname) or die "Cannot connect to $rmt_hostname: $!";
$scp->login($user);
$scp->cwd($rmt_dir);

## read current counter
#my $counter_file="/home/slony/Scripts/slony_counter.test.txt";

unless ( -f $counter_file ) {

  my $sql_cmd = "\"select at_counter from _$cluster_name.sl_archive_tracking\"";

  my $result = `$PSQL --pset tuples_only --command $sql_cmd`;

  my $counter = $result =~ /(\d+)/?$1:1;

  my $cmd="echo $counter > $counter_file";

  my $status=system($cmd);

  if ($status !=0) {
   die "Could not run [$cmd] [$!]\n";
  }
}

my $slony_ingest=1;

local $/;
open my $fhR, "<$counter_file" or die "Can't Open $counter_file:$!";
my $cur_counter=<$fhR>;
close $fhR;

## move counter to point to the next log
$cur_counter++;
  
print "Start Counter is $cur_counter\n";


while (defined $slony_ingest) {

  $slony_ingest=undef;

  my ($next_batch)=($cur_counter=~/^(\d+)\d\d\d$/?$1:0);
  
  ###################################
  ## START with stand alone sql log files
  
  my $ls_sql_file=sprintf ("slony1_log_2_%017d[0-9][0-9]*.sql",$next_batch);
  my @list = get_log_list($ls_sql_file);
  
  
  #print Dumper [@list];
  my $warnflag=undef;
  for my $log (@list) {
    my ($counter)= ($log=~/slony1_log_2_0+(\d+).sql/);
    if ($counter == $cur_counter) {
      $slony_ingest=1;
      ingest_log($log, $scp);
      save_current_counter($counter_file, $log);
      $cur_counter++;
    } elsif ($counter > $cur_counter && !defined $warnflag) {
  #     send_error($cluster_name, $log, "counter [$counter] greater than current counter [$cur_counter]");
       $warnflag=1; 
    }
  }
  
  ## NOW check tar files ##
  
  my $ls_tar_name="slony*.tar*";
  my @tar_list = get_log_list($ls_tar_name);
  
  for my $tar (@tar_list) {
    next unless ($tar=~/(slony_logs_(\d+)-(\d+).tar(\.gz)?)/); 
    #print $tar, "\n";
    #my ($tar_file, $counter1,$counter2)= ($tar=~/(slony_logs_(\d+)-(\d+).tar(\.gz)?)/);
    my ($tar_file, $counter1,$counter2)= ($1,$2,$3);
    if ($counter2 >= $cur_counter && $counter1 <= $cur_counter) {
      print "Counter1 [$counter1] and Counter2 [$counter2]\n";
      print "Tar file is $tar_file\n";
      # ftp get tar file
      $scp->get($tar, "./$tar_file") or die "error [$!] ", $scp->{errstr};;
      # get list from tar file
      my $tar_test=$tar_file=~/\.gz$/? "tar tfz $tar_file" : "tar tf $tar_file";
      my $list = `$tar_test`;
      if ($? != 0) {
        print "Error executing [$tar_test]\n";
        exit
      }
  
  
      my @list=split ("\n", $list);
  
      # expand tar;
      my $tar_exp=$tar_file=~/\.gz$/? "tar xfz $tar_file": "tar xf $tar_file";
      system($tar_exp);
      if ($? !=0) {
        print "failed to execute tar command [$tar_exp]: $!\n";
        exit;
      }
      for my $log (@list) {
        my ($counter)= ($log=~/slony1_log_2_0+(\d+).sql/);
        if ($counter >= $cur_counter) {
          $slony_ingest=1;
          ingest_log($log);
          save_current_counter($counter_file, $log);
        }
      }
    }
  }
}

close (SLOCK);
__DATA__
