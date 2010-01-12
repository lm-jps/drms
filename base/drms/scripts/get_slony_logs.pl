#!/usr/bin/perl -w

## 
##  2009/05/05  :: igor    : Make sure one script is running per cluster name
##                         : Keep going to the slony source directory till all
##                         : log files are dealt with.
##  2009/07/10  :: igor    : Default slony counter from database
##  2009/08/17  :: igor    : Fix ssh port number bug.
##  2009/12/21  :: kevin   : Added diecheck routine 
##  2009/12/22  :: kevin   : Read config variables from subscribe_series/etc
##  2010/01/11  :: arta    : Add the Net::SCP put subroutine and use it to save counter file on server
##
use lib qw(/home/slony/Scripts);


use Net::FTP;
use Data::Dumper;
use Net::SSH qw(ssh sshopen3);
use Fcntl ':flock';

use strict;

my $cluster_name=();
my $site_name=();
my $config_file=$ARGV[0] or die ("No config_file specified");

#### CONFIGURATION variables
my $ssh_cmd="";                       ## path to fast ssh instance
my $scp_cmd="";                       ## path to fast scp instance
my $ssh_rmt_port="";                         ## port to fast scp server in remote machine
my $rmt_hostname="";               ## remote machine name
my $user="";                                  ## user login in remote machine
my $rmt_slony_dir="";   ## directory in remote machine where slony logs get stage
my $slony_logs="";                ## local slony cluster directory area
my $PSQL=""; 		             ## psqahy$mi_it'';                   ## list of people to email in case some problem occurs.
my @line=();
my $email_list="";
my $pg_user="";
my $pg_dbname="";
my $ingestion_path="";
#### END of CONFIGURATION variables
###################################################################################################################

#### Get CONFIGURATION variables from $config_file
if (!open(SETTINGS, "$config_file") ) {
    my $error = "Error cannot open config file [$config_file]\n";
    print $error;
    exit 1;
}

while (<SETTINGS>) {
    chomp;
    # ignore everything behind a pound sign (inclusive)
    $_ =~ s/\#.+//;
    if ($_ =~ /^#/) {
        next;
    }
    # Ignore blank lines
    if (length($_) == 0 ) {
        next;
    }
    # Creates new array @lilne with line[0] as setting name and line[1] as setting value
    @line = split /=/;
    # Strip white spaces
    $line[0] =~ s/^\s+//;
    $line[0] =~ s/\s+$//;
    $line[1] =~ s/^\s+//;
    $line[1] =~ s/\s+$//;
    $line[1] =~ s/\'//g;

        if ( $line[0] eq "scp_cmd" ) {
                $scp_cmd=$line[1];
        } elsif ( $line[0] eq "ssh_cmd" ) {
                $ssh_cmd=$line[1];
        } elsif ( $line[0] eq "rmt_hostname" ) {
                $rmt_hostname=$line[1];
        } elsif ( $line[0] eq "ssh_rmt_port" ) {
                $ssh_rmt_port=$line[1];
        } elsif ( $line[0] eq "rmt_user" ) {
                $user=$line[1];
        } elsif ( $line[0] eq "rmt_slony_dir" ) {
                $rmt_slony_dir=$line[1];
        } elsif ( $line[0] eq "slony_logs" ) {
                $slony_logs=$line[1];
        } elsif ( $line[0] eq "PSQL" ) {
                $PSQL=$line[1];
        } elsif ( $line[0] eq "slony_cluster" ) {
                $cluster_name=$line[1];
        } elsif ( $line[0] eq "node" ) {
                $site_name=$line[1];
        } elsif ( $line[0] eq "pg_user" ) {
                $pg_user=$line[1];
        } elsif ( $line[0] eq "pg_dbname" ) {
                $pg_dbname=$line[1];
        } elsif ( $line[0] eq "ingestion_path" ) {
                $ingestion_path=$line[1];
        } elsif ( $line[0] eq "email_list" ) {
                $email_list=$line[1];
        }

}
close(SETTINGS);

open SLOCK, ">/tmp/$cluster_name.lock" or die ("error opening file /tmp/$cluster_name.lock [$!]\n");

## only one version of this program running
unless (flock(SLOCK, LOCK_EX|LOCK_NB)) {
    print "$0 is already running. Exiting.\n";
    exit(1);
}



## build PSQL command
$PSQL=$PSQL." -U".$pg_user." ".$pg_dbname;
print "PSQL = [$PSQL]\n";

$Net::SSH::ssh=$ssh_cmd;
$ssh_rmt_port= (defined $ssh_rmt_port && $ssh_rmt_port=~/^\d+$/)
               ? $ssh_rmt_port
               : 22;
@Net::SSH::ssh_options= ("-p", $ssh_rmt_port,  "-o BatchMode=yes");

my $rmt_dir="$rmt_slony_dir/$site_name";              ## directory in remote machine
my $work_dir="$slony_logs/$site_name";                ## local working directory
my $counter_file="$work_dir/slony_counter.txt";          ## path to file keeping the slony counter.

#### exit if die file is found
sub diecheck {
  my ($cluster_name, $ingestion_path) = @_;	
  
  die_error ($cluster_name, "Config variable ingestion_path not set or doesn't exists") unless (defined $ingestion_path && -d $ingestion_path);
  my $diecheck = `ls $ingestion_path | grep get_slony_logs.$cluster_name.die`;
  if ( $diecheck ne "" ) {
    print "Die file found. Exiting.\n";
	exit;
  }
}

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

  if (defined $email_list && $email_list !~ /^$/) {
    `echo "$error_msg" | mail -s "ERROR:: Slony log cluster [$cluster_name] [$log_file]" $email_list`;
  }
}

sub die_error {
  my ($cluster_name, $error_msg) = @_;

  if (defined $email_list && $email_list !~ /^$/) {
    `echo "$error_msg" | mail -s "ERROR:: Slony log cluster [$cluster_name]" $email_list`;
  }
  die ($error_msg);
}

chdir $work_dir;

my $vscp = VSO::Net::SCP->new($rmt_hostname) or die "Cannot connect to $rmt_hostname: $!";
$vscp->scp_cmd($scp_cmd);
$vscp->scp_port($ssh_rmt_port);
$vscp->login($user);
$vscp->cwd($rmt_dir);

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


#### MAIN ####


#### exit if die file is found
diecheck($cluster_name, $ingestion_path);

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


  ## next batch is measure in thousands ...
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
      ingest_log($log, $vscp);
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
      $vscp->get($tar, "./$tar_file") or die "error [$!] ", $vscp->{errstr};;
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
          $cur_counter = $counter + 1;
          save_current_counter($counter_file, $log);
        }
      }
    }
  }

  # Art: Now push the counter file back to the server so that manage_logs.pl can use it
  # If I understand things right, the first arg is the local file, and the second
  # is the remote file.  And the remote file will be put into the
  # remote-site dir (which is where we want it to go).
  if (-f $counter_file)
  {
     $vscp->put($counter_file, "slony_counter.txt") or die "error [$!] ", $vscp->{errstr};
  }
}

close (SLOCK);

######################
######################
package VSO::Net::SCP;


## Copied from Net::SCP

use strict;
use vars qw($VERSION $DEBUG);
use Exporter;
use Carp;
use File::Basename;
use String::ShellQuote;
use IO::Handle;
use IPC::Open3;
use Data::Dumper;

$VERSION = '0.08';

our $scp = "scp";

$DEBUG = 0;


sub scp {
  my $self = ref($_[0]) ? shift : {};
  my($src, $dest, $interact) = @_;
  if (exists $self->{scp_cmd} and defined $self->{scp_cmd}) {
    $scp=$self->{scp_cmd};
  }
  my $flags .= '-p';
  $flags .= 'r' unless &_islocal($src) && ! -d $src;
  my @cmd;
  if ( ( defined($interact) && $interact )
       || ( defined($self->{interactive}) && $self->{interactive} ) ) {
    @cmd = ( $scp, $flags, $src, $dest );
    print join(' ', @cmd), "\n";
    unless ( &_yesno ) {
      $self->{errstr} = "User declined";
      return 0;
    }
  } else {
    $flags .= 'qB';
    $flags .= " -P " . $self->{scp_port} if exists $self->{scp_port} and defined $self->{scp_port};
    @cmd = ( $scp, $flags, $src, $dest );
  }
  my($reader, $writer, $error ) =
    ( new IO::Handle, new IO::Handle, new IO::Handle );
  $writer->autoflush(1);#  $error->autoflush(1);
  local $SIG{CHLD} = 'DEFAULT';
  my $pid = open3($writer, $reader, $error, join (" ", @cmd) );
  waitpid $pid, 0;
  if ( $? >> 8 ) {
    my $errstr = join('', <$error>);
    #chomp(my $errstr = <$error>);
    $self->{errstr} = $errstr;
    0;
  } else {
    1;
  }
}



sub _islocal {
  shift !~ /^[^:]+:/
}

sub scp_cmd {
  my ($self,$cmd)=@_;
  $self->{scp_cmd} = $cmd;
}

sub scp_port {
  my ($self,$value)=@_;
  $self->{scp_port} = $value;
}

sub new {
  my $proto = shift;
  my $class = ref($proto) || $proto;
  my $self;
  if ( ref($_[0]) ) {
    $self = shift;
  } else {
    $self = {
              'host'        => shift,
              'user'        => ( scalar(@_) ? shift : '' ),
              'interactive' => 0,
              'cwd'         => '',
            };
  }
  bless($self, $class);
}


sub login {
  my($self, $user) = @_;
  $self->{'user'} = $user if $user;
}


sub cwd {
  my($self, $cwd) = @_;
  $self->{'cwd'} = $cwd || '/';
}


sub get {
  my($self, $remote, $local) = @_;
  $remote = $self->{'cwd'}. "/$remote" if $self->{'cwd'} && $remote !~ /^\//;
  $local ||= basename($remote);
  my $source = $self->{'host'}. ":$remote";
  $source = $self->{'user'}. '@'. $source if $self->{'user'};
  $self->scp($source,$local);
}

# Art: added for copying counter file back to server
sub put {
  my($self, $local, $remote) = @_;
  $remote ||= basename($local);
  $remote = $self->{'cwd'}. "/$remote" if $self->{'cwd'} && $remote !~ /^\//;
  my $dest = $self->{'host'}. ":$remote";
  $dest = $self->{'user'}. '@'. $dest if $self->{'user'};
  warn "scp $local $dest\n" if $DEBUG;
  $self->scp($local, $dest);
}

sub binary { 1; }

sub quit { 1; }

__DATA__
