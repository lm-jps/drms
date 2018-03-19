#!/usr/bin/perl --

# was 'jsoc.cgi' renamed to 'drms_export.cgi'

# 2010/06/23 : joe : new param -- compress (can set to 'rice', defaults to 'none')
# 2010/07/28 : joe : use 20 '**NONE**' in cparm to get around bug; use local instance name for ID

$|=1;
use lib '/opt/vso/lib/perl5';

use strict;
use warnings;

use CGI qw(:standard :cgi-lib escapeHTML);
use IO::Handle;
use IPC::Open2;
use Data::Dumper;

use Physics::Solar::VSO::DataProvider::JSOC::Registry;
use Physics::Solar::VSO::Config;

my($DUMMY_TAR_FILE_NAME) = 'data.tar';

# move this to VSO::Config ?
# (or something lighter weight?)
# actually, JSOC::Registry uses VSO::Config, so probably doesn't matter

my $config     ||= Physics::Solar::VSO::Config->new();
my $export_cmd ||= $config->{'JSOC_EXPORT_CMD'} || '/opt/netdrms/bin/linux_x86_64/drms_export_cgi';
my $instance   ||= $config->{'INSTANCE_ID'} || $config->{'SERVER_ID'} || 'VSO';
my($expProgType) ||= $config->{'JSOC_EXPORT_CMD_TYPE'} || 'streamableExport';

my @file_handles = ();

### clean up
$SIG{'TERM'}= sub {
    map {close ($_) } @file_handles;
};

my $cgi = new CGI;

my $offset=0;
my $http_range=undef;
if (defined($cgi->http("HTTP_RANGE"))) {
  $http_range = $cgi->http("HTTP_RANGE");
  if ($http_range =~/^bytes=(\d+)\-/) {
    $offset = $1;
    print STDERR "HTTP OFFSET is [$offset]\n";
    print STDERR "HTTP RANGE [$http_range]\n";
  } else {
    print STDERR "INVALID HTTP RANGE [$http_range]\n";
  }
}

my $registry = new Physics::Solar::VSO::DataProvider::JSOC::Registry;


my $series_param = $cgi->param("series")
	or abort ( 'No series specified' );

my $compression = ( $cgi->param("compress") eq 'rice' ) ? 'rice' :  'none';


my %series_info = $registry->GetData_info( $series_param );
abort ( 'Unknown series' ) if ! scalar %series_info;

my $archive_pattern  = $series_info{'getdata_archivename'} || abort ( 'Error Processing Series' );
my $argument_regex   = $series_info{'getdata_arg_regex'}   || abort ( 'Error Processing Series' );
my $drms_query       = $series_info{'getdata_drms_query'}  || abort ( 'Error Processing Series' );
my $drms_series      = $series_info{'drms_series'}         || abort ( 'Error Processing Series' );
my $filename_pattern = $series_info{'getdata_filename'}    || abort ( 'Error Processing Series' );

if ($drms_series =~ /aia_test.synoptic2/) {

 my $message = shift;
 print $cgi->header,
        $cgi->start_html("Data not available"),
        $cgi->h1("The data you are requesting series [$drms_series] is no longer available, please contact " . 'help@virtualsolar.org' . " for help."),
        $cgi->end_html();
        exit();
}

my @series_args = ();
print STDERR "series_param=$series_param\n";

my $args_param = $cgi->param('record');
if ( !$args_param ) {
  abort('No record specified');
}

my $regex = qr/$argument_regex/;
if ( ! $regex ) {
  abort ("Internal Error : Error processing record pattern");
}

my $tarfile = undef;

# pass the cart ID?
my $request_num = int(rand(1000000));
my $request_id = $instance.'_'.$request_num;


unless (@series_args = ($args_param =~ m/$regex/ )) {
  if ( defined($series_info{'getdata_group_regex'}) ) {
    my $group_regex     = $series_info{'getdata_group_regex'}  || abort ( 'Error Processing Series' );
       $drms_query      = $series_info{'getdata_group_query'}  || abort ( 'Error Processing Series' );
    my $group_args      = $series_info{'getdata_group_arg_count'};
    @series_args = ();

    my (@args) = ( $args_param =~ m/$group_regex/ );
    if (@args != (1+$group_args) ) {
      abort ( 'Error Processing Series' );
    }
    if ( $group_args ) {
      push @series_args, (shift @args)
        for 1..$group_args;
    }
    my @date = localtime();
    my $date = sprintf( '%04i%02i%02i', 1900+$date[5], 1+$date[4], $date[3] );

    $tarfile = sprintf($archive_pattern, @series_args, $date, $request_num);

    push @series_args, ( join ',', map { "'$_'" } ( split ':', $args[0] ) );



  } else { # we don't support group selections
    abort('Invalid Arguments');
  }
}


# removed -- we sanitize the series name for the filename,
# but not the query, so just pass it into the query, and
# hard code it in the filename
# ... or, we just hard code the drms query, too.
## make sure that @series_args starts with the series name
# unshift @series_args, $drms_series;

# filename DOES NOT end in '.tar' so we can change to
# .zip or similar more easily
$tarfile ||= sprintf($archive_pattern, @series_args);


# is 'ffmt' something that we need to change per series?

my(@cmd);

# choose the "export program" - the program that reads the metadata from the DRMS DB and the FITS files from SUMS and makes
# stand-alone FITS files from these inputs
if ($expProgType eq 'export-cgi')
{
    # use drms_export_cgi - this program was forked off of jsoc_export_as_fits and was designed to support the streaming
    # of TAR files to the VSO users' machine; this feature was abandoned due to the imposition of high load on the 
    # VSO server, but you can still use this program, albeit without the tar-streaming feature
    @cmd = ( $export_cmd, 'rsquery='.sprintf($drms_query, @series_args), 'reqid='.$request_id,  'ackfile=/tmp/ACKFILE'.$request_id, qw( expversion=0.5  method=url_cgi protocol=FITS path=jsoc), "ffmt=$filename_pattern", "tarfile=$tarfile", "cgi_request=".$ENV{'REQUEST_METHOD'} );
    
    if ( $compression ne 'rice' ) 
    {
	    push @cmd, "cparms=**NONE**,**NONE**,**NONE**,**NONE**,**NONE**,**NONE**,**NONE**,**NONE**,**NONE**,**NONE**,**NONE**,**NONE**,**NONE**,**NONE**,**NONE**,**NONE**,**NONE**,**NONE**,**NONE**,**NONE**"
    }
    
    # need to key this into a valid temp dir
    if ( ! -d '/tmp/jsoc' ) 
    {
        mkdir '/tmp/jsoc' or
        abort('Internal error : temp directory initialization');
    }
    
    print STDERR "@cmd\n";
    # eval { 
    my ($read_fh, $wrt_fh);
    my $pid = open2($read_fh, $wrt_fh, @cmd);

    unless ( defined $read_fh && defined $wrt_fh) {
    abort ('Unable to generate tarball');
    }

    binmode(STDOUT);
    binmode($read_fh);

    #   use POSIX ":sys_wait_h";
    #   waitpid $pid, WNOHANG;

    my $error = ( $? << 8 );
    if ($error) { exit $error }

    while (<$read_fh>) 
    {
        # 
        print STDOUT $_;
    }

}
elsif ($expProgType eq 'export-stdout')
{
    my($makeTarFile);
    my($dbHost) ||= $config->{'JSOC_DB_HOST'} || 'unknownhost';
    my($dbName) ||= $config->{'JSOC_DB_NAME'} || 'data';
    my($dbUser) ||= $config->{'JSOC_DB_USER'} || 'apache';
    # DB PASSWORD IS IN .pgpass

    # use drms-export-to-stdout
    # this program dumps a tar file directly to stdout; it makes an effort to always write some kind of tar file; 
    # should there be problems putting FITS files in the tar file, it will put errors in the jsoc/error_list.txt
    # file in the tar; if a tar could not be created 
    
    $makeTarFile = ((defined($tarfile) && length($tarfile) > 0) ? 1 : 0);
    
    @cmd = ($export_cmd, 'spec='.sprintf($drms_query, @series_args), "ffmt=$filename_pattern", 'ackfile=/tmp/ACKFILE');
    if (!$makeTarFile)
    {
        push @cmd, 's=1';
    }
    push @cmd, ('JSOC_DB_HOST=' . $dbHost, 'JSOC_DB_NAME=' . $dbName, 'JSOC_DB_USER=' . $dbUser);
    
    if ($compression ne 'rice')
    {
        push @cmd, "cparms=none";
        push @cmd, "a=1"; # apply 'none' to all segments
    }
    
    my($read_fh, $wrt_fh);
    my($pid);

    $pid = open2($read_fh, $wrt_fh, @cmd);

    unless (defined $read_fh && defined $wrt_fh)
    {
        abort('unable to generate tarball');
    }

    binmode(STDOUT);
    binmode($read_fh);

    my($error);
    my($partial);
    my($errorBlock);
    my($errMsg);
    my($htmlHeaderPrinted);
    
    $error = ($? << 8);
    if ($error)
    {
        # if $error, then there should be a general error message to display - look for the file, in the tar named "jsoc/error.txt";
        # there could still be a tar file (and there should be) on STDOUT
        $errorBlock = "";
        $partial = undef;
    }

    $htmlHeaderPrinted = 0; # false
    while (<$read_fh>) 
    {
        if ($error)
        {
            if (!defined($partial))
            {
                ($partial) = ($_ =~ m#(jsoc/error.txt.*)#);
            }
            
            $errorBlock = $errorBlock . $partial;
        }

        if (!$htmlHeaderPrinted)
        {
            my($tarFileName);
            
            if (!defined($tarfile) || length($tarfile) == 0)
            {
                $tarFileName = sprintf($drms_query, @series_args);
                if (length($tarFileName) == 0)
                {
                    $tarFileName = $DUMMY_TAR_FILE_NAME;
                }
            }
            else
            {
                $tarFileName = $tarfile;
            }

            # force alphanumeric (plus '_')
            $tarFileName =~ s/[^a-zA-Z0-9]/_/g;
            $tarFileName = $tarFileName . ".tar";
        
            print STDOUT "Content-type: application/octet-stream\n";
            print STDOUT 'Content-Disposition: attachment; filename="' . $tarFileName . '"\n';
            print STDOUT "Content-transfer-encoding: binary\n\n";
            $htmlHeaderPrinted = 1;
        }

        print STDOUT $_;
    }
    
    # not sure what to do with the error message?
    $errorMsg = substr($errorBlock, -512);
}
else
{
    abort('Invalid Arguments: invalid export program type ' . $expProgType);
}



##  print STDOUT <$read_fh>;

  # do we want to die this late?
  close ($read_fh) || die "close: $!";
  close ($wrt_fh) || die "close: $!";
# };
# if ( $@ ) {
#   abort ( 'Error generating tarball' );
# }


sub abort {
 my $message = shift;
 print $cgi->header,
        $cgi->start_html("ERROR PROCESSING REQUEST"),
        $cgi->h1("Error Processing Request"),
        $cgi->h2($message),
        $cgi->end_html();
 use Carp;
 carp ( 'drms_export : ABORTING : ', $message );
 die;
}
