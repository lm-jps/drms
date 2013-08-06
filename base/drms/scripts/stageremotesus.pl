#!/home/jsoc/bin/linux_x86_64/activeperl -w

# This script runs an HTTP client that requests information about SUMS storage units from
# an HTTP server that can provide that information. Once it has the needed information
# it sends a SUMS data transfer request to the remotesums daemon. This script will, by 
# default, NOT wait for the data transfer to complete. However, if this script was 
# started with the NOASYNC flag, then it will block until the request has completed.
# The daemon, which must have been started by a production user, will transfer storage-unit 
# data from the serving SUMS to the local SUMS. 

# This script takes one required argument, despite the handling implementation use of
# GetOptions. There needs to be either a comma-separated list of sunums, a record-set query,
# the path to a file containing a newline- or comma-separated (or both) list of sunums, or a file
# containing a newline- or comma-separated (or both) list of record-set queries. This script
# will convert all these input methods into a set of unique SUNUMs, and then send an HTTP
# request to SUMS to provide information about the SUNUMs. If any SU is offline, the CGI
# program invoked by this script will attempt to bring that SU online.

require HTTP::Request;
use LWP::UserAgent;
use Getopt::Long;
use URI::Escape;

use constant kTmpDir     => "/tmp";
use constant kDefBaseURI => "http://jsoc.stanford.edu/cgi-bin/ajax/jsoc_fetch";
use constant kChunkSize  => 128;

# Remotesums Daemon codes
use constant kDaemonCodeGetSUNUMS    => 1;

# Return codes
use constant kRvSuccess            =>  0;
use constant kRvNoSunums           => -1
use constant kRvDaemonNotRunning   => -2;

my($baseuri) = kDefBaseURI;
my($rsspec);
my($rsspecfile);
my($sunumfile);
my($sunumlst);
my($noasync);

my($status);
my(@sunums);

unless (GetOptions("baseuri=s"    => \$baseuri,    # The part of the URI to the left of the '?'.
                   "rsspec=s"     => \$rsspec,     # A record-set specifiction.
                   "rsspecfile=s" => \$rsspecfile, # A file containing one or more record-set specifications.
                   "sunumsfile=s" => \$sunumfile, # A file containing one or more SUNUMs.
                   "sunums=s"     => \$sunumlst,   # A comma-separated list of SUNUMS.
                   "noasync"      => \$noasync))   # If set, directs the script to block while 
                                                   # SUMS fetches SU information and/or reads from tape.
{
   print STDERR "Bad command-line options @ARGV.\n"
}

# Coalese all SUNUM-speficifying arguments into a single list of SUNUMs. Break into chunks and invoke
# CGI for each chunk.
@sunums = CoalesceSunums($rsspec, $rsspecfile, \$status);

unless ($#sunums >= 0)
{
   print STDOUT "No SUNUMs specified.\n";
   exit(kRvNoSunums);
}

# Check that remotesums daemon is running and bail if not.
unless (DaemonIsRunning())
{
   print STDERR "Remotesums daemon is not running; bailing out...eject, eject!.\n";
   exit(kRvDaemonNotRunning);
}

# Make an HTTP request to the HTTP server.
my($reqstr);
my($req);
my($ua);
my($rsp);
my($content);

$reqstr = "$baseuri?op=exp_su&sunum=$sunumlst&method=url_quick&format=txt&protocol=as-is";

# percent-encode the URI string.
uri_escape($reqstr);

$ua = LWP::UserAgent->new();
$ua->agent("AgentName/0.1 " . $ua->agent);

# Append arguments to URI. Do not use the content() member of HTTP::Request to set the arguments...
# i.e., this doesn't work -> $req->content("op=exp_su&sunum=$sunumlst&method=url_quick&format=txt&protocol=as-is");
$req = HTTP::Request->new(GET => $reqstr);
$req->content_type('application/x-www-form-urlencoded');

# Send the request
$rsp = $ua->request($req);

if ($rsp->is_error)
{
   $content = $rsp->status_line;
   print STDERR "$content\n";
}
else
{
   # $rsp->content has result
   $content = $rsp->decoded_content;
   print "content:$content\n";
}

sub CoalesceSunums
{
   my($rsspec) = $_[0];
   my($rsspecfile) = $_[1];
   my($rvref) = $_[2];

   if (defined($rsspec) && length($rsspec) > 0 && defined($rsspecfile) && length($rsspecfile) > 0)
   {
   
   }
   elsif (defined($rsspec) && length($rsspec) > 0)
   {

   }
   elsif (defined($rsspecfile) && length($rsspecfile) > 0)
   {

   }


   
}



