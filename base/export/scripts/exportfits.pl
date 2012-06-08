#!/home/jsoc/bin/linux_x86_64/perl5.12.2

use strict;
use warnings;
use Data::Dumper;
require HTTP::Request;
use LWP::UserAgent;
use URI::Escape;
use Sys::Hostname;
use JSON -support_by_pp;
use FindBin qw($Bin);
use lib "$Bin/../../../base/libs/perl";
use drmsLocks;
use drmsArgs;

# Argument names
use constant kArgRsSpec    => "rs";
use constant kArgRequestor => "requestor";
use constant kOptBaseURI   => "uri";
use constant kOptFnameFmt  => "ffmt";

# Default argument values
use constant kDefBaseURI   => "http://jsoc.stanford.edu/cgi-bin/ajax/jsoc_fetch";

# Other constants
use constant kTimeOut      => 300; # seconds

# Return values
use constant kRetSuccess       => 0;
use constant kRetInvalidArgs   => 1;
use constant kRetHTTPreqFailed => 2;
use constant kRetExpReqFailed  => 3;

my($argsinH);
my($optsinH);
my($opt);
my($opts);
my($arg);
my($args);
my($baseuri) = kDefBaseURI;
my($rsspec);
my($uri);
my($rv);

# Required arguments
$argsinH =
{
    &kArgRsSpec    => 's',  
};

$optsinH =
{
    &kOptBaseURI   => 's',
    &kOptFnameFmt  => 's',
    &kArgRequestor => 's'
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
    my($requestor);
    my($fnamefmt);
    my($content);
    my($reqid);
    
    # Use the base URI provided on the command-line (default to &kDefBaseURI).
    $opt = $opts->Get(&kOptBaseURI);
    $baseuri = defined($opt) ? $opt : &kDefBaseURI;
    
    # Use the requestor's name provided on the command-line (default to hostname).
    $opt = $opts->Get(&kArgRequestor);
    $requestor = defined($opt) ? $opt : hostname();
    
    # Use the export file-name format provided on the command-line (default to providing no
    # file-name format, which will default to whatever jsoc_fetch does).
    $opt = $opts->Get(&kOptFnameFmt);
    $fnamefmt = defined($opt) ?  "&filenamefmt=$opt" : "";

    # Fetch, from the command-line, the record-set specification that identifies the desired records.
    # This record-set-specification argument is required.
    $rsspec = $args->Get(&kArgRsSpec);
    
    $uri = "$baseuri?op=exp_request&ds=$rsspec&protocol=fits$fnamefmt&format=json&method=url&requestor=$requestor";
    
    # Send an aynchronous HTTP GET request to the server identified in $baseuri.
    if (!HTTPget($uri, \$content))
    {
        my($json) = JSON->new->utf8;
        my($txt);
        my($expstat);
        my($reqid);
        
        # The HTTP request will return JSON content. Parse that with one of Perl's built-in JSON parsers.
        $txt = $json->decode($content);
        
        # Extract the export-request status.
        $expstat = $txt->{status};
        
        if ($expstat == 1 || $expstat == 2)
        {
            # Extract the export-request request ID. We'll need this to check on the status of the request.
            $reqid = $txt->{requestid};
            
            # If the request succeeded, poll with a second request until we get confirmation that the 
            # export request has completed.
            $uri = "$baseuri?op=exp_status&requestid=$reqid";
            while (1)
            {
                if (!HTTPget($uri, \$content))
                {
                    $txt = $json->decode($content);
                    $expstat = $txt->{status};
                    if ($expstat == 6 || $expstat == 2 || $expstat == 1)
                    {
                        # The request hasn't completed yet.
                        print STDERR "Request not complete (status $expstat), trying again in 1 second\n";
                        sleep(1);
                        next;
                    }
                    elsif ($expstat == 0)
                    {
                        # Export complete!
                        print STDERR "Export complete!\n";
                        PrintResults($baseuri, $txt);
                        last;
                    }
                    else
                    {
                        # The request errored-out.
                        print STDERR "exp_request failure (2), status $expstat.\n";
                        $rv = &kRetExpReqFailed;
                    }
                }
                else
                {
                    print STDERR "HTTP GET request failure.\n";
                    $rv = &kRetHTTPreqFailed;
                    last;
                }
            }
        }
        else
        {
            print STDERR "exp_request failure (1), status $expstat.\n";
            $rv = &kRetExpReqFailed;
        }
    }
    else
    {
        print STDERR "HTTP GET request failure.\n";
        $rv = &kRetHTTPreqFailed;
    }
}

exit($rv);

sub HTTPget
{
    my($uri) = shift;
    my($content) = $_[0];
    
    my($ua);
    my($req);
    my($rsp);
    my($rv);
    
    # percent-encode the URI string.
    uri_escape($uri);
    
    $ua = LWP::UserAgent->new();
    $ua->agent("AgentName/0.1 " . $ua->agent);
    
    # Append arguments to URI. Do not use the content() member of HTTP::Request to set the arguments...
    # i.e., this doesn't work -> $req->content("op=exp_su&sunum=$sunumlst&method=url_quick&format=txt&protocol=as-is");
    $req = HTTP::Request->new(GET => $uri);
    $req->content_type('application/x-www-form-urlencoded');
    
    # Send the request
    $rsp = $ua->request($req);
    
    if ($rsp->is_error)
    {
        $$content = $rsp->status_line;
        print STDERR "$$content\n";
        $rv = 1;
    }
    else
    {
        # $rsp->content has result
        $$content = $rsp->decoded_content;
        $rv = 0;
    }

    return $rv;
}

sub PrintResults
{
    my($baseuri) = shift;
    my($rspjson) = shift;

    my($dir);
    my($record);
    my($filename);
    my($root);
    
    if ($baseuri =~ /\s*(http:\/\/[^\/]+)\//)
    {
        $root = $1;
    }   
    
    print "URLs of requested data files:\n";
    $dir = $rspjson->{dir};
    
    foreach my $datum (@{$rspjson->{data}})
    {
        $record = $datum->{record};
        $filename = $datum->{filename};
        
        print "$record\t$root$dir/$filename\n";
    }
}
