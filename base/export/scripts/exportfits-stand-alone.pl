#!/usr/bin/env perl

# Use like this:
# exportfits-stand-alone.pl uri=http://jsoc.stanford.edu/cgi-bin/ajax rs=hmi.m_45s'[2014.5.12]' requestor=me address='me@gmail.com' ffmt='hmi.m_45s.{T_REC:A}.{CAMERA}.{segment}'
# Parameters:
# rs - [ Required ] A DRMS record-set query used to specify the desired records (see http://jsoc.stanford.edu/jsocwiki/DrmsNames for more information).
# requestor - [ Required ] A user name (that does not need to be registered with JSOC). Visit http://jsoc.stanford.edu/ajax/register_email.html for information on how to use this to specify a SolarMail user.
# address - [ Required ] An email address registered with JSOC (visit http://jsoc.stanford.edu/ajax/register_email.html to register an email address). Confirmation email messages will be sent to this address (as will messages from the team should there be an issue with an export request).
# uri - [ Optional ] The base URI of the various CGIs (e.g., jsoc_info) used by this script (defaults to http://jsoc.stanford.edu/cgi-bin/ajax).
# ffmt - [ Optional ] A format string used as a template for generating the output files names. This parameter allows the user to control these file names. Visit http://jsoc.stanford.edu/ajax/exportdata.html and click on the Filename Format tooltip for more information.

use strict;
use warnings;
use Data::Dumper;
require HTTP::Request;
use LWP::UserAgent;
use URI::Escape;
use Sys::Hostname;
use CGI;
use JSON -support_by_pp;


{
# Imported from the drmsArgs package file.
package drmsArgs;

use Getopt::Long;
use FileHandle;

# argsinH -  Input: reference to hash array whose keys are the argument names (as specified in GetOptions()), and whose values are the argument type (as specified in GetOptions()).
# argsoutH - Output: reference to hash array whose keys are the argument names (as specified in GetOptions()), and whose values are the argument values as specified on the cmd-line.
sub new
{
    my($clname) = shift;
    my($argsinH) = shift;
    my($req) = shift; # if 1, then these are required args, otherwise optional options.
                      # Defaults to args (req is optional).
    my($err);
    my($rv);
    
    my($self) = 
    {
        _argsinH => undef,
        _argsoutH => {},
        _req => undef
    };
    
    $err = 0;
    bless($self, $clname);
    
    if (defined($argsinH))
    {
        $self->{_argsinH} = $argsinH;
        
        if (!defined($req) || $req != 0)
        {
            $self->{_req} = 1;
        }
        else
        {
            $self->{_req} = 0;
        }
        
        if ($self->{_req} == 1)
        {
            # required args
            $rv = $self->ParseArgs();
        }
        else
        {
            # optional options
            $rv = $self->ParseOpts();
        }
        
        if (!defined($rv) || $rv == 0)
        {
            print STDERR "Invalid or missing cmd-line argument.\n";
            $err = 1;
        }
    }
    else
    {
        print STDERR "Invalid arguments.\n";
        $err = 1;
    }
    
    if ($err)
    {
        $self = undef;
    }
    
    return $self;
}

sub DESTROY
{
    my($self) = shift;
}

# Given a list of expected arguments, ParseArgs() parses the cmd-line and captures the expected-argument values.
# If an expected argument is missing, then ParseArgs() returns 0.
#
# Returns undef on error, 1 on success, and 0 if any argument name does not exist on the cmd-line.
# 
# NOTE: Each cmd-line argument name must match exactly one of the argument names in argsinH. No argument name
# can begin with '-' (GetOptions() considers that an invalid argument name).
sub ParseArgs
{
    my($self) = shift;
    my($argsinH) = $self->{_argsinH};
    my($argsoutH) = $self->{_argsoutH}; 
    
    my($rv) = 0;
    my(@argsA);
    my(@optlist);
    my($iarg);
    my($type);
    my($rsp);
    
    # Create hash array to pass to GetOptions
    @argsA = keys(%$argsinH);
    foreach $iarg (@argsA)
    {
        $type = $argsinH->{$iarg};
        
        if ($type =~ /noval/i)
        {
            push(@optlist, "$iarg");
        }
        else
        {    
            push(@optlist, "$iarg=$type");
        }
    }
    
    # Trick to get GetOptions() to ignore what we'd consider options. If an argument
    # name, from the cmd-line, starts with an optional space, then it matches, and 
    # GetOptions() will consider it an option and process it. It will also accept
    # things that start with '-', but in the @optlist, no argument will start with '-'.
    # If we want this to work for options too, defined as arguments that start with
    # '-', then each option in @optlist should start with '-'.
    # ( |) works for some reason too.
    # pass_through allows the skipping of unknown options (instead of causing GetOptions()
    # to fail.
    Getopt::Long::Configure("prefix_pattern=( ?)", "pass_through");
    
    # Returns undef if any options on cmd-line are not in @optlist. Returns 1 otherwise, even if
    # there is an item in @optlist that is not on the cmd-line.
    $rsp = GetOptions($argsoutH, @optlist);
    
    $rv = $rsp if (defined($rsp));
    
    if ($rv)
    {
        # Now check that all arguments in argsinH were actually specified on the cmd-line.
        my(@check);
        
        @argsA = keys(%$argsinH);
        @check = map ({(!defined($argsoutH->{$_}) ? $_ : ())} @argsA);
        $rv = ($#check < 0);
        
        unless ($rv)
        {
            my($msg) = join(', ', @check);
            print STDERR "Missing required argument(s) '$msg'.\n";
        }
    }
    
    return $rv;
}

sub ParseOpts
{
    my($self) = shift;
    my($argsinH) = $self->{_argsinH};
    my($argsoutH) = $self->{_argsoutH}; 
    
    my($rv) = 0;
    my(@argsA);
    my(@optlist);
    my($iarg);
    my($type);
    my($rsp);
    
    # Create hash array to pass to GetOptions
    @argsA = keys(%$argsinH);
    foreach $iarg (@argsA)
    {
        $type = $argsinH->{$iarg}; 
        if ($type =~ /noval/i)
        {
            push(@optlist, "$iarg");
        }
        else
        {
            push(@optlist, "$iarg=$type");            
        }
    }
    
    # A previous Configure call could mess everything up. Just always precede GetOptions()
    # with the proper configuration.
    Getopt::Long::Configure("default");
    Getopt::Long::Configure("pass_through");
    
    $rsp = GetOptions($argsoutH, @optlist);
    
    $rv = $rsp if (defined($rsp));
    
    return $rv;
}

# Returns argument value (as string)
sub Get
{
    my($self) = shift;
    my($name) = shift;
    my($rv);
    
    if (exists($self->{_argsoutH}->{$name}))
    {
        return $self->{_argsoutH}->{$name};
    }
    else
    {
        return undef;
    }
}
}


# Argument names
use constant kArgRsSpec    => "rs";
use constant kArgRequestor => "requestor";
use constant kArgAddress   => "address";
use constant kOptBaseURI   => "uri";
use constant kOptFnameFmt  => "ffmt";

# Default argument values
use constant kDefBaseURI   => "http://jsoc.stanford.edu/cgi-bin/ajax";

# Other constants
use constant kTimeOut      => 300; # seconds

# Return values
use constant kRetSuccess       => 0;
use constant kRetInvalidArgs   => 1;
use constant kRetHTTPreqFailed => 2;
use constant kRetExpReqFailed  => 3;
use constant kRetRsSummFailed  => 4;
use constant kRetNoRecords     => 5;

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
    &kArgRequestor => 's',
    &kArgAddress   => 's'
};

$optsinH =
{
    &kOptBaseURI   => 's',
    &kOptFnameFmt  => 's'
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
    my($address);
    my($fnamefmt);
    my($content);
    my($reqid);
    my($json) = JSON->new->utf8;
    my($txt);
    my($expstat);
    my($errmsg);
    
    # Use the base URI provided on the command-line (default to &kDefBaseURI).
    $opt = $opts->Get(&kOptBaseURI);
    $baseuri = defined($opt) ? $opt : &kDefBaseURI;
    
    # Use the export file-name format provided on the command-line (default to providing no
    # file-name format, which will default to whatever jsoc_fetch does).
    $opt = $opts->Get(&kOptFnameFmt);
    $fnamefmt = defined($opt) ?  "&filenamefmt=$opt" : "";

    # Fetch, from the command-line, the record-set specification that identifies the desired records.
    # This record-set-specification argument is required.
    $rsspec = $args->Get(&kArgRsSpec);
    $requestor = $args->Get(&kArgRequestor);
    $address = $args->Get(&kArgAddress);
    
    print "CGI root: $baseuri\n";
    
    # First, check to see if user properly specified at least one record.
    $uri = "$baseuri/jsoc_info?op=rs_summary&ds=$rsspec";
    if (!HTTPget($uri, \$content))
    {
        $txt = $json->decode($content);
        $expstat = $txt->{status};
        $errmsg = $txt->{error};
        
        if (defined($errmsg))
        {
            chomp($errmsg);
        }
        
        if ($expstat == 0)
        {
            my($count) = $txt->{count};
            
            if ($count == 0)
            {
                # No records, bail.
                print STDERR "Record-set specification identifies no records.\n";
                $rv = &kRetNoRecords;
            }
        }
        else
        {
            print STDERR "rs_summary failure, status $expstat: \"$errmsg\".\n";
            $rv = &kRetRsSummFailed;
        }
    }
    else
    {
        print STDERR "HTTP GET request failure.\n";
        $rv = &kRetHTTPreqFailed;
    }
    
    if ($rv == &kRetSuccess)
    {
        $uri = "$baseuri/jsoc_fetch?op=exp_request&ds=$rsspec&protocol=fits$fnamefmt&format=json&method=url&requestor=$requestor&notify=$address";
        
        # Send an aynchronous HTTP GET request to the server identified in $baseuri.
        if (!HTTPget($uri, \$content))
        {
            my($reqid);
            
            # The HTTP request will return JSON content. Parse that with one of Perl's built-in JSON parsers.
            $txt = $json->decode($content);
            
            # Extract the export-request status.
            $expstat = $txt->{status};
            $errmsg = $txt->{error};
            
            if (defined($errmsg))
            {
                chomp($errmsg);
            }
            
            if ($expstat == 1 || $expstat == 2)
            {
                # Extract the export-request request ID. We'll need this to check on the status of the request.
                $reqid = $txt->{requestid};
                
                # If the request succeeded, poll with a second request until we get confirmation that the 
                # export request has completed.
                $uri = "$baseuri/jsoc_fetch?op=exp_status&requestid=$reqid";
                while (1)
                {
                    if (!HTTPget($uri, \$content))
                    {
                        $txt = $json->decode($content);
                        $expstat = $txt->{status};
                        $errmsg = $txt->{error};
                        
                        if (defined($errmsg))
                        {
                            chomp($errmsg);
                        }
                        
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
                            print STDERR "exp_request failure (2), status $expstat: \"$errmsg\".\n";
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
                # Parse response to obtain the error message
                print STDERR "exp_request failure (1), status $expstat: \"$errmsg\".\n";
                $rv = &kRetExpReqFailed;
            }
        }
        else
        {
            print STDERR "HTTP GET request failure.\n";
            $rv = &kRetHTTPreqFailed;
        }
    }
}

exit($rv);

sub HTTPget
{
    my($uri) = shift;
    my($content) = $_[0];
    
    my($base);
    my($args);
    my($ua);
    my($req);
    my($rsp);
    my($rv);
    
    # percent-encode the URI string.
    $uri = URIescape($uri);
    
    if (length($uri) > 0)
    {
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
    }
    else
    {
        $rv = 1;
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
    
    if ($rspjson->{count} > 0)
    {
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
    else
    {
        print "The requested FITS files are not present, check the record-set specification.\n";
    }
}

sub URIescape
{
    my($uri) = shift;
    my($rv);
    my($base);
    my($args);
    my($arg);
    my($argname);
    my($argval);
    
    $rv = "";
    
    if ($uri =~ /^([^?]+)\?(.+)/)
    {
        $base = $1;
        $args = $2;
        
        $rv = "$base?";
        $arg = $args;
        
        while ($arg =~ /([^=]+)=([^&]+)&(.+)/)
        {
            $rv = $rv . $1 . "=" . CGI::escape($2) . "&";
            $arg = $3;
        }
        
        if ($arg =~ /(.+)=(.+)/)
        {
            $rv = $rv . $1 . "=" . CGI::escape($2);
        }
    }
    else
    {
        print STDERR "Invalid URI $uri\n";
    }
    
    return $rv;
}
