#!/usr/bin/perl -w 

# This script performs the 


# The cmd takes the following form:
#   remotesums_master.pl <url> '=' <sunum> ',' <sunum...> '#' <url> '=' <sunum> ',' <sunum...> # ...
#   Example:
#     remotesums_master.pl http://jsoc.stanford.edu/cgi-bin/ajax/jsoc_fetch=series1{1234567,1234568}&series2{1234569}#http://jsoc.stanford.edu/cgi-bin/ajax/jsoc_fetch2=series1{7654321,7654320}

# When calling this script from DRMS, print error messages to STDERR - STDOUT is redirected
# to a pipe back to a parent process in DRMS.

# The proper way to set up ssh-agent for use to obtain files from Stanford:
#   First, create passphrased private (id_rsa)/public keys (id_rsa.pub) and place the 
#   public key content in j0.stanford.edu's authorized_keys file. Then:
#     ssh-agent -c > ~/.ssh-agent_rs
#     source ~/.ssh-agent_rs
#     ssh-add ~/.ssh/id_rsa (enter passphrase)
#

use CGI;

# Global defines
my($kRSERROR) = "-1";
my($kTRYLATER) = "0";
my($kTRYAGAIN) = "1";
my($kMETHOD) = "url_quick";
my($kPROTO) = "as-is";
my($kOP) = "exp_su";
my($kEXPSTATUS) = "exp_status";
my($kGETAPP) = "wget";
my($kGETAPPFLAG) = "-nv";
my($kGETAPPOUT) = "/tmp/jsoc_export.$$";
my($kGETAPPOFLAG) = "-O $kGETAPPOUT";
my($kSIZECUTOFF) = 134217728; # 128MB
my($kRSINGEST) = "remotesums_ingest";
my($kAGENTFILE) = "$ENV{'HOME'}/.ssh-agent_rs";

my($expURL);
my($list);
my(%sulists);
my($sunum);
my($arg);
my($subrequest);
my($resp);

my(@urls);
my(@lists);
my($aurl);
my($series);
my($alist);
my($sublist);
my($sunumlist);
my($escURL);

my($cgiargs);
my($cmd);
my($line);

# print STDERR "$ARGV[0]\n";

# Get arguments
if (scalar(@ARGV) != 1)
{
    print STDERR "A single argument, a list of SUNUMs, is expected.\n";
    exit(1);
}

$arg = $ARGV[0];

while (defined($subrequest = GetToken($arg, "\#")))
{
    if ($subrequest =~ /(.+)=(.+)/)
    {
        $expURL = $1;
        $list = $2;

        # print STDERR "sunums for url $expURL: $list\n";

        $sulists{$expURL} = $list;
    }
}

# Evaluate the source site's export URL
@urls = keys %sulists;
@lists = values %sulists;

my(@reqsunums);
my(@reqseries);
my(@reqfiles);
my($status);
my($method);
my($requestid);
my($count);
my($size);
my($gotstatus) = 0;
my($gotmethod) = 0;
my($gotrequestid) = 0;
my($gotcount) = 0;
my($gotsize) = 0;
my($getpaths) = 0;
my($totsize) = 0;
my($totcnt) = 0;
my($trylater) = 0;

if (-e $kGETAPPOUT)
{
    unlink($kGETAPPOUT);
}

while (defined($aurl = shift(@urls)) && defined($alist = shift(@lists)))
{
    while (defined($sublist = GetToken($alist, "\&")))
    {
        # $sublist has series{1,2,3,...}
        if ($sublist =~ /(.+)\{(.+)\}/)
        {
            $series = $1;
            $sunumlist = $2;
        }
        else
        {
            print STDERR "Invalid series{sulist} argument; skipping\n";
            next;
        }

        # Escape funny chars in the the su list so it can be used as a cgi argument
        $escURL = CGI::escape($sunumlist);
        
        $cgiargs = "op=$kOP&ds=$sunumlist&method=$kMETHOD&format=txt&protocol=$kPROTO";
        $cmd = "$kGETAPP $kGETAPPFLAG $kGETAPPOFLAG \"$aurl?$cgiargs\"";

        # print STDERR "cmd: $cmd\n";

        # Download cgi url. This submits a request to jsoc_export_manage, which will
        # then call jsoc_export_SU_as_is, which writes the index.html file to
        # the output directory. 

        `$cmd`;
        
        # Check status to ensure request was submitted properly.
        if (defined(open(RESPFILE, "<$kGETAPPOUT")))
        {
            while (defined($line = <RESPFILE>) && 
                   (!$gotstatus || !$gotmethod || !$gotrequestid || !$gotcount || !$gotsize || $getpaths))
            {
                chomp($line);

                if ($line =~ /status\s*=\s*(\d+)/i)
                {
                    $status = $1;
                    $gotstatus = 1;
                    if ($status == 0)
                    {
                        # need to read the entire file
                        $getpaths = 1;
                    }
                }
                elsif ($line =~ /method\s*=\s*(\w+)/i)
                {
                    # "url_quick" can be converted to "url" if data was offline
                    $method = $1;
                    $gotmethod = 1;
                }
                elsif ($line =~ /requestid\s*=\s*(.+)/i)
                {
                    $requestid = $1;
                    $gotrequestid = 1;
                }
                elsif ($line =~ /count\s*=\s*(\w+)/i)
                {
                    $count = $1;
                    $gotcount = 1;
                }
                elsif ($line =~ /size\s*=\s*(\w+)/i)
                {
                    $size = $1;
                    $gotsize = 1;
                }
                elsif ($line =~ /\# data/i)
                {
                    while ($line =~ /^\s*$/)
                    {
                        $line = <RESPFILE>;
                        chomp($line);
                    }

                    while (defined($line = <RESPFILE>))
                    {
                        chomp($line);
                        
                        if ($line =~ /(\d+)\s+(\S+)\s+(\S+)\s*/)
                        {
                            push(@reqsunums, $1);
                            push(@reqseries, $2);
                            push(@reqfiles, $3);
                        }
                    }
                }
                
            } # while line in response file

            # print STDERR "status $gotstatus, method $gotmethod, reqid $gotrequestid, count $gotcount, size $gotsize\n";

            if (!$gotstatus || !$gotmethod || !$gotrequestid || !$gotcount || !$gotsize)
            {
                # no status line - continue.
                print STDERR "Improper response cgi response from export URL '$aurl$cgiargs'.\n";
                close(RESPFILE);
                unlink($kGETAPPOUT);
                next;
            }

            close(RESPFILE);
            unlink($kGETAPPOUT);

            if ($status != 0)
            {
                if ($method eq "url")
                {
                    # Creation of index.html happens asynchronously, so must poll
                    # by downloading a second cgi url.
                    sleep(2);

                    $gotstatus = 0;
                    $gotcount = 0;
                    $getpaths = 0;

                    $cgiargs = "op=$kEXPSTATUS&requestid=$requestid&format=txt";
                    $cmd = "$kGETAPP $kGETAPPOFLAG $aurl?$cgiargs";

                    while (1)
                    {
                        `$cmd`;
                        if (defined(open(RESPFILE, "<$kGETAPPOUT")))
                        {
                            chomp($line);
                            while (defined($line = <RESPFILE>) && 
                                   (!$gotstatus || !$gotcount || !$gotsize || $getpaths))
                            {
                                if ($line =~ /status\s*(\d+)/i)
                                {
                                    $status = $1;
                                    $gotstatus = 1;

                                    if ($status == 0)
                                    {
                                        # need to read the entire file
                                        $getpaths = 1;
                                    }
                                }
                                elsif ($line =~ /count\s*(\w+)/i)
                                {
                                    $count = $1;
                                    $gotcount = 1;
                                }
                                elsif ($line =~ /size\s*=\s*(\w+)/i)
                                {
                                    $size = $1;
                                    $gotsize = 1;
                                }
                                elsif ($line =~ /selected data/i)
                                {
                                    while ($line !~ /^\s*$/)
                                    {
                                        $line = <RESPFILE>;
                                        chomp($line);
                                    }

                                    while (defined($line = <RESPFILE>))
                                    {
                                        chomp($line);
                                        
                                        if ($line =~ /(\d+)\s+(\S+)\s+(\S+)\s*/)
                                        {
                                            push(@reqsunums, $1);
                                            push(@reqseries, $2);
                                            push(@reqfiles, $3);
                                        }
                                    }
                                }
                            }

                            if (!$gotstatus || !$gotcount || !$gotsize)
                            {
                                # no status line - break and fail.
                                print STDERR "Improper response cgi response from export URL '$aurl$cgiargs'.\n";
                                close(RESPFILE);
                                unlink($kGETAPPOUT);
                                last;
                            }
                            elsif ($status == 0)
                            {
                                # Export request has finished
                                close(RESPFILE);
                                unlink($kGETAPPOUT);
                                last;
                            }

                            close(RESPFILE);
                            unlink($kGETAPPOUT);
                        }
                        else
                        {
                            print STDERR "No response for '$cmd'.\n";
                            last;
                        }
                    } # while status != 0
                }
            }
        }
        else
        {
            # No response
            print STDERR "No response for '$cmd'.\n";  
        }

        # The status is for a single reponse (which may contain multiple SUs)
        if ($gotstatus == 0 || $status != 0)
        {
            # If we didn't find any SUs, skip to the next SU list.
            next;
        }

        # At this point, the original request has been serviced, status == 0, and we have
        # SUDIRS.
        $totsize += $size;
        $totcnt += $count;
    }
    
} # while item in sulists

if ($totcnt > 0)
{
    # Now is the time to determine if the SUs should be retrieve synchronously
    # or asynchronously. For now use the file size to determine that.
    if ($totsize > $kSIZECUTOFF)
    {
        # Request is too large to perform synchronously.
        # print STDERR "trying later, size is $totsize\n";
        $trylater = 1;
        print "$kTRYLATER\n";
    }
   
    my($onesunum);
    my($oneseries);
    my($first);
    my($listsunums);
    my($listpaths);
    my($listseries);
    my($inggood);

    # request is small - perform synchronously. 
    $first = 1;
    while ($totcnt-- > 0)
    {
        # The array @reqsunums contains the requested SUNUMs. The array @reqseries contains
        # the requested series. The array @reqfiles contains paths to the SUNUMS in the 
        # remote SUMS.
        $onesunum = shift(@reqsunums);
        $onepath = shift(@reqfiles);
        $oneseries = shift(@reqseries);
        
        if (!$first)
        {
            $listsunums = "$listsunums,$onesunum";
            $listpaths = "$listpaths,$onepath";
            $listseries = "$listseries,$oneseries";
        }
        else
        {
            $listsunums = $onesunum;
            $listpaths = $onepath;
            $listseries = $oneseries;
            $first = 0;
        }
    }

    # Call remotesums_ingest program - pass four parameters
    #   1. comma-separated list of sunums
    #   2. comma-separated list of supaths
    #   3. comma-separated list of series (may be redundant)
    #   4. path to ssh-agent configuration file
    $cmd = "$kRSINGEST sunums=$listsunums paths=$listpaths series=$listseries agentfile=$kAGENTFILE";
    if ($trylater)
    {
        $cmd = "$cmd &";
    }
    # print STDERR "$cmd\n";

    # Run cmd - the ingest script is now responsible for writing the error code needed by DRMS
    # to determine its next action (-1 error, 1 success - can't write 0 since the decision to 
    # synchronously download has already been made).
    if (system($cmd) != 0)
    {
        # cmd didn't run
        print STDERR "Couldn't run remotesums_ingest.\n";
        print "$kRSERROR\n";
    }
}
else
{
    print STDERR "No requested storage units located on serving SUMS.\n";
    print "$kRSERROR\n";
}

exit;



# It is possible that the local site has cached the remote SUs somewhere OFF the SUMS
# system altogether, and that the SUs are now not found in SUMS because they have aged-
# off.  There should be a site-specific check here for the presence of such 
# archived data.


# XXX - Check VSO to find the best place to find the data.  Perhaps this goes in DRMS.


sub GetToken
{
    my($line) = \$_[0];
    my($delim) = $_[1];
    my($ret);

    if (defined($$line))
    {
        if ($$line =~ /^$delim*([^$delim]+)$delim+([^$delim]+.*)/)
        {
            $ret = $1;
            $$line = $2;
        }
        elsif ($$line =~ /^$delim*([^$delim]+)/)
        {
            $ret = $1;
            $$line = undef;
        }
    }

    return $ret;
}
