#!/usr/bin/perl -w 

# Usage: extcvscomm.pl <date>
#   where <date> specifies the beginning of the time inteval (end is present)
#   from which to cull logs.
#
#   Ex: extcvscomm.pl 2007-12-7

# Not surprisingly, CVS botches the "cvs log -d" command.  If you provide a date argument
# and there is a change on that date, 
# it appears that CVS finds the last change on that date, and then prints the change right
# before that.  If there was no change on that date, then it prints the last change before
# that date.
# Man, does CVS stink!

use Time::Local; # to compensate for CVS's stinkiness

$NCOMM = 256;

my($cmd);
my($arg);
my($line);
my($nextfile) = 0;
my($grabtext) = 0;
my($dateArg);
my($file);
my($date);
my($author);
my($commprefix);
my($key);
my(@clarr);
my(%clmap);
my(%fmap);
my(%tmap);
my(@dateArr);
my(@desiredDateArr);
my(@desiredDateArrB);
my(@desiredDateArrInt);
my($secs);
my($desiredSecs);
my($desiredSecsB);
my($dateArgB);
my($range) = "na";

while ($arg = shift(@ARGV))
{
    $dateArg = $arg;
    $aidx++;
}

if ($aidx > 1)
{
    PrintUsage();
    exit(1);
}

if ($dateArg =~ /^\s*\<(.+)\s*$/)
{
    # Less than or equal to a date
    @desiredDateArr = GetTimeArgs($1);
    $desiredSecs = timelocal(@desiredDateArr);
    @desiredDateArrInt = AddDay(1, @desiredDateArr);
    $dateArg = GetDate(@desiredDateArrInt);
    $dateArg = "<$dateArg";
    $range = "lt";
}
elsif ($dateArg =~ /^\s*\>(.+)\s*$/)
{
    # Greater than or equal to a date
    @desiredDateArr = GetTimeArgs($1);
    $desiredSecs = timelocal(@desiredDateArr);
    @desiredDateArrInt = AddDay(-1, @desiredDateArr);
    $dateArg = GetDate(@desiredDateArrInt);
    $dateArg = ">$dateArg";
    $range = "gt";
}
elsif ($dateArg =~ /^\s*(.+)\s*:\s*(.+)\s*$/)
{
    # An interval of dates
    @desiredDateArrB = GetTimeArgs($1);
    @desiredDateArr = GetTimeArgs($2);
    $desiredSecsB = timelocal(@desiredDateArrB);
    $desiredSecs = timelocal(@desiredDateArr);
    @desiredDateArrInt = AddDay(-1, @desiredDateArrB);
    $dateArgB = GetDate(@desiredDateArrInt);
    @desiredDateArrInt = AddDay(1, @desiredDateArr);
    $dateArg = GetDate(@desiredDateArrInt);
    $dateArg = "$dateArgB<$dateArg";
    $range = "int";
}
elsif ($dateArg =~ /^\s*(.+)\s*$/)
{
    @desiredDateArr = GetTimeArgs($1);
    $desiredSecs = timelocal(@desiredDateArr);
    @desiredDateArrInt = AddDay(-1, @desiredDateArr);
    $dateArgB = GetDate(@desiredDateArrInt);
    @desiredDateArrInt = AddDay(1, @desiredDateArr);
    $dateArg = GetDate(@desiredDateArrInt);
    $dateArg = "$dateArgB<$dateArg";
    $range = "na";
}

$cmd = "((cvs log -Nd \"$dateArg\" \.) 2>&1) |";

# print "$cmd\n";
open(CMDRET, $cmd);

while (defined($line = <CMDRET>))
{
    chomp($line);
    
    if ($nextfile == 1)
    {
        if ($line =~ /^description:.*/)
        {
            $grabtext = 1;
            $nextfile = 0;
        }
        
        next;
    }
    elsif ($grabtext == 1)
    {
        if ($line =~ /============.+/)
        {
            $grabtext = 0;
            next;
        }
        else
        {
            if ($line =~ /^date:\s*(.+?)\s*;\s+author:\s*(.+?);.*/)
            {   
                # Unfortunately, CVS!
                # To work around this deficiency, use the first 256 bytes of the
                # actual comment used during the commit.  Some people won't provide
                # that - skip those lines.
                $date = $1;
                $author = $2;
                
                # skip things caused by CVS's stink
                @dateArr = GetTimeArgs($date);
                $secs = timelocal(@dateArr);
                #		print "secs: $secs, desiredsecs: $desiredSecs\n";
                
                if ($range eq "na")
                {
                    if ($secs != $desiredSecs)
                    {
                        next;
                    }
                }
                elsif ($range eq "lt")
                {
                    if ($secs > $desiredSecs)
                    {
                        next;
                    }
                }
                elsif ($range eq "gt")
                {
                    if ($secs < $desiredSecs)
                    {
                        next;
                    }
                }
                elsif ($range eq "int")
                {
                    if ($secs < $desiredSecsB || $secs > $desiredSecs)
                    {
                        next;
                    }
                }
                
                $commprefix = "";
                
                # There could be a newline at the beginning of the comment. If so, skip that line and go onto the next
                while ($commprefix !~ /\s*\S/)
                {
                    $line = <CMDRET>;
                    chomp($line);
                    $commprefix = substr($line, 0, $NCOMM);
                }            
                
                $key = "::A:${author}::C:$commprefix";
                
                if (defined($clmap{$key}))
                {
                    $fmap{$key} = "$fmap{$key}||$file";
                }
                else
                {
                    $clmap{$key} = $line;
                    $fmap{$key} = $file;
                    $tmap{$key} = "$date";
                    push(@clarr, $key);
                }
                
            }
        }
    }
    elsif ($line =~ /^RCS file:\s*(.+)/)
    {
        $file = $1;
        $nextfile = 1;
        next;
    }
}

while (defined($key = shift(@clarr)))
{
    if ($key =~ /::A:(\S+)::C:.+/)
    {
        print STDOUT "date: $tmap{$key} ($1)\nfiles: $fmap{$key}\ncomments: $clmap{$key}\n\n";
    }
}

sub PrintUsage
{
    print "extcvscomm.pl DATE           #Show all changes on DATE\n";
    print "extcvscomm.pl <DATE          #Show all changes on DATE or before\n";
    print "extcvscomm.pl >DATE          #Show all changes on DATE or after\n";
    print "extcvscomm.pl DATE1:DATE2    #Show all changes on or after DATE1 and on or before DATE2\n";
    return;
}

sub GetTimeArgs
{
    my($date) = @_;
    my(@args);
    my($dom);
    my($month);
    my($year);
    
    $date =~ s/\//-/g;
    
    if ($date =~ /([0-9][0-9][0-9][0-9])\-([0-9]+)\-([0-9]+)/)
    {
        $year = $1;
        $month = $2;
        $dom = $3;
    }
    else
    {
        print STDERR "Invalid date '$date'\n";
        exit(1);
    }
    
    push(@args, 0); # sec
    push(@args, 0); # min
    push(@args, 0); # hr
    push(@args, $dom); # day of month
    push(@args, $month - 1); # month, 0-based
    push(@args, $year - 1900); # year is relative to 1900
    
    return @args;
}

sub GetDate
{
    my($sec, $min, $hr, $dom, $month, $yr) = @_;

    $month++; #dates are 1-based, but args are 0-based
    $yr = $yr + 1900;
    $date = "$yr-$month-$dom";
    return $date
}

sub AddDay
{
    my($ndays, $sec, $min, $hr, $dom, $month, $yr) = @_;
    my($resultSecs);
    my(@resultArr);

    $resultSecs = timelocal($sec, $min, $hr, $dom, $month, $yr) + $ndays * 86400;
    @resultArr = localtime($resultSecs);

    return @resultArr;
}
