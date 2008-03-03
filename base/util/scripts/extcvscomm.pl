#!/usr/bin/perl -w 

# Usage: extcvscomm.pl <date>
#   where <date> specifies the beginning of the time inteval (end is present)
#   from which to cull logs.
#
#   Ex: extcvscomm.pl 2007-12-7

my($cmd);
my($arg);
my($line);
my($nextfile) = 0;
my($grabtext) = 0;
my($lastRelDate);
my($file);
my($key);
my(@clarr);
my(%clmap);
my(%fmap);

while ($arg = shift(@ARGV))
{
    $lastRelDate = $arg;
    $aidx++;
}

if ($aidx > 1)
{
    print STDERR "extcvscomm.pl takes a single argument: a date\n";
    exit(1);
}

$cmd = "((cvs log -Nd \">$lastRelDate\" \.) 2>&1) |";
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
	    if ($line =~ /^date:(.+)author:(.+?);.*/)
	    {
		$key = "::D:$1::A:$2";
		$line = <CMDRET>;
		chomp($line);
		my($files);

		if (defined($clmap{$key}))
		{
		    $clmap{$key} = "$clmap{$key}.$line";
		    $fmap{$key} = "$fmap{$key}||$file";
		}
		else
		{
		    $clmap{$key} = $line;
		    $fmap{$key} = $file;
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
    if ($key =~ /::D:\s*(\S+)\s*(\S+);\s*::A:\s*(\S+)/)
    {
	print STDOUT "date: $1 $2 ($3)\nfiles: $fmap{$key}\ncomments: $clmap{$key}\n\n";
    }
}



