#!/usr/bin/perl -w 

# Usage: extcvscomm.pl <date>
#   where <date> specifies the beginning of the time inteval (end is present)
#   from which to cull logs.
#
#   Ex: extcvscomm.pl 2007-12-7

$NCOMM = 256;

my($cmd);
my($arg);
my($line);
my($nextfile) = 0;
my($grabtext) = 0;
my($lastRelDate);
my($file);
my($date);
my($author);
my($commprefix);
my($key);
my(@clarr);
my(%clmap);
my(%fmap);
my(%tmap);

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
	    if ($line =~ /^date:\s*(.+?)\s*;\s+author:\s*(.+?);.*/)
	    {
		# Unfortunately, CVS!
		# To work around this deficiency, use the first 256 bytes of the
		# actual comment used during the commit.  Some people won't provide
		# that - skip those lines.
		$date = $1;
		$author = $2;

		$line = <CMDRET>;
		chomp($line);
		$commprefix = substr($line, 0, $NCOMM);

		if ($commprefix !~ /\S/)
		{
		    next;
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
