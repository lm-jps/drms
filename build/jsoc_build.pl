#!/usr/bin/perl --

use strict;
use warnings;
use Data::Dumper;

my($JSOC_DEV) = "jsoc_dev\@sun.stanford.edu";
# my($JSOC_DEV) = "arta\@sun.stanford.edu"; # for testing purposes

$ENV{'CVSROOT'} = ':ext:lws.stanford.edu:/home/cvsuser/cvsroot';
$ENV{'CVS_RSH'} = 'rsh';
$ENV{'PATH'} .= ':/usr/local/bin';

my $cmd = "cd /tmp; rm -rf JSOC; cvs co JSOC; cvs update JSOC; cd JSOC; ./configure";

$cmd = join("; ", map {
    $_." 2>&1 1>/dev/null";
} split("; ", $cmd));

`$cmd; make universe -k >& log`;

# `cd /tmp/JSOC; make universe -k >& log`;

open FH, "cd /tmp/JSOC; \(tail -n 4 log | grep Error\)  |" || die "Can't open log file: $!\n";
my @line = <FH>;
my($oneline);
my($errmsg) = "";

close FH;
if (scalar(@line)) {
    open FH, "/tmp/JSOC/log";
    while($oneline = <FH>)
    {
        chomp($oneline);
        if ($oneline =~ /:\serror:\s/)
        {
            $errmsg = "${errmsg}${oneline}\n";
            $oneline = <FH>;
            if ($oneline)
            {
               $errmsg = "${errmsg}${oneline}\n"; 
            }
            $oneline = <FH>;
            if ($oneline)
            {
               $errmsg = "${errmsg}${oneline}\n"; 
            }
        }
    }
    close FH;

    open FH, "| /usr/bin/Mail -s \"JSOC build problem\" $JSOC_DEV";
    print FH $errmsg;
    close FH;

} else {
    my $cmd = "/tmp/JSOC/doc/doxygen/gendox.csh";
    `chmod +x $cmd`;
    `$cmd >& /tmp/JSOC/doxygen.log`;
    create_sl();
}

sub create_sl {
    my $dir = '/web/jsoc/htdocs/doxygen_html';
    opendir DH, $dir;

    my $file;
    while (defined($file = readdir(DH))) {
	if ($file =~ /-example/) {
	    my $file2 = $file;
	    $file2 =~ s/-example/-source/;
	    my $cmd = "ln -s $dir/$file $dir/$file2";
	    #	print $cmd, "\n";
	    `$cmd`;
	}
    }

    closedir DH;
}
