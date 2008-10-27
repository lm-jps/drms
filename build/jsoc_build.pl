#!/usr/bin/perl --

use strict;
use warnings;
use Data::Dumper;

my($TESTERR) = "no";
my($JSOC_DEV) = "jsoc_dev\@sun.stanford.edu";
#my($JSOC_DEV) = "arta\@sun.stanford.edu"; # for testing purposes
my($ROOTDIR) = "/tmp/jsoc";
my($MAKELOG) = "make.log";
my($ret);

$ENV{'CVSROOT'} = ':ext:sunroom.stanford.edu:/home/cvsuser/cvsroot';
$ENV{'CVS_RSH'} = 'ssh';
$ENV{'PATH'} .= ':/usr/local/bin';

if ($TESTERR eq "no")
{
    if (-e "$ROOTDIR/JSOC")
    {
        `rm -rf "$ROOTDIR/JSOC" 2>&1 1>>/dev/null`;
    }

    `mkdir -p "$ROOTDIR/JSOC" 2>&1 1>>/dev/null`;

    my $cmd = "cd $ROOTDIR; cvs co JSOC; cvs update JSOC; cd JSOC; ./configure";
    $cmd = join("; ", map {
        $_." 2>&1 1>>$ROOTDIR/JSOC/cmd.log";
    } split("; ", $cmd));

    $ret = `$cmd; make universe -k >& $ROOTDIR/JSOC/$MAKELOG`;
}
else
{
    `cd $ROOTDIR/JSOC; make universe -k >& $MAKELOG`;
}

open FH, "cd $ROOTDIR/JSOC; \(tail -n 4 $MAKELOG | grep Error\)  |" || die "Can't open log file: $!\n";
my @line = <FH>;
my($oneline);
my($errmsg) = "";

close FH;
if (scalar(@line)) {
    open FH, "$ROOTDIR/JSOC/$MAKELOG";
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
    my $cmd = "$ROOTDIR/JSOC/doc/doxygen/gendox.csh";
    `chmod +x $cmd`;
    `$cmd >& $ROOTDIR/JSOC/doxygen.log`;
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
