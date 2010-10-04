#!/home/jsoc/bin/linux_x86_64/perl -w

use strict;
use warnings;
use Data::Dumper;

use constant kNContext => 5;

my($TESTERR) = "no";
my($JSOC_DEV) = "jsoc_dev\@sun.stanford.edu";
#my($JSOC_DEV) = "arta\@sun.stanford.edu"; # for testing purposes
my($ROOTDIR) = "/tmp/jsoc";
my($MAKELOG) = "make.log";
my($ret);
my($MAXERRLINES) = 8;

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

open(FH, "<$ROOTDIR/JSOC/$MAKELOG") || die "Can't open log file: $!\n";

my(@lines) = <FH>;
my($linenum);
my($firstidx);
my(@errslice);
my($errmsg) = "";
my($date);
my($chomped);

close(FH);

if ($#lines >= 0) 
{
   # There was a build error.
   # Go through each line, looking for recognized error strings
   $linenum = 0;
   $firstidx = 0;
   foreach my $aline (@lines)
   {
      $chomped = $aline;
      chomp($chomped);
   
      if (RecognizedError($chomped))
      {
         # Collect kNContext lines before error
         if ($linenum - kNContext >= 0)
         {
            $firstidx = $linenum - kNContext;
         }

         @errslice = @lines[$firstidx..$linenum];

         last;
      }

      $linenum++;    
   }

   if ($#errslice >= 0)
   {
      # We have lines that may indicate an error - look at last few lines to be sure.
      # make isn't consistent so the last line doesn't always have an error code - it
      # may appear in earlier lines.
      my($iline);
      my($goterror);

      $goterror = 0;
      $iline = $#lines;
      while ($iline >= 0 && $#lines - $iline < 4)
      {
         if ($lines[$iline] =~ /\*\*\*\s+\[.+\]\s+Error/i)
         {
            $goterror = 1;
            last;
         }
         
         $iline--;
      }

      if ($goterror)
      {
         $linenum = @errslice;
         $date = `date`;
         chomp($date);
         $errmsg = "${errmsg}JSOC Build Failed on ${date}:\n";
         $linenum++;
         $errmsg = join('', $errmsg, @errslice);

         if ($linenum >= $MAXERRLINES)
         {
            $errmsg = "${errmsg}...\n";
         }
      }
   }

   if (length($errmsg) > 0)
   {
      open FH, "| /usr/bin/Mail -s \"JSOC build problem\" $JSOC_DEV";
      print FH $errmsg;
      close FH;
   }
} 
else 
{
    my $cmd = "$ROOTDIR/JSOC/doc/doxygen/gendox.csh";
    `chmod +x $cmd`;
    `$cmd >& $ROOTDIR/JSOC/doxygen.log`;
    create_sl();
}

sub RecognizedError
{
   my($line) = $_[0];
   my($rv) = 0;

   if ($line =~ /:\s+undefined reference to\s+/ || $line =~ /:\s\S*\serror:\s/i || $line =~ /icc: error/ || $line =~ /:\serror:/)
   {
      $rv = 1;
   }

   return $rv;
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
