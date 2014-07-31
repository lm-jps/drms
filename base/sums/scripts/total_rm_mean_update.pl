#!/usr/bin/perl
#total_rm_mean_update.pl
#
#Utility to read the last month in the 
#/home/production/cvs/JSOC/base/sums/scripts/total.rm file
#and recalculate the mean for that month and update the total.mean file with
#the new mean value.
#The total.rm file originally contains the # of bytes deleted by the sum_rm's 
#for each day:
#GRAND TOTAL for daynum=7145 Jul 25:5719410893646 4
#GRAND TOTAL for daynum=7146 Jul 26:7312748618205 4
#GRAND TOTAL for daynum=7147 Jul 27:6389830630192 4
#GRAND TOTAL for daynum=7148 Jul 28:6430154028051 4
#GRAND TOTAL for daynum=7149 Jul 29:8702777511907 4
#
#
#Return the current date in form for a label e.g. 2012.06.20
sub labeldate {
  local($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst,$date,$sec2,$min2,$hour2,$mday2,$year2);
  ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
  $sec2 = sprintf("%02d", $sec);
  $min2 = sprintf("%02d", $min);
  $hour2 = sprintf("%02d", $hour);
  $mday2 = sprintf("%02d", $mday);
  $mon2 = sprintf("%02d", $mon+1);
  $year4 = sprintf("%04d", $year+1900);
  $date = $year4.".".$mon2.".".$mday2;
  return($date);
}

$MODULE = "total_rm_mean_update.pl";
$TFILE = "/home/production/cvs/JSOC/base/sums/scripts/total.rm";
$MFILE = "/home/production/cvs/JSOC/base/sums/scripts/total.mean";
$MFILENEW = "/home/production/cvs/JSOC/base/sums/scripts/total.mean.new";

$ldate = &labeldate();
print "Run of $MODULE on $ldate\n";
$first = 1;
$total = 0;
@bymo = ();
open(TF, $TFILE) || die "Can't open $TFILE: $!\n";
seek TF, -120, 2;       #back up >1 line at EOF
while(<TF>) {
  $line = $_;
}
#line looks like:
#GRAND TOTAL for daynum=7158 Aug 07:4637631205778 4
($a,$b,$c,$d,$mo,$daystr) = split(/\s+/, $line);
($day) = split(/:/, $daystr);
$pos = $day * 56;   #back up this far in file
seek TF, -$pos, 2;

while(<TF>) {
  if(/^#/ || /^\n/) { #ignore any comment or blank lines
    next;
  }
  if(/^GRAND /) {
    ($a,$b,$c,$daynumstr,$mos, $daystr) = split(/\s+/);
    ($x, $daynum) = split(/=/, $daynumstr);
    ($day,$bytes) = split(/:/, $daystr);
    if($mos eq $mo) {  #this is the month we want
      if($first) {
        $startdaynum = $daynum;
        $mosv = $mo;
        $first = 0;
        push(@bymo, $bytes);
      }
      else {
          push(@bymo, $bytes);
      }
    }
  }
}
#Now do the last month
$cnt = 0;
while($b = shift(@bymo)) {
  $total += $b;
  $cnt++;
}
if($cnt) { $m = int $total/$cnt; }  #!!TBD ck why 0
else { $m = $total; }
$newline = "MEAN for $mosv  = $m with start_daynum=$startdaynum\n";
close(TF);
open(MF, "$MFILE") || die "Can't open $MFILE: $!\n";
#open(MFN, "$MFILENEW") || die "Can't open $MFILENEW: $!\n";
$foundmo = 0;
@mflines = ();
while(<MF>) {
  if(/^\n/) { #ignore blank lines
    next;
  }
  push(@mflines, $_);
  if(/^MEAN /) {
    ($a,$b,$momean) = split(/\s+/);
    if($momean eq $mosv) {	#this month already in file
      $foundmo = 1;
    }
  }
}
close(MF);
if($foundmo) {
  $x = pop(@mflines);  #replace last line
  push(@mflines, $newline);
}
else {
  push(@mflines, $newline);
}
print "This line put in the total.mean file:\n$newline\n";
open(MF, ">$MFILE") || die "Can't open $MFILE: $!\n";
while($x = shift(@mflines)) {
  print MF "$x";
}
close(MF);
