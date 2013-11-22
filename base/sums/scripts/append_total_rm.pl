#!/usr/bin/perl
#
#Utility to update the /home/production/cvs/JSOC/base/sums/scripts/total.rm file.
#This file contains the # of bytes deleted by the sum_rm's for each day:
#GRAND TOTAL for daynum=7145 Jul 25:5719410893646 4
#GRAND TOTAL for daynum=7146 Jul 26:7312748618205 4
#GRAND TOTAL for daynum=7147 Jul 27:6389830630192 4
#GRAND TOTAL for daynum=7148 Jul 28:6430154028051 4
#GRAND TOTAL for daynum=7149 Jul 29:8702777511907 4
#
#Will read the last line and determine the next day needed, e.g. Jul 30.
#Will then find all the sum_rm.log_[0,1,2]* files that have this day
#and extract all the lines and append the grand total of the storage
#for the day into $TFILE.
#
#This must be run on k1 which has the log files (in $D).

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

sub commify {
  local $_  = shift;
  1 while s/^([-+]?\d+)(\d{3})/$1,$2/;
  return $_;
}

$TIMEINDEX = "/home/jsoc/bin/linux_avx/time_index";
$MODULE = "append_total_rm.pl";
$D = "/usr/local/logs/SUM/";
$TFILE = "/home/production/cvs/JSOC/base/sums/scripts/total.rm";
@mo = ('Jan ','Feb ','Mar ','Apr ','May ','Jun ','Jul ','Aug ','Sep ','Oct ','Nov ','Dec ');
  $host = `hostname -s`;
  chomp($host);
  if($host ne "k1") {
    print "Error: This must be run on k1 where the /usr/local/logs/SUM is\n";
    exit;
  }

$ldate = &labeldate();
print "Run of $MODULE on $ldate\n";
open(TF, $TFILE) || die "Can't open $TFILE: $!\n";
seek TF, -120, 2;	#back up >1 line at EOF
while(<TF>) {
  if(/^#/ || /^\n/) { #ignore any comment or blank lines
    next;
  }
  $line = $_;
}
close(TF);
$pos = index($line, "daynum=");
$pos2 = index($line, ' ', $pos);
$daynum = substr($line, $pos+7, $pos2-($pos+7));
$daynum++;
$datex = `$TIMEINDEX -t day=$daynum`;  #get e.g. 2012.07.30_00:00:00_TAI
$date = substr($datex, 0, 10);
($yrx,$mox,$dayx) = split(/\./, $date);
$mix = $mox - 1;
$amonth = @mo[$mix];
#print "Find data for $date\n";

for($i = 0; $i < 3; $i++) {
  $cmpfile = "sum_rm.log_$i.$date";
  @lslog = `cd $D; /bin/ls sum_rm.log_$i.*`;
  @leq = ();
  while ($x = shift(@lslog)) {
    chomp($x);
    $lfile = substr($x, 0, 23);  #get in form sum_rm.log_0.2012.07.20
    if($lfile lt $cmpfile) {
      $lastlt = $x;		#save the last lt file
    }
    if($lfile eq $cmpfile) {
      push(@leq, $x);
    }
    if($lfile gt $cmpfile) {
      last;
    }
  }
  #The files we need are $lastlt and the ones in @leq
  if($i == 0) {
    $lastlt0 = $lastlt;
    @leq0 = @leq;
  } elsif($i == 1) {
    $lastlt1 = $lastlt;
    @leq1 = @leq;
  } else {
    $lastlt2 = $lastlt;
    @leq2 = @leq;
  }
  #print "$lastlt\n";
  #while($y = pop(@leq)) { print "$y\n"; }
}

%HofTotal = (); #hash key is "mon day", e.g. "Jul 9"
print "Going to search these files for our $date data:\n";
for($i=0; $i < 3; $i++) {
  $total = 0;
  $first = 1;
  if($i == 0) { 
    $file = "$D"."$lastlt0";
    push(@leqX, $file);
    while($x = shift(@leq0)) {
      $y = "$D"."$x";
      push(@leqX, $y);
    }
  }
  elsif ($i == 1) { 
    $file = "$D"."$lastlt1";
    push(@leqX, $file);
    while($x = shift(@leq1)) {
      $y = "$D"."$x";
      push(@leqX, $y);
    }
  }
  else { 
    $file = "$D"."$lastlt2";
    push(@leqX, $file);
    while($x = shift(@leq2)) {
      $y = "$D"."$x";
      push(@leqX, $y);
    }
  }
}
while($file = shift(@leqX)) {
  print "Processing file $file\n";
  open(ID, $file) || die "Can't open $file: $!\n";
  while(<ID>) {
    if(/^#/ || /^\n/ || /^\*/ || /^ /) {
      next;
    }
    ($mock,$dayy) = split(/\s+/);
    $day = sprintf("%02d", $dayy);
    $mock = $mock." ";
    if($x = grep(/$mock/, @mo)) {
      if($first) {
        #print "$_";
        $daysv = $dayx;
        $mosv = $amonth;
        if(($day != $dayx) || ($amonth ne $mock)) { next; }
        $first=0;
        #print "first now 0  $amonth $dayx\n";
      }
      else {
        if($day ne $daysv) {
          $first = 1;
          #$date = "$mock"."$daysv";
          $datem = "$mosv"."$daysv";
          $HofTotal{$datem} += $total;
          printf("TOTAL for day $datem for $file: %16s\n", commify(int($total)));
          $total = 0;
          print "!!Start of New Day\n"; !!TEMP
        }
      }
      #print "$_";
      next;
    }
    if($first) { next; };	#skip to find the day we want
    if(/Attempt to del/) {
      #print "$_";
      next;
    }
    if(/^bytes deleted/) {
      #print "$_";
      chomp;
      ($a, $bytes) = split(/=/);
      #print "bytes = $bytes\n";
      $total += $bytes;
      #print "total = $total\n";
    }
  }
  close(ID);
}
  $ccnt = 0;
  if(!$datem) {  #no data found
    $datem = "$amonth"."$dayx";
    $HofTotal{$datem} = 0;
  }
  else {
    $bcx = commify(int($HofTotal{$datem}));
    while(($pos = index($bcx, ',')) != -1) {
      $bcx = substr($bcx, $pos+1);
      $ccnt++;
    }
  }

  #printf("GRAND TOTAL for $datem: %16s $ccnt\n", int($HofTotal{$datem}));
  print "GRAND TOTAL for daynum=$daynum $datem:$HofTotal{$datem} $ccnt\n";
  #Now update the /home/production/cvs/JSOC/base/sums/scripts/total.rm
  open(TL, ">>$TFILE") || die "Can't open $file: $!\n";
  print TL "GRAND TOTAL for daynum=$daynum $datem:$HofTotal{$datem} $ccnt\n";
  close(TL); 


