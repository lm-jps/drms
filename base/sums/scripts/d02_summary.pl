#!/home/jsoc/bin/linux_x86_64/perl
#Take a query file from the query:


if($#ARGV != 0) {
  print "Must give an input file\n";
  exit;
}
$INFILE = $ARGV[0];

$user = $ENV{'USER'};
if($user ne "production") {
  print "You must be user production to run\n";
  exit;
}
%HofOwn = ();		#hash key is $ownseries. Content is bytes
open(ID, $INFILE) || die "Can't open $INFILE: $!\n";
$i = 0;
while(<ID>) {
  if($i < 2) { $i++; next; }
  if(/^#/ || /^\n/) { #ignore any comment or blank lines
    next;
  }
  chomp();
  ($wd, $ownseries, $dsx, $bytes) = split(/\| /);
  $ownseries  =~  s/\s+$//;		#elim trail space
  if(!grep(/$ownseries/, @owns)) {
    push(@owns, $ownseries);
  }
  $HofOwn{$ownseries} += $bytes;
}
close(ID);
print "Owning series total bytes on d02:\n";
print "---------------------------------\n";
while($s = shift(@owns)) {
  #print "$s\t\t$HofOwn{$s}\n"
  $info = sprintf("%s %22s\n", $s, $HofOwn{$s});
  #$info = sprintf("%s %20.20s\n", $s, $HofOwn{$s});
  print $info;
}
