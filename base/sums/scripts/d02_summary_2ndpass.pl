#!/home/jsoc/bin/linux_x86_64/perl
#TBD

sub commify {
                  local $_  = shift;
                  1 while s/^([-+]?\d+)(\d{3})/$1,$2/;
                  return $_;
             }

sub commify2 {
  my $text = reverse $_[0];
  $text =~ s/(\d\d\d)(?=\d)(?!\d*\.)/$1,/g;
  return scalar reverse $text;
}


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
  if(/^#/ || /^\n/) { #ignore any comment or blank lines
    next;
  }
  chomp();
  ($ownseries, $bytes) = split(/\s+/);
  $c = commify(int($bytes));
  printf "%-52s %20s\n", $ownseries, $c;
#  $ownseries  =~  s/\s+$//;		#elim trail space
#  if(!grep(/$ownseries/, @owns)) {
#    push(@owns, $ownseries);
#  }
#  $HofOwn{$ownseries} += $bytes;
}
close(ID);
#print "Owning series total bytes on d02:\n";
#print "---------------------------------\n";
#while($s = shift(@owns)) {
#  #print "$s\t\t$HofOwn{$s}\n"
#  $info = sprintf("%s %22s\n", $s, $HofOwn{$s});
#  #$info = sprintf("%s %20.20s\n", $s, $HofOwn{$s});
#  print $info;
#}
