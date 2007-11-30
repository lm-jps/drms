#!/usr/bin/perl
#
for($j=0; $j < 100; $j++) {
  #$cmd = "date 1> /tmp/xx.$j 2>&1";
  $cmd = "sum_test 1> /tmp/sum_test.$j 2>&1";
  if($fpid = fork) {
    #This is the parent. The child's pid is in $fpid
    print stdout "pid is $fpid for j=$j\n";
    #wait;
  } elsif (defined $fpid) {     # $fpid is zero here if defined
    exec $cmd;                  # run the program
    exit;                       # never fall through
  } else {
    #fork error
    print stdout "!!! Can't fork a program for j=$j: $!\n";
  }
}
