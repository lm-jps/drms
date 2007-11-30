#!/usr/bin/perl
#
#Usage: prune_log_list.pl in_file
#where in_file = ls -lt list to extract just names from.
#Make sure this is a clean list only of files to be gzip'd.
#
#Entries in the in_file look like:
#-rw-rw-r--  1 production SOI      107 Sep 14 15:48 sum_pe_svc_1066.log
#-rw-rw-r--  1 production SOI   603443 Sep 14 15:45 sum_rm.log.2007.09.13.133044


if($infile = shift(@ARGV)) {
}
else {
  print "Must give an in_file of ls -lt of files\n";
  exit;
}

open(IN, "<$infile") || die "Can't open $infile: $!\n";

while(<IN>) {
  if(/^#/ || /^\n/) { #ignore any comment or blank lines
    next;
  }
  ($a,$b,$c,$d,$e,$f,$g,$h,$name) = split(/\s+/);
  print "$name\n";
  #`/bin/rm $name`;
  `gzip $name`;
  #exit(0);
}
