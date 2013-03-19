#!/usr/bin/perl
#
#Usage: irisdc_prodtest_cmp.pl in_file
#where in_file = ls -l list of files to compare with 
#/home/production/cvs/JSOC file.
#Make sure this is a clean list only of files to be compared.
#
#Entries in the in_file look like, where the first line must be cd:
#cd /home/prodtest/cvs/JSOC/proj/lev0/apps
#-rw-rw-r-- 1 prodtest prodtest 11159 Jul  8  2009 add_small_image.c
#-rw-rw-r-- 1 prodtest prodtest 18109 Jan  5  2011 aia_despike.c
#

if($infile = shift(@ARGV)) {
}
else {
  print "Must give an in_file of ls -l of files\n";
  exit;
}

open(IN, "<$infile") || die "Can't open $infile: $!\n";

while(<IN>) {
  if(/^#/ || /^\n/) { #ignore any comment or blank lines
    next;
  }
  if(/^cd /) {
    ($a, $dir) = split(/\s+/);
    print "chdir $dir\n";
    chdir $dir or die "Can't change dir\n";
    next;
  }
  ($a,$b,$c,$d,$e,$f,$g,$h,$name) = split(/\s+/);
  #print "$name\n";
  $cmd = "cvs status $name";
  print "$cmd\n";
  @status = `$cmd`;
  #print "@status\n";
  $line = @status[1];
  if($line =~ /Up-to-date/) { next; }
  print "$line";
  if($line =~ /Unknown/) { next; }
  $cmd = "scp sunroom:$dir/$name $dir/XX";
  print "$cmd\n";
  @ans = `$cmd`;
  $cmd = "diff -q $name XX";
  @ans = `$cmd`;
  print "@ans\n";
}
