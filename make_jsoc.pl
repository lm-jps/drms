#!/usr/bin/perl -w 

# run this on each machine to be used.
#    n00 - for linux_ia32 machines such as n00, ..., n11, etc.
#    n12 - for linux_x86_64 machines
# 
# *** The caller of this script must cd to the JSOC root directory before running this script. ***

use Cwd qw(realpath);

my($cdir) = realpath($ENV{'PWD'});
print STDOUT "make of JSOC $cdir\n";
system("date");

# make clean 
# make -j 4 

system("make clean");

if (-e "configsdp.txt")
{
    system("make all dsds");
}
else
{
    system("make");
}

system("date");
