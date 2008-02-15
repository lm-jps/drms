#!/usr/bin/perl -w 

# run this on each machine to be used.
#    n00 - for linux_ia32 machines such as n00, ..., n11, etc.
#    n12 - for linux_x86_64 machines

use Cwd qw(realpath);

my($scriptpath) = realpath($0);
print STDOUT "make of JSOC $scriptpath\n";
system("date");

my($wd);
$wd = $scriptpath;

if ($wd =~ /(.+)\/\S+$/)
{
    $wd = $1;
}
else
{
    print STDERR "Invalid make_jsoc.pl path; bailing!\n";
    exit(1);
}

chdir($wd);

# make clean 
# make -j 4 

if (-e "suflag.txt")
{
    system("make all dsds");
}
else
{
    system("make");
}

system("date");
