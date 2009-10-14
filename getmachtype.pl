#!/usr/bin/perl -w 

use Sys::Hostname;

my($hostname);
my($machtype);

 # first figure out what type of Stanford machine this script is running on
$hostname = hostname();

if ($hostname =~ /j1/)
{
   $machtype = "j1";
}
elsif ($hostname =~ /d02/)
{
   $machtype = "d02";
}
elsif ($hostname =~ /hmidb/)
{
   $machtype = "dbserver";
}
elsif ($hostname =~ /cl1n0/)
{
   $machtype = "cluster";
}
elsif ($hostname =~ /dcs/)
{
   $machtype = "dcs";
}

if (defined($machtype))
{
   print STDOUT $machtype;
}
