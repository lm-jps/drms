#!/usr/bin/perl --

use strict;
use warnings;
use Data::Dumper;

use lib '/home/karen/cvs/jsoc/_linux_x86_64/base/sums/libs/api/perl';
use SUMSAPI;
my $sum = SUMSAPI::SUMOpen("d02.Stanford.EDU", "hmidb");
# print Dumper($sum->{uid});
# print Dumper($sum->{tdays});
# print Dumper($sum->{username});
# print Dumper($sum->{storeset});
# print Dumper($sum->{dsix_ptr});

my @suid = (2405744, 2405737);
$sum->{dsix_ptr} = \@suid;
$sum->{mode} = 16;
$sum->{tdays} = 10;
$sum->{reqcnt} = scalar(@suid);
my $status = SUMSAPI::SUMGet($sum);
if (!$status) {
    print "SUMGet: \n";
    print Dumper($sum->{wd});
}

$sum->{reqcnt} = 1;
$sum->{bytes} = 32*1024*1024;
$status = SUMSAPI::SUMAlloc($sum);
my $suid;
my $wd;
if (!$status) {
    $suid = @{$sum->{dsix_ptr}}[0];
    $wd = @{$sum->{wd}}[0];
}

print "SUMAlloc: $suid, $wd\n";

$sum->{mode} = 2;
$sum->{reqcnt} = 1;
$sum->{dsname} = "testonly";
$status = SUMSAPI::SUMPut($sum);
if (!$status) {
    print "SUMPut() success\n";
}

SUMSAPI::SUMClose($sum);

#my $r = SUMSAPI::SUM_poll($sum);

