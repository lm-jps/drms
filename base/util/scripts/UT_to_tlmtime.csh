#! /bin/csh -f

#!!TBD ck if this is correct

set UT = $1
set UTs = `time_convert time=$UT`
set UTs = `basename $UTs .000`
set BASE = `time_convert time=2004.01.01`
set BASE = `basename $BASE .000`
@ TLM = $UTs + 115215 - $BASE
echo $TLM

