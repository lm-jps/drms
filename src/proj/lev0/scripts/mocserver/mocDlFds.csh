#!/bin/csh

# save path
set cp = `pwd`

# actual bits
set scriptPath = "/home/jsoc/sdo"
set fdsDataPath = "/home/jsoc/sdo/fds"
set cmdStr = "$scriptPath/dlMOCDataFiles.pl -c $fdsDataPath/mocDlFdsSpec.txt -s $fdsDataPath/mocDlFdsStatus.txt -r $fdsDataPath/MOCFiles/ -t 30 $1"

cd $fdsDataPath
perl $cmdStr

# restore path
cd $cp

