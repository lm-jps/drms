#!/bin/csh

# save path
set cp = `pwd`

# actual bits
set scriptPath = "/home/jsoc/sdo"
set lzpDataPath = "/home/jsoc/sdo/lzp"
set cmdStr = "$scriptPath/dlMOCDataFiles.pl -c $lzpDataPath/mocDlLzpSpec.txt -s $lzpDataPath/mocDlLzpStatus.txt -r $lzpDataPath/MOCFiles/ -t 120 $1"

cd $lzpDataPath
perl $cmdStr

# restore path
cd $cp
