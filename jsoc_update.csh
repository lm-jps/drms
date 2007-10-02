#! /bin/csh -f

# script for updating jsoc cvs release in my "sandbox"

# run this on each machine to be used.
#    n02 - for linux_X86_64 machines
#    n00 - for linux4 machines such as n00, phil, etc.
# 
#    n12 formerly used for AMD x86-64 can also be used instead of n02
#    lws Itaniam processors no longer supported for JSOC

set LCMD = "R" # latest release, default is latest source
set REV = ""
set LATESTREL = "Ver_LATEST"

foreach ARG ($argv)
    set FLAG = `echo $ARG | awk '{print substr($0, 2)}'`
    if ($FLAG == $LCMD) then
      set REV = "-r $LATESTREL"
    endif
end

set UPDATE = "cvs update -APd $REV"
set CVSLOG =  "cvsupdate.log"

cd $JSOCROOT

echo "####### Start cvs update ####################"
echo $UPDATE ">&" $CVSLOG

$UPDATE >& $CVSLOG

echo "##"
echo "## cvs update done, now check cvsupdate.log for any files with a 'C' status."
echo "## A scan of cvsupdate.log for files with conflicts follows:"
echo "## Start scanning cvsupdate.log"
grep "^C " cvsupdate.log
echo "## Done scanning cvsupdate.log"
echo "## Any lines starting with a 'C' between the 'Start' and 'Done' lines above should be fixed." 
echo "## See release notes to deal with 'C' status conflicts."
echo "##" 
echo "## Now Check cvsstatus for files that should be in the release ##"
echo "####### Start checking status ####################"

alias cvsstatus '$JSOCROOT/scripts/cvsstatus.pl'
cvsstatus

echo "####### Done checking status ####################"
echo "## If no lines between the 'Start' and 'Done' lines then there are no cvsstatus problems"
echo "## Continue with 'fg' when ready."
suspend

./configure 

echo "start build on linux_X86_64"
ssh n02 $JSOCROOT/make_jsoc.csh >& make_jsoc_linux_X86_64.log  
echo "done on linux_X86_64"

#echo "start build on linux_ia64"
#ssh lws $JSOCROOT/make_jsoc.csh >& ./make_jsoc_linux_ia64.log
#echo "done on linux_ia64"

echo "start build on linux_ia32"
ssh n00 $JSOCROOT/make_jsoc.csh >& make_jsoc_linux_ia32.log 
echo "done on linux_ia32"

echo "JSOC update Finished."

