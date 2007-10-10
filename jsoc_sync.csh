#! /bin/csh -f

# script for synchronizing your CVS working directory with the CVS JSOC module (new tree)

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

if (-e $JSOCROOT/suflag.txt) then
    set CVSMOD = "JSOC"
    echo "Synchronizing to JSOC (Stanford) user"
else
    set CVSMOD = "DRMS"
    echo "Synchronizing to DRM (base system only) user"
endif

set UPDATE = "cvs checkout -AP $REV $CVSMOD"
set CVSLOG = "cvsupdate.log"

cd ..
set wd = `pwd`

echo "Calling" $UPDATE ">& JSOC/"$CVSLOG "from $wd"
$UPDATE >& JSOC/$CVSLOG

echo "JSOC synchronization finished."
