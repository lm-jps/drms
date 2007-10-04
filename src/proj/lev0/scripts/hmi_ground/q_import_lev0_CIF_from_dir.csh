#! /bin/csh -f

# call with tape dir and range of "filexxx" numbers for directory names in <tapedir>
# $2 should be first and $3 should be final.  All directories in that
# range with names "filexxx" will be examined for a file named "00*.fits" and
# if found, the FSN will be taken from the fits file name and it will be processed.
# e.g.   import_lev0_CIF_from_tmp.csh tape_070102 58 59

# set echo
 
set MAX_IN_QUEUE = 200
set JSROOT = /home/jsoc/hmi/ground
set SCRIPTS = $JSOCROOT/src/proj/lev0/scripts/hmi_ground

set BATCHDIR = "$JSROOT/batch_jobs/$HOST.$$"
mkdir $BATCHDIR

#adding in ingest_test_config_files.csh to ingest user files -Hao
echo "Running $SCRIPTS/ingest_test_config_files.csh\n"

$SCRIPTS/ingest_test_config_files.csh

echo $0 $* >>$JSROOT/logs/log_run_q

set TAPEDIR = $1
shift

set FFN = $1
set LFFN = $FFN
shift 

if ($#argv >= 1) then
  set LFFN = $1
  shift
endif

# setup for CIF 
set PROG = hmi_ground
set LEVEL = lev0
set SERIES = egse_hmifsfm
set FLAGS = "fsn_key=FSN keymap=/home/jsoc/hmi/ground/jsd/lev0.keymap"

set FN = $FFN
while ($FN <= $LFFN)
  set DNAME = "file"$FN
  set LEV0_PATH = $TAPEDIR/$DNAME
  if (-d $LEV0_PATH) then
    set FITSNAME = `/bin/ls $LEV0_PATH/00*.fits`
    if ($status == 0) then
      if (-e $FITSNAME) then
        set FITSNM = `basename $FITSNAME`
        set FSN = `basename $FITSNM .fits`
        @ FSN = $FSN
        set TRIGGER = "$BATCHDIR/$FSN"
        touch $TRIGGER
        $SCRIPTS/q_import_lev0_CIF_from_file.csh $TRIGGER $FITSNAME
      else
        echo No file found $FITSNAME
      endif
    endif
  else
    echo " "
    echo No dir found $LEV0_PATH
  endif
  @ FN = $FN + 1
# pause here if the cluster queue is too full.
  set inQ = `qstat | grep $USER | wc -l`
  while ($inQ > $MAX_IN_QUEUE)
    echo Queue has $inQ $USER jobs: sleep a bit
    sleep 10
    set inQ = `qstat | grep $USER | wc -l`
  end

end


# now wait until all jobs are done
set N_IN_Q = 1
while ($N_IN_Q > 0)
  set N_IN_Q = `/bin/ls $BATCHDIR | wc -l`
  sleep 2
echo $N_IN_Q
end

rm -rf $BATCHDIR

set FSN = $FFSN
while ($FSN <= $LFSN)
  set QLOG = $JSROOT/logs/log.$FSN
  set OK = `grep -c "import done.*.fits" $QLOG`
  if ($OK > 0) then
    echo -n "     "
    grep  "import done" $QLOG
  else
    echo "XXX Failed $FSN, see $QLOG"
  endif
  @ FSN = $FSN + 1
end
