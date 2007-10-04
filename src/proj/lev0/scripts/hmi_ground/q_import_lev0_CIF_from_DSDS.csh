#! /bin/csh -f

# set echo

# call with FSN range as 1 or 2 numbers then   LEVSN range if needed as 0, 1, or 2 numbers.

set noglob

set MAX_IN_QUEUE = 200
set JSROOT = /home/jsoc/hmi/ground
set SCRIPTS = $JSOCROOT/src/proj/lev0/scripts/hmi_ground
set PEQ = /home/phil/bin/_$MACHINE/PEQ

set BATCHDIR = "$JSROOT/batch_jobs/$HOST.$$"
mkdir $BATCHDIR

#adding in ingest_test_config_files.csh to ingest user files -Hao
echo "Running $SCRIPTS/ingest_test_config_files.csh\n"

$SCRIPTS/ingest_test_config_files.csh

echo $0 $* >>$JSROOT/logs/log_run_q

echo "BEGIN " $0 $*


set FFSN = $1
set LFSN = $FFSN
shift 

if ($#argv >= 1) then
  set LFSN = $1
  shift
endif

set LEVNO = 0
set FLEV = 1
set LLEV = 1
if ($#argv >= 1) then
  set FLEV = $1
  set LEVNO = 1
  set LLEV = $FLEV
  shift
endif

if ($#argv >= 1) then
  set LLEV = $1
  shift
endif

# setup for CIF 
set PROG = hmi_ground
set LEVEL = lev0
set SERIES = egse_hmifsfm
set FLAGS = "fsn_key=FSN keymap=/home/jsoc/hmi/ground/jsd/lev0.keymap"

#stage data to disk
if ($LEVNO > 0) then
  set LEVRANGE = "["$FLEV"-"$LLEV"]"
else
  set LEVRANGE = ""
endif
set FSN = $FFSN
while ($FSN <= $LFSN)
  @ LSN = $FSN + 299
  if ($LSN > $LFSN) then
    set LSN = $LFSN
  endif
#  echo Staging data with peq -A -t5  "prog:"$PROG",level:"$LEVEL$LEVRANGE",series:"$SERIES"["$FSN"-"$LSN"]"
#  peq -A -t5 -w  "prog:"$PROG",level:"$LEVEL$LEVRANGE",series:"$SERIES"["$FSN"-"$LSN"]"
  @ FSN = $LSN + 1
end

set FSN = $FFSN
while ($FSN <= $LFSN)
  set LEV = $FLEV
  while ($LEV <= $LLEV)
    if ($LEVNO > 0) then
      set LEVSPEC = "["$LEV"]"
    else
      set LEVSPEC = ""
    endif
    set DSDSNAME = $PROG","$LEVEL"$LEVSPEC"","$SERIES"["$FSN"]"
    set DSDSFULLNAME = "prog:"$PROG",level:"$LEVEL$LEVSPEC",series:"$SERIES"["$FSN"]"
    set LEV0_PATH = `$PEQ $DSDSFULLNAME`
    if ($LEV0_PATH[1] == "###") then
	echo "$DSDSNAME" not found
        echo "import done: No DSDS data fits found for FSN=$FSN" > $JSROOT/logs/log.$FSN
    else
        set FITSNAME = `printf "%09d" $FSN`
        set TRIGGER = $BATCHDIR/$FSN"_"$LEV
        touch $TRIGGER
        $SCRIPTS/q_import_lev0_CIF_from_file.csh $TRIGGER  $LEV0_PATH/$FITSNAME.fits
    endif
    @ LEV = $LEV + 1
  end
  @ FSN = $FSN + 1
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

