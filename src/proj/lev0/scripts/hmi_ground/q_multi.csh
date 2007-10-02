#

set BATCHSIZE = 500
# set echo 
ingest_test_config_files.csh

set f = $1
set l = $2
set set = 1
set LEVEL
if ($#argv == 3) then
  set LEVEL = $3
  endif

while ($f <= $l)
  set tl = $l
  @ n = $tl - $f
  @ n = $n + 1
  if ($n > $BATCHSIZE) then
    @ tl = $f + $BATCHSIZE - 1
  endif
  drms_run -lifetime 30 q_import_lev0_CIF_from_DSDS.csh $f $tl $LEVEL >& set.$set.$$ &
  @ set = $set + 1
  @ f = $tl + 1
end

wait
 
  
