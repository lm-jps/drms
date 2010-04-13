#! /bin/csh -f
# this should be run on cl1n001
# remove the file touched below to stop the script.

touch /home/phil/get_dcs_times_keeprunning

while (1)
  if (!(-e /home/phil/get_dcs_times_keeprunning)) then
    break
  endif
  set HMI = `/bin/ls -L -lt /dds/soc2pipe/hmi | head -2 | tail -1`
  set AIA = `/bin/ls -L -lt /dds/soc2pipe/aia | head -2 | tail -1`
  set HMImt = `stat -L -c '%Y' /dds/soc2pipe/hmi` 
  set AIAmt = `stat -L -c '%Y' /dds/soc2pipe/aia` 
  set ONTIME
  foreach VC (VC01 VC02 VC04 VC05)
    set PSLINE = `ps -ef | grep ingest_lev0 | grep "^388"| grep $VC`
    if ($#PSLINE > 5) then
      set ONTIME = ( $ONTIME $PSLINE[5] )
    else
      set ONTIME = ( $ONTIME OFF)
    endif
    end

  set TIMES = /home/jsoc/public_html/dcs_times
  echo '{' > $TIMES.wrk
  echo '"hmilast":"'$HMI[8]'",' >> $TIMES.wrk
  echo '"aialast":"'$AIA[8]'",' >> $TIMES.wrk
  echo '"hmimt":"'$HMImt'",' >> $TIMES.wrk
  echo '"aiamt":"'$AIAmt'",' >> $TIMES.wrk
  echo '"VC01":"'$ONTIME[1]'",' >> $TIMES.wrk
  echo '"VC02":"'$ONTIME[2]'",' >> $TIMES.wrk
  echo '"VC04":"'$ONTIME[3]'",' >> $TIMES.wrk
  echo '"VC05":"'$ONTIME[4]'",' >> $TIMES.wrk
  echo '}' >> $TIMES.wrk
  mv $TIMES.wrk $TIMES.json
  sleep 60
end

