#! /bin/csh 


# call with sunum of an older existing DRMS record.
# This version is made to retrieve lost files.

# e.g.   import_lev0_CIF_from_sunum.csh 717824

# set echo
set sunum = $1
set recnum = `echo "select  recnum from hmi_ground.lev0 where sunum = $1;\q" | psql -h hmidb.stanford.edu jsoc | head -3 | tail -1`
set FSN = `echo "select  FSN from hmi_ground.lev0 where sunum = $1;\q" | psql -h hmidb.stanford.edu jsoc | head -3 | tail -1`
echo  Re-ingesting:
show_keys ds="hmi_ground.lev0[$FSN][:#$recnum]" -p key=FSN seg=file -q -k

set LEV0_PATH = `show_keys ds="hmi_ground.lev0[$FSN][:#$recnum]" -p seg=file -q`
set TRIGGER = /home/jsoc/hmi/ground/reingest/$sunum.running
touch $TRIGGER

 
set TARGET = hmi_ground.lev0

set JSROOT = /home/jsoc/hmi/ground
set KEYMAPS = $JSOCROOT/src/proj/lev0/scripts/hmi_ground

echo $0 $* >>$JSROOT/logs/log_run_q

# setup for CIF WITH Re-ingest flag
set FLAGS = "-R  -L -V fsn_key=FSN keymap=$KEYMAPS/lev0.keymap"

set FITSNAME = $LEV0_PATH
if (-e $FITSNAME) then
      set FITSNAME = `basename $FITSNAME`
      set FSN = `basename $FITSNAME .fits`
      @ FSN = $FSN
      set DSDSNAME = $LEV0_PATH
# now ready to run command, write to script for qsub
      echo " "
      echo Queue $LEV0_PATH 
      set QCMD = $JSROOT/qscripts/"FSN_"$FSN
      if ( -e $QCMD ) then 
        rm -f $QCMD
      endif
      set QLOG = $JSROOT/logs/log.$FSN
      if ( -e $QLOG ) then 
        rm -f $QLOG
      endif
# make script
      echo "#! /bin/csh -f"					 >$QCMD
      echo "set noglob"						>>$QCMD
      echo "cd /home/jsoc/hmi/ground/scripts"			>>$QCMD
      echo "echo Do $LEV0_PATH 			  >&$QLOG" 	>>$QCMD
      echo "unsetenv JSOC_MACHINE                >>&$QLOG"      >>$QCMD
      echo "source $HOME/.sunrc             >>&$QLOG"      >>$QCMD
      echo "source /home/jsoc/.setJSOCenv        >>&$QLOG"      >>$QCMD
      echo "hostname				 >>&$QLOG"	>>$QCMD
      echo "echo -n 'run at '			 >>&$QLOG" 	>>$QCMD
      echo "date				 >>&$QLOG" 	>>$QCMD
      echo "hmi_import_egse_lev0  in=$LEV0_PATH out=$TARGET dsds=$DSDSNAME $FLAGS  >>&$QLOG" >>$QCMD
      echo "echo -n import done:		 >>&$QLOG" 	>>$QCMD
      echo "show_keys ds=hmi_ground.lev0[$FSN] key=FSN,T_OBS seg=file -p -q >>&$QLOG" >>$QCMD
      echo "rm  $TRIGGER			 >>&$QLOG" 	>>$QCMD
# execute script in queue
       # qsub -r y -V -q x.q,o.q -o $QLOG -e $QLOG $QCMD
       # qsub -r y -V -q x.q -o $QLOG -e $QLOG $QCMD
        qsub -r y -V -q o.q -o $QLOG -e $QLOG $QCMD
else
      echo " "
      echo No file found in $LEV0_PATH
endif


