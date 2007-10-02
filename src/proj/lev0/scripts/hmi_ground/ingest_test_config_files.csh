#! /bin/csh -f

# set echo

# Ingest HMI ground test configuration files from LMSAL
# Store into hmi_ground.test_config_files indexed by date and type
# version 2 for .usr files.

# Input directory is fixed
set DIR = /home/jsoc/hmi/ground/files.usr
set DIR_DONE = /home/jsoc/hmi/ground/files.usr_done

cd $DIR

set TYPES = (                   \
            pcu      PCU_POS	\
            fts_log  FTS_LOG    \
            )

# make unique temp filename
set NOW = `date "+%Y%m%d"`
set TMP = /tmp/ingest.$NOW.$$

# loop through the possible types to get and process files
set ntypes = $#TYPES
set itype = 1
while ($itype < $ntypes)
  set type = $itype
  @ suffix = $itype + 1
  @ itype = $itype + 2
  echo Looking for $TYPES[$suffix] files
  # get length of filename suffix, note \n gets counted and accounts for the _
  set FWIDE = `echo $TYPES[$suffix] | wc -c`
  # get list of files of current type
  /bin/ls 200*'_'$TYPES[$suffix].usr > $TMP
  set fcount = `wc -l <$TMP`
  if ($fcount == 0) then
    echo No Files Found
    continue
  endif
  # now ingest the files of current type
  foreach FILE (`cat $TMP`)
    # convert time from filename to JSOC time format
    set DATE = `echo $FILE | awk -v FIELDWIDTHS="4 2 2 1 2 2 2" '{printf("%s.%s.%s_%s:%s:%s",$1,$2,$3,$5,$6,$7)}'`
    # Store file in DRMS/SUMS
    set_keys -c ds=hmi_ground.test_config_files date=$DATE type=$TYPES[$type] file=$FILE 
    mv $FILE $DIR_DONE
    # set newfile = `show_keys "ds=hmi_ground.test_config_files[$DATE]" seg=file -p -q`
    # echo Saved file=$FILE date=$DATE type=$TYPES[$type] to=$newfile 
    # ls -l $newfile
    end
  end

rm -rf $TMP

