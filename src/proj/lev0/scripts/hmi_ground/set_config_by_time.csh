#! /bin/csh -f
set noglob
 set echo

# set_config_by_time {T_OBS} {FSN} {DRMSSESSION}

# modified 24 Jan 07 to do PCU first then ann FTS params.

#  source of config files, assume index keys: type, date, and segment: file
set CONFIG_FILES = hmi_ground.test_config_files

#  target series to update.  assume index key fsn, and other keys: date and keys from config files.
set CONFIG_SERIES = hmi_ground.lev0_config

# get time and FSN for this image
set IMAGE_TIME = $1
set FSN = $2
if ($#argv == 3) then
  setenv DRMSSESSION $3
endif

set EXITSTAT = 0
set IMAGE_SECS = `time_convert time=$IMAGE_TIME`

# make temp file name
set NOW = `date "+%Y%m%d_%H%M%S"`
set TMP = /tmp/fsn_$NOW"_"$$
mkdir $TMP

# First get the "PCU" file with all keywords that is just before the image time
# get filename for "PCU_LOG" config file at or just before this image time
set PCU_INFO = `show_keys "ds=hmi_ground.test_config_files[? recnum = (select recnum from hmi_ground.test_config_files where type = 'pcu'  and date <= $IMAGE_SECS order by date desc, recnum desc limit 1) ?]" -p -q key=date seg=file `

# Now have PCU params 
set pcu_time = `time_convert time=$PCU_INFO[1]`
cp $PCU_INFO[2] $TMP/data
# Add ".." around each line to preserve imbedded blanks in strings
awk '{printf("\"%s\"\n",$0)}' <$TMP/data >$TMP/params
# Set PCU params in a new record:
set_keys -c ds=$CONFIG_SERIES FSN=$FSN T_OBS=$IMAGE_TIME PCU_FILE=$PCU_INFO[2]  @$TMP/params

@ EXITSTAT = $EXITSTAT + $status

# now get FTS params
set FTS_INFO = `show_keys "ds=hmi_ground.test_config_files[? recnum = (select recnum from hmi_ground.test_config_files where type = 'fts_log'  and date <= $IMAGE_SECS order by date desc, recnum desc limit 1) ?]" -p -q key=date seg=file `
if ($#FTS_INFO != 2) then
        echo Problem, no config data found for $IMAGE_TIME for FTS_LOG, $FTS_INFO
        exit 1
endif

# Now have FTS params 
set fts_time = `time_convert time=$FTS_INFO[1]`

set newer_fts = `echo $fts_time ">=" $pcu_time | bc -l`
if ($newer_fts == 1) then
  cp $FTS_INFO[2] $TMP/data
  # Add "... " around each line to preserve imbedded blanks in strings
  awk '{printf("\"%s\"\n",$0)}' <$TMP/data >$TMP/params
  # echo Set FTS params:
  set_keys "ds="$CONFIG_SERIES"["$FSN"]"  FTS_FILE=$FTS_INFO[2] @$TMP/params
  @ EXITSTAT = $EXITSTAT + $status
endif

rm -rf $TMP
exit $EXITSTAT
