#!/bin/csh -f
# script to generate NetDRMS initialization task script

set LOCALINF = ./config.local
if (!(-e $LOCALINF)) then
  set WWW = http://jsoc.stanford.edu/netdrms
  echo "Error: local configuration file $LOCALINF not found"
  echo "You may create one from the template at:"
  echo "	$WWW/setup.html"
  exit
endif

# parse the local config file

set LOCAL_CONFIG_SET = `egrep "^LOCAL_CONFIG_SET" $LOCALINF | awk '{print $2}'`
set POSTGRES_ADMIN = `egrep "^POSTGRES_ADMIN" $LOCALINF | awk '{print $2}'`
set POSTGRES_LIBS = `egrep "^POSTGRES_LIBS" $LOCALINF | awk '{print $2}'`
set POSTGRES_INCS = `egrep "^POSTGRES_INCS" $LOCALINF | awk '{print $2}'`
set DBSERVER_HOST = `egrep "^DBSERVER_HOST" $LOCALINF | awk '{print $2}'`
set DRMS_DATABASE = `egrep "^DRMS_DATABASE" $LOCALINF | awk '{print $2}'`
set DRMS_SITE_CODE = `egrep "^DRMS_SITE_CODE" $LOCALINF | awk '{print $2}'`
set DRMS_SAMPLE_NAMESPACE = `egrep "^DRMS_SAMPLE_NAMESPACE" $LOCALINF | awk '{print $2}'`
set SUMS_SERVER_HOST = `egrep "^SUMS_SERVER_HOST" $LOCALINF | awk '{print $2}'`
set SUMS_LOG_BASEDIR = `egrep "^SUMS_LOG_BASEDIR" $LOCALINF | awk '{print $2}'`
set SUMS_MANAGER = `egrep "^SUMS_MANAGER" $LOCALINF | awk '{print $2}'`
set SUMS_TAPE_AVAILABLE = `egrep "^SUMS_TAPE_AVAILABLE" $LOCALINF | awk '{print $2}'`
set THIRD_PARTY_LIBS = `egrep "^THIRD_PARTY_LIBS" $LOCALINF | awk '{print $2}'`
set THIRD_PARTY_INCS = `egrep "^THIRD_PARTY_INCS" $LOCALINF | awk '{print $2}'`

# check that local config file has been edited appropriately
if ($#LOCAL_CONFIG_SET == 1) then
  if ($LOCAL_CONFIG_SET =~ "NO") then
    echo "Error: local configuration file $LOCALINF must be edited"
    echo "  After editing the file appropriately, rerun this script ($0)"
    exit
  endif
endif

if ($#SUMS_LOG_BASEDIR != 1) then
  echo "Error: $SUMS_LOG_BASEDIR undefined in local configuration file $LOCALINF"
  exit
endif
if ($#SUMS_MANAGER != 1) then
  echo "Error: SUMS_MANAGER undefined in local configuration file $LOCALINF"
  exit
endif

set SUMRM_CONFIG = $SUMS_LOG_BASEDIR/sum_rm.cfg
if (-e $SUMRM_CONFIG) then
  echo "A sum_rm.cf configuration file already exists in $SUMS_LOG_BASEDIR"
  echo "  Edit it at any time to modify the configuration"
  exit
endif

cat /dev/null > $SUMRM_CONFIG
if ($status) then
  echo "Error: either the directory $SUMS_LOG_BASEDIR does not exist, or"
  echo "  you do not have write permission in it; the directory should be"
  echo "  created or made writeable, and this script run by user $SUMS_MANAGER"
  exit
endif

echo "# configuration file for sum_rm program" >> $SUMRM_CONFIG
echo "#" >> $SUMRM_CONFIG
echo "# You may edit this file any time, it is read each time sum_rm is run," >> $SUMRM_CONFIG
echo "# which will occur at the intervals specified by the SLEEP parameter" >> $SUMRM_CONFIG
echo "#" >> $SUMRM_CONFIG
echo "# when sum_rm finishes, sleep for n seconds before re-running" >> $SUMRM_CONFIG
echo "SLEEP=3600" >> $SUMRM_CONFIG
echo "# delete until this many Megabytes are free on the specified SUMS disk" >> $SUMRM_CONFIG
echo "# partitions (one entry, numbered 0-n, for each partition" >> $SUMRM_CONFIG
echo "MAX_FREE_0=100000" >> $SUMRM_CONFIG
echo "# name of the log file (opened at startup; date and pid are appended to this" >> $SUMRM_CONFIG
echo "# name); do not change the directory without changing SUMS_LOG_BASEDIR in" >> $SUMRM_CONFIG
echo "# the config.local and rebuilding sums" >> $SUMRM_CONFIG
echo "LOG=$SUMS_LOG_BASEDIR/sum_rm.log" >> $SUMRM_CONFIG
echo "# whom to bother when there's a notable problem" >> $SUMRM_CONFIG
echo "MAIL=$SUMS_MANAGER" >> $SUMRM_CONFIG
echo "# to prevent sum_rm from doing anything set non-zero (for testing)" >> $SUMRM_CONFIG
echo "NOOP=0" >> $SUMRM_CONFIG
echo "# sum_rm can only be enabled for a single user" >> $SUMRM_CONFIG
echo "USER=$SUMS_MANAGER" >> $SUMRM_CONFIG
echo "# dont run sum_rm between these NORUN hours of the day (0-23)" >> $SUMRM_CONFIG
echo "# comment out to ignore or set them both to the same hour" >> $SUMRM_CONFIG
echo "# The NORUN_STOP must be >= NORUN_START" >> $SUMRM_CONFIG
echo "# dont run when the hour first hits NORUN_START" >> $SUMRM_CONFIG
echo "NORUN_START=7" >> $SUMRM_CONFIG
echo "# start running again when the hour first hits NORUN_STOP" >> $SUMRM_CONFIG
echo "NORUN_STOP=7" >> $SUMRM_CONFIG

echo "A sum_rm.cf configuration file with default values has been generated"
echo "  in $SUMS_LOG_BASEDIR"
echo "Review and edit the values as appropriate; in particular, if there is"
echo "  more than one SUMS disk partition, add the appropriate number of"
echo "  MAX_FREE_n lines and values"
