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
set DBSERVER_HOST = `egrep "^DBSERVER_HOST" $LOCALINF | awk '{print $2}'`
set DRMS_DATABASE = `egrep "^DRMS_DATABASE" $LOCALINF | awk '{print $2}'`
set DRMS_SITE_CODE = `egrep "^DRMS_SITE_CODE" $LOCALINF | awk '{print $2}'`
set DRMS_SAMPLE_NAMESPACE = `egrep "^DRMS_SAMPLE_NAMESPACE" $LOCALINF | awk '{print $2}'`
set POSTGRES_ADMIN = `egrep "^POSTGRES_ADMIN" $LOCALINF | awk '{print $2}'`
set SLONY_ADMIN = `egrep "^SLONY_ADMIN" $LOCALINF | awk '{print $2}'`
set SLONY_LOG_BASEDIR = `egrep "^SLONY_LOG_BASEDIR" $LOCALINF | awk '{print $2}'`
set SLONY_NOTIFY = `egrep "^SLONY_NOTIFY" $LOCALINF | awk '{print $2}'`
set SUMS_SERVER_HOST = `egrep "^SUMS_SERVER_HOST" $LOCALINF | awk '{print $2}'`
set SUMS_LOG_BASEDIR = `egrep "^SUMS_LOG_BASEDIR" $LOCALINF | awk '{print $2}'`
set SUMS_BIN_BASEDIR = `egrep "^SUMS_BIN_BASEDIR" $LOCALINF | awk '{print $2}'`
set SUMS_MANAGER = `egrep "^SUMS_MANAGER" $LOCALINF | awk '{print $2}'`
set SUMS_GROUP = `egrep "^SUMS_GROUP" $LOCALINF | awk '{print $2}'`
set SUMS_TAPE_AVAILABLE = `egrep "^SUMS_TAPE_AVAILABLE" $LOCALINF | awk '{print $2}'`
set SUMEXP_METHFMT = `perl -n -e 'if ($_ =~ /^SUMEXP_METHFMT\s+(.+)/) { print $1; }' $LOCALINF`
set SUMEXP_USERFMT = `perl -n -e 'if ($_ =~ /^SUMEXP_USERFMT\s+(.+)/) { print $1; }' $LOCALINF`
set SUMEXP_HOSTFMT = `perl -n -e 'if ($_ =~ /^SUMEXP_HOSTFMT\s+(.+)/) { print $1; }' $LOCALINF`
set SUMEXP_PORTFMT = `perl -n -e 'if ($_ =~ /^SUMEXP_PORTFMT\s+(.+)/) { print $1; }' $LOCALINF`

# check that local config file has been edited appropriately
if ($#LOCAL_CONFIG_SET == 1) then
  if ($LOCAL_CONFIG_SET =~ "NO") then
    echo "Error: local configuration file $LOCALINF must be edited"
    echo "  After editing the file appropriately, rerun this script ($0)"
    exit
  endif
endif

@ ADMIN_STATUS = 0
if ($#DBSERVER_HOST != 1) then
  echo "Error: DBSERVER_HOST undefined in local configuration file $LOCALINF"
  exit
endif
if ($#DRMS_DATABASE != 1) then
  echo "Error: DRMS_DATABASE undefined in local configuration file $LOCALINF"
  exit
endif
if ($#DRMS_SITE_CODE != 1) then
  echo "Error: DRMS_SITE_CODE undefined in local configuration file $LOCALINF"
  exit
endif
if ($#DRMS_SAMPLE_NAMESPACE != 1) then
  echo "Warning: DRMS_SAMPLE_NAMESPACE undefined in local configuration file $LOCALINF"
  @ ADMIN_STATUS = 1
endif
if ($#POSTGRES_ADMIN != 1) then
  echo "Error: POSTGRES_ADMIN undefined in local configuration file $LOCALINF"
  exit
endif
if ($#SLONY_ADMIN != 1) then
  echo "Warning: SLONY_ADMIN undefined in local configuration file $LOCALINF"
  @ ADMIN_STATUS = 1
endif
if ($#SLONY_LOG_BASEDIR != 1) then
  echo "Warning: $SLONY_LOG_BASEDIR undefined in local configuration file $LOCALINF"
  @ ADMIN_STATUS = 1
endif
if ($#SLONY_NOTIFY != 1) then
  echo "Warning: SLONY_NOTIFY undefined in local configuration file $LOCALINF"
  @ ADMIN_STATUS = 1
endif
if ($#SUMS_SERVER_HOST != 1) then
  echo "Error: $SUMS_SERVER_HOST undefined in local configuration file $LOCALINF"
  exit
endif
if ($#SUMS_LOG_BASEDIR != 1) then
  echo "Error: $SUMS_LOG_BASEDIR undefined in local configuration file $LOCALINF"
  exit
endif
if ($#SUMS_BIN_BASEDIR != 1) then
  echo "Error: $SUMS_BIN_BASEDIR undefined in local configuration file $LOCALINF"
  exit
endif
if ($#SUMS_MANAGER != 1) then
  echo "Error: SUMS_MANAGER undefined in local configuration file $LOCALINF"
  exit
endif
if ($#SUMS_GROUP != 1) then
  echo "Error: SUMS_GROUP undefined in local configuration file $LOCALINF"
  exit
endif
if ($#SUMS_TAPE_AVAILABLE != 1) then
  echo "Error: SUMS_TAPE_AVAILABLE undefined in local configuration file $LOCALINF"
  exit
endif

# generate script drms_series.sql
set SCRIPT = scripts/drms_series.sql
echo "*** generating $SCRIPT ***"
cat /dev/null > $SCRIPT
echo "CREATE OR REPLACE FUNCTION drms_series() RETURNS SETOF $DRMS_SAMPLE_NAMESPACE.drms_series AS "'$$' >> $SCRIPT
echo "DECLARE" >> $SCRIPT
echo "  ns  RECORD;" >> $SCRIPT
echo "  rec RECORD;" >> $SCRIPT
echo "  next_row REFCURSOR;" >> $SCRIPT
echo "BEGIN" >> $SCRIPT
echo "  FOR ns IN SELECT name || '.drms_series' as tn FROM admin.ns order by name LOOP" >> $SCRIPT
echo "     OPEN next_row FOR EXECUTE 'SELECT * FROM ' || ns.tn;" >> $SCRIPT
echo "     LOOP" >> $SCRIPT
echo "       FETCH next_row INTO rec;" >> $SCRIPT
echo "       IF NOT FOUND THEN" >> $SCRIPT
echo "          EXIT;" >> $SCRIPT
echo "       END IF;" >> $SCRIPT
echo "       RETURN NEXT rec;" >> $SCRIPT
echo "     END LOOP;" >> $SCRIPT
echo "     CLOSE next_row;" >> $SCRIPT
echo "  END LOOP;" >> $SCRIPT
echo "  RETURN;" >> $SCRIPT
echo "END;" >> $SCRIPT
echo '$$' >> $SCRIPT
echo "LANGUAGE plpgsql;" >> $SCRIPT

# generate script drms_session.sql
set SCRIPT = scripts/drms_session.sql
echo "*** generating $SCRIPT ***"
cat /dev/null > $SCRIPT
echo "CREATE OR REPLACE FUNCTION drms_session() RETURNS SETOF $DRMS_SAMPLE_NAMESPACE.drms_session AS "'$$' >> $SCRIPT
echo "DECLARE" >> $SCRIPT
echo "  ns  RECORD;" >> $SCRIPT
echo "  rec RECORD;" >> $SCRIPT
echo "  next_row REFCURSOR;" >> $SCRIPT
echo "BEGIN" >> $SCRIPT
echo "  FOR ns IN SELECT name as tn FROM admin.ns order by name LOOP" >> $SCRIPT
echo "     OPEN next_row FOR EXECUTE 'SELECT * FROM ' || ns.tn || '.drms_session';" >> $SCRIPT
echo "     LOOP" >> $SCRIPT
echo "       FETCH next_row INTO rec;" >> $SCRIPT
echo "       rec.username := rec.username || '(' || ns.tn || ')';" >> $SCRIPT
echo "       IF NOT FOUND THEN" >> $SCRIPT
echo "          EXIT;" >> $SCRIPT
echo "       END IF;" >> $SCRIPT
echo "       RETURN NEXT rec;" >> $SCRIPT
echo "     END LOOP;" >> $SCRIPT
echo "     CLOSE next_row;" >> $SCRIPT
echo "  END LOOP;"  >> $SCRIPT
echo "  RETURN;" >> $SCRIPT
echo "END;" >> $SCRIPT
echo '$$' >> $SCRIPT
echo "LANGUAGE plpgsql;" >> $SCRIPT

# generate script create_sumindex.sql
set SCRIPT = scripts/create_sumindex.sql
echo "*** generating $SCRIPT ***"
cc -o seed_sums seed_sums.c
if (-x ./seed_sums) then
  ./seed_sums $DRMS_SITE_CODE > $SCRIPT
  rm seed_sums
else
  echo "Error creating script"
  echo "  compile the seed_sums.c program and run it with $DRMS_SITE_CODE as the argument"
endif

@ SUMS_TAPE_AVAIL = $SUMS_TAPE_AVAILABLE

# find UID of SUMS_MANAGER
cc -o getuid getuid.c
if (-x ./getuid) then
  @ SUMS_MANAGER_UID = `./getuid $SUMS_MANAGER`
  rm getuid
  if ($SUMS_MANAGER_UID < 0) then
    echo "Error: no such user $SUMS_MANAGER"
    echo "  Either create the account or modify $LOCALINF"
    exit
  endif
else
  echo "Error: unable to generate getuid program"
  echo "  You must edit the file $SCRIPT to add the line"
  echo '#define SUMS_MANAGER_UID'"        \(uid\)"
  echo "  where uid is the uid of the $SUMS_MANAGER user"
endif

# generate file localization.h
set SCRIPT = base/include/localization.h
echo "*** generating $SCRIPT ***"
cat /dev/null > $SCRIPT
echo '#ifndef __LOCALIZATION_H' >> $SCRIPT
echo '#define __LOCALIZATION_H' >> $SCRIPT
echo '#define SERVER			"'$DBSERVER_HOST'"' >> $SCRIPT
echo '#define DBNAME			"'$DRMS_DATABASE'"' >> $SCRIPT
echo '#define DRMS_LOCAL_SITE_CODE	'$DRMS_SITE_CODE >> $SCRIPT
echo '#define POSTGRES_ADMIN		"'$POSTGRES_ADMIN'"' >> $SCRIPT
echo '#define USER			NULL' >> $SCRIPT
echo '#define PASSWD			NULL' >> $SCRIPT
echo '#define SUMSERVER		"'$SUMS_SERVER_HOST'"' >> $SCRIPT
echo '#define SUMS_MANAGER		"'$SUMS_MANAGER'"' >> $SCRIPT
echo '#define SUMS_MANAGER_UID		"'$SUMS_MANAGER_UID'"' >> $SCRIPT
echo '#define SUMS_GROUP		"'$SUMS_GROUP'"' >> $SCRIPT
echo '#define SUMLOG_BASEDIR		"'$SUMS_LOG_BASEDIR'"' >> $SCRIPT
echo '#define SUMBIN_BASEDIR		"'$SUMS_BIN_BASEDIR'"' >> $SCRIPT
echo '#define SUMS_TAPE_AVAILABLE    '\($SUMS_TAPE_AVAIL\)'' >> $SCRIPT
if ($#SUMEXP_METHFMT) then 
  echo '#define LOC_SUMEXP_METHFMT	'$SUMEXP_METHFMT >> $SCRIPT
endif
if ($#SUMEXP_USERFMT) then
  echo '#define LOC_SUMEXP_USERFMT	'$SUMEXP_USERFMT  >> $SCRIPT
endif
if ($#SUMEXP_HOSTFMT) then
  echo '#define LOC_SUMEXP_HOSTFMT	'$SUMEXP_HOSTFMT  >> $SCRIPT
endif
if ($#SUMEXP_PORTFMT) then
  echo '#define LOC_SUMEXP_PORTFMT	'$SUMEXP_PORTFMT  >> $SCRIPT
endif
echo '#endif' >> $SCRIPT

# modify targets as appropriate in custom.mk
# don't do anything here that will modify custom.mk. Another script
# does that.

if ($ADMIN_STATUS) then
  echo
  echo "Some configuration parameters were missing from the config.local file"
  echo "  as described in warning messages above.  These are only needed for"
  echo "  SUMS or Slony administration; they are not needed for an ordinary build."
  echo "Check the file config.local.template for sample entries to put in your"
  echo "  config.local file."
endif
