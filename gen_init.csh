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

if ($#POSTGRES_ADMIN != 1) then
  echo "Error: POSTGRES_ADMIN undefined in local configuration file $LOCALINF"
  exit
endif
if ($#POSTGRES_LIBS != 1) then
  echo "Error: POSTGRES_LIBS undefined in local configuration file $LOCALINF"
  exit
endif
if ($#POSTGRES_INCS != 1) then
  echo "Error: POSTGRES_INCS undefined in local configuration file $LOCALINF"
  exit
endif
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
  echo "Error: DRMS_SAMPLE_NAMESPACE undefined in local configuration file $LOCALINF"
  exit
endif
if ($#SUMS_SERVER_HOST != 1) then
  echo "Error: $SUMS_SERVER_HOST undefined in local configuration file $LOCALINF"
  exit
endif
if ($#SUMS_LOG_BASEDIR != 1) then
  echo "Error: $SUMS_LOG_BASEDIR undefined in local configuration file $LOCALINF"
  exit
endif
if ($#SUMS_MANAGER != 1) then
  echo "Error: SUMS_MANAGER undefined in local configuration file $LOCALINF"
  exit
endif
if ($#SUMS_TAPE_AVAILABLE != 1) then
  echo "Error: SUMS_TAPE_AVAILABLE undefined in local configuration file $LOCALINF"
  exit
endif
if ($#THIRD_PARTY_LIBS != 1) then
  echo "Error: THIRD_PARTY_LIBS undefined in local configuration file $LOCALINF"
  exit
endif
if ($#THIRD_PARTY_INCS != 1) then
  echo "Error: THIRD_PARTY_INCS undefined in local configuration file $LOCALINF"
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
echo '#define SUMSERVER			"'$SUMS_SERVER_HOST'"' >> $SCRIPT
echo '#define SUMS_MANAGER		"'$SUMS_MANAGER'"' >> $SCRIPT
echo '#define SUMLOG_BASEDIR		"'$SUMS_LOG_BASEDIR'"' >> $SCRIPT
echo '#define SUMS_TAPE_AVAILABLE       '\($SUMS_TAPE_AVAIL\)'' >> $SCRIPT
echo '#endif' >> $SCRIPT

cd include
ln -s ../$SCRIPT localization.h
cd ..


# make third-party links
echo "*** linking third-party libs and includes ***"
set JSOC_MACHINE = `build/jsoc_machine.csh`
ln -sfv $THIRD_PARTY_INCS lib_third_party/include
if (!(-d lib_third_party/lib)) mkdir lib_third_party/lib
ln -sfv $THIRD_PARTY_LIBS lib_third_party/lib/$JSOC_MACHINE
