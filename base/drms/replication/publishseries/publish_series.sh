#!/bin/bash


function SafeExit {
    exitstat="$1"

    rm -f "$publockpath"
    trap - INT TERM HUP
    
    if [ -e "$publockpath" ] 
    then
        logwrite "ERROR: Unable to remove publication lock file ${publockpath}."
    fi

    exit $exitstat
}


function restartslons {
#--------------------------------------------------------------------
# Stop slon daemons - sometimes slony gets blocked (it can't process
# lag events for some reason - like unfixable error is occurring).
# Stopping and restarting appears to fix a race condition that leads
# to problems like this. When the slon daemons start up, they
# work around the race condition.                
#--------------------------------------------------------------------
    if [ -f $kMSMasterPIDFile -o -f $kMSSlavePIDFile ]
    then
        # slon daemons are running
        echo "stopping daemons"
        "$kRepDir/manageslony/sl_stop_slon_daemons.sh" "$config_file"
        wait
        sleep 5

        if [ -f $kMSMasterPIDFile -o -f $kMSSlavePIDFile ]
        then
            echo "slon daemons not successfully stopped"
            exit 1
        fi
    fi

    #--------------------------------------------------------------------
    # Start up slony daemons.
    #--------------------------------------------------------------------
    echo "starting slon daemons"
    "$kRepDir/manageslony/sl_start_slon_daemons.sh" "$config_file"
    wait
    sleep 5

    if [ ! -f $kMSMasterPIDFile -o ! -f $kMSSlavePIDFile ]
    then
        echo "slon daemons not successfully started"
        exit 1
    fi
}


#--------------------------------------------------------------------
# Publish Series: 
# Syntax: ./publish_series.sh <config_file> <schema of series to publish> <table of series to publish>
# <config_file> must be repserver.cfg
#--------------------------------------------------------------------
if [ $# -ne 3 ]
then
	echo "ERROR: Usage: $0 <config_file> <schema of series to publish> <table of series to publish>"
	exit
else
	config_file=$1
	publish_schema=$2
	publish_table=$3
fi

. $config_file

#--------------------------------------------------------------------
# Setting up the log
#--------------------------------------------------------------------
logfile=$REP_PS_LOGDIR/$publish_schema.$publish_table.publish_series.$$.log
echo > $logfile
logwrite () {
        echo `date +"%m-%d-%Y %H:%M:%S - "` "$1" >> $logfile
        if [ ! $2 == "" ]
        then
                echo `date +"%m-%d-%Y %H:%M:%S - "` >> $logfile
        fi
}

function logecho {
  if [ ! -z "$logfile" ]
  then
    echo `date +"%m-%d-%Y %H:%M:%S - "` "$1" | tee -a $logfile;
  fi
}

logwrite "Starting $0" nl

#--------------------------------------------------------------------
# Lock this instance - only one instance allowed running at a time
#--------------------------------------------------------------------
publockpath="$kServerLockDir/$kPubLockFile"

while [[ 1 == 1 ]]; do
    if ( set -o noclobber; echo "$$" > "$publockpath") 2> /dev/null;
    then
        trap 'rm -f "$publockpath"; exit 1' INT TERM HUP  
        break
    else
        logecho "Could not acquire the publication lock (another $0 instance is running)"
        logecho "Sleeping for 1 minute; enter ctrl-c to give up"
    fi

    sleep 60
done

# Must have publication lock to continue

#--------------------------------------------------------------------
# Checking to see if the series to be published exists on the master
#--------------------------------------------------------------------
logwrite "Checking to see if the series to be published exists on the master" 
logwrite "Executing: [psql -t -h $MASTERHOST -p $MASTERPORT -U $REPUSER -c \"select * from pg_tables where tablename ilike '$publish_table' and schemaname ilike '$publish_schema'\" $MASTERDBNAME]"
check=`psql -t -h $MASTERHOST -p $MASTERPORT -U $REPUSER -c "select * from pg_tables where tablename ilike '$publish_table' and schemaname ilike '$publish_schema'" $MASTERDBNAME`
logwrite "Result: [$check]"
if [ -n "$check" ]
then
	logwrite "The series $publish_schema.$publish_table exists, continuing..." nl
else
	logwrite "The series $publish_schema.$publish_table does not appear to exist. ABORTING!" nl
	echo "The series $publish_schema.$publish_table does not appear to exist. ABORTING!"
	SafeExit 0
fi
unset check

#--------------------------------------------------------------------
# Checking to see if the series to be published has already been published
#--------------------------------------------------------------------
logwrite "Checking to see if the series to be published has already been published" 
check=`psql -t -h $MASTERHOST -p $MASTERPORT -U $REPUSER -c "select * from _$CLUSTERNAME.sl_table where tab_relname ilike '$publish_table' and tab_nspname ilike '$publish_schema'" $MASTERDBNAME`
if [ -n "$check" ]
then
	echo "The series $publish_schema.$publish_table has already been published. ABORTING!"
	SafeExit 0
else
	logwrite "The series $publish_schema.$publish_table has not yet been published, continuing..." nl
fi
unset check

##--------------------------------------------------------------------
## Checking to see if there is a second replication set already
##--------------------------------------------------------------------
#logwrite "Checking to see if there is a second replication set already" 
#check=`psql -h $MASTERHOST -p $MASTERPORT -t -U $REPUSER -c "select max(sub_set) from _$CLUSTERNAME.sl_subscribe" $MASTERDBNAME`
#check=${check// /}
#
#if [ $check -eq "1" ]
#then
#	logwrite "Found only one replication set, continuing..." nl 
#else
#	logwrite "Another replication set was found. ABORTING!"
#	echo "Another replication set was found. ABORTING!"
#	exit
#fi
#unset check

echo "All initial checks were successful. Continuing..."

#--------------------------------------------------------------------
# Runs createns
#--------------------------------------------------------------------
#pg_dump -n $publish_schema --schema-only --exclude-table="$publish_schema.*" -f $REP_PS_TMPDIR/createns_$publish_schema.$publish_table.sql $MASTERDBNAME
nsCheckSQL="select count(*) from pg_namespace where nspname = '$publish_schema'"
ret=`echo "$nsCheckSQL" | psql -t -h $SLAVEHOST -p $SLAVEPORT -U $REPUSER $SLAVEDBNAME`
set - $ret
nsCheck=$1

if [ $nsCheck -lt 1 ]; then
	logwrite "Executing: [./createns ns=$publish_schema nsgroup=user  dbusr=$REPUSER >> $REP_PS_TMPDIR/createns_$publish_schema.$publish_table.sql]"

	$kModDir/createns ns=$publish_schema nsgroup=user dbusr=$REPUSER > $REP_PS_TMPDIR/createns_$publish_schema.$publish_table.sql
else
	echo "namespace already exists, skipping createns.."
fi

#--------------------------------------------------------------------
# Runs createtabstruct
#--------------------------------------------------------------------
#pg_dump --schema-only -t "$publish_schema.$publish_table" -f $REP_PS_TMPDIR/createtabstruct_$publish_schema.$publish_table.sql $MASTERDBNAME

tabCheckSQL="select count(*) from pg_tables where schemaname = '$publish_schema' and tablename = '$publish_table'"
ret=`echo "$tabCheckSQL" | psql -t -h $SLAVEHOST -p $SLAVEPORT -U $REPUSER $SLAVEDBNAME`
set - $ret
tabCheck=$1

if [ $tabCheck -lt 1 ]; then
	logwrite "Executing: [./createtabstructure in=$publish_schema.$publish_table  out=$publish_schema.$publish_table owner=slony >> $REP_PS_TMPDIR/createtabstruct_$publish_schema.$publish_table.sql]"

	$kModDir/createtabstructure in=$publish_schema.$publish_table out=$publish_schema.$publish_table owner=slony > $REP_PS_TMPDIR/createtabstruct_$publish_schema.$publish_table.sql
else
	echo "table [${publish_schema}.${publish_table}] already exists, aborting..."
	SafeExit 1;
fi

#--------------------------------------------------------------------
# Execute sql files on $SLAVEHOST and check for errors
#--------------------------------------------------------------------
if [ $nsCheck -lt 1 ]; then
	logwrite "Executing [psql -h $SLAVEHOST -p $SLAVEPORT -U $REPUSER -f $REP_PS_TMPDIR/createns_$publish_schema.$publish_table.sql $SLAVEDBNAME > $REP_PS_TMPDIR/createns.log 2>&1]"
	psql -h $SLAVEHOST -p $SLAVEPORT -U $REPUSER -f $REP_PS_TMPDIR/createns_$publish_schema.$publish_table.sql $SLAVEDBNAME > $REP_PS_TMPDIR/createns.log 2>&1
else 
	echo "skipping execution of psql for createns"
fi

logwrite "Executing [psql -h $SLAVEHOST -p $SLAVEPORT -U $REPUSER -f $REP_PS_TMPDIR/createtabstruct_$publish_schema.$publish_table.sql $SLAVEDBNAME > $REP_PS_TMPDIR/createtabstruct.log 2>&1]"
psql -h $SLAVEHOST -p $SLAVEPORT -U $REPUSER -f $REP_PS_TMPDIR/createtabstruct_$publish_schema.$publish_table.sql $SLAVEDBNAME > $REP_PS_TMPDIR/createtabstruct.log 2>&1
check=`cat $REP_PS_TMPDIR/createtabstruct.log | grep ERROR`
#cp $REP_PS_TMPDIR/createtabstruct.log $REP_PS_TMPDIR/createtabstruct.debug #remove
if [ -n "$check" ]
then
        echo "ERROR: There was an error creating the tables on the slave. ABORTING"
        logwrite "ERROR: There was an error creating the tables on the slave. ABORTING"
	logwrite "ERROR: [$check]" nl
	#cp $REP_PS_TMPDIR/createtabstruct_$publish_schema.$publish_table.sql $REP_PS_TMPDIR/debug.error #remove
	# rm -f $REP_PS_TMPDIR/createns.log $REP_PS_TMPDIR/createtabstruct.log $REP_PS_TMPDIR/createtabstruct_$publish_schema.$publish_table.sql $REP_PS_TMPDIR/createns_$publish_schema.$publish_table.sql
        SafeExit 1
else
        logwrite "Createtabstruct output application on the slave was successful, continuing..." nl
fi
unset check

rm -f $REP_PS_TMPDIR/createns.log $REP_PS_TMPDIR/createtabstruct.log $REP_PS_TMPDIR/createtabstruct_$publish_schema.$publish_table.sql $REP_PS_TMPDIR/createns_$publish_schema.$publish_table.sql

#--------------------------------------------------------------------
# Checking to see if the series to be published exists on the slave
#--------------------------------------------------------------------
logwrite "Checking to see if the series to be published exists on the slave" 
check=`psql -t -h $SLAVEHOST -p $SLAVEPORT -U $REPUSER -c "select * from pg_tables where tablename ilike '$publish_table' and schemaname ilike '$publish_schema'" $SLAVEDBNAME`
if [ -n "$check" ]
then
	logwrite "The series $publish_schema.$publish_table exists on the slave, continuing..." nl
else
	logwrite "The series $publish_schema.$publish_table does not appear to exist on the slave. ABORTING!"nl
	echo "The series $publish_schema.$publish_table does not appear to exist on the slave. ABORTING!"
	SafeExit 1
fi
unset check
#exit #remove

#--------------------------------------------------------------------
# Finds the next subscription ID needed
#--------------------------------------------------------------------
subid=`psql -h $MASTERHOST -p $MASTERPORT -U $REPUSER -t -c "select max(tab_id) from _$CLUSTERNAME.sl_table" $MASTERDBNAME`
subid=${subid//" "/""}
logwrite "Found latest subscribed table ID is $subid, adding one" 
subid=$(($subid + 1))
logwrite "Using $subid as the next subscription id" 

#--------------------------------------------------------------------
# Finds the next replication set ID needed
#--------------------------------------------------------------------
repid=`psql -h $MASTERHOST -p $MASTERPORT -U $REPUSER -t -c "select max(set_id) from _$CLUSTERNAME.sl_set" $MASTERDBNAME`
repid=${repid//" "/""}
logwrite "Found latest subscribed replication set ID is $repid, adding one" 
repid=$(($repid + 1))
logwrite "Using $repid as the next replication set id" 

#--------------------------------------------------------------------
# Creates temporary script to create the new replication set
#--------------------------------------------------------------------
newpubfn=$REP_PS_TMPDIR/publish.$publish_schema.$publish_table
tempnewpubfn=$REP_PS_TMPDIR/publish.$publish_schema.$publish_table.temp

# create lower case versions of the schema and table strings
publish_schema_lower=`echo $publish_schema | tr '[A-Z]' '[a-z]'`
publish_table_lower=`echo $publish_table | tr '[A-Z]' '[a-z]'`

echo -n "#!" > $newpubfn
which bash >> $newpubfn
# The next cmd used to be
# cat $slony_config_file >> $newpubfn
cat $config_file >> $newpubfn
sed 's/<schema.table>/'$publish_schema_lower.$publish_table_lower'/g' $kRepDir/publishseries/subscribetemplate.sh >> $tempnewpubfn
sed 's/<repsetid>/'$repid'/g' $tempnewpubfn >> $tempnewpubfn.2
sed 's/<id>/'$subid'/g' $tempnewpubfn.2 >> $newpubfn
rm -f $tempnewpubfn $tempnewpubfn.2 
chmod 777 $newpubfn
logwrite "Executing [$newpubfn > $REP_PS_TMPDIR/subscribe.$publish_schema.$publish_table.log 2>&1]"
$newpubfn > $REP_PS_TMPDIR/subscribe.$publish_schema.$publish_table.log 2>&1
logwrite "Executing: [cat $REP_PS_TMPDIR/subscribe.$publish_schema.$publish_table.log | grep ERROR]"
check=`cat $REP_PS_TMPDIR/subscribe.$publish_schema.$publish_table.log | grep ERROR`
#cp $REP_PS_TMPDIR/subscribe.$publish_schema.$publish_table.log $REP_PS_TMPDIR/subscribe.$publish_schema.$publish_table.log.debug #remove
cp $newpubfn $newpubfn.debug  #remove
logwrite "Result: [$check]"
if [ -n "$check" ]
then
        echo "ERROR: Subscription of the new set was not successful, aborting"
        logwrite "ERROR: Subscription of the new set was not successful, aborting"
	logwrite "$check"
	rm -f $REP_PS_TMPDIR/subscribe.$publish_schema.$publish_table.log
	rm -f $newpubfn
        SafeExit 1
else
        logwrite "Subscription of the new set was successful, continuing..." nl 
fi
unset check

#--------------------------------------------------------------------
# Check for propagation of table to temp replication set
#--------------------------------------------------------------------

tabchk=0
echo "Checking for propagation of published table to slave temporary replication set $repid"
while [ $tabchk -lt 1 ]
do 
    sqlret=`psql -h $SLAVEHOST -p $SLAVEPORT -U $REPUSER -t -c "select count(*) from _$CLUSTERNAME.sl_table where tab_id = $subid" $SLAVEDBNAME`
    set - $sqlret
    tabchk=$1

    echo -n "."
    sleep 1
done


# Must release publication lock (otherwise we'd never be able to run publish_series.sh again)
rm -f "$publockpath"
trap - INT TERM HUP

echo
echo "$0 finished"

logwrite "$0 finished"
