#!/bin/bash

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
logfile=$REP_PS_LOGDIR/$publish_schema.$publish_table.publish_series.log
echo > $logfile
logwrite () {
        echo `date +"%m-%d-%Y %H:%M:%S - "` "$1" >> $logfile
        if [ ! $2 == "" ]
        then
                echo `date +"%m-%d-%Y %H:%M:%S - "` >> $logfile
        fi
}

logwrite "Starting $0" nl

#--------------------------------------------------------------------
# Checking to see if the series to be published exists on the master
#--------------------------------------------------------------------
logwrite "Checking to see if the series to be published exists on the master" 
logwrite "Executing: [psql -t -h $MASTERHOST -p $MASTERPORT -U $REPUSER -c \"select * from pg_tables where tablename = '$publish_table' and schemaname = '$publish_schema'\" $MASTERDBNAME]"
check=`psql -t -h $MASTERHOST -p $MASTERPORT -U $REPUSER -c "select * from pg_tables where tablename = '$publish_table' and schemaname = '$publish_schema'" $MASTERDBNAME`
logwrite "Result: [$check]"
if [ -n "$check" ]
then
	logwrite "The series $publish_schema.$publish_table exists, continuing..." nl
else
	logwrite "The series $publish_schema.$publish_table does not appear to exist. ABORTING!" nl
	echo "The series $publish_schema.$publish_table does not appear to exist. ABORTING!"
	exit
fi
unset check

#--------------------------------------------------------------------
# Checking to see if the series to be published has already been published
#--------------------------------------------------------------------
logwrite "Checking to see if the series to be published has already been published" 
check=`psql -t -h $MASTERHOST -p $MASTERPORT -U $REPUSER -c "select * from _$CLUSTERNAME.sl_table where tab_relname = '$publish_table' and tab_nspname = '$publish_schema'" $MASTERDBNAME`
if [ -n "$check" ]
then
	echo "The series $publish_schema.$publish_table has already been published. ABORTING!"
	exit
else
	logwrite "The series $publish_schema.$publish_table has not yet been published, continuing..." nl
fi
unset check

#--------------------------------------------------------------------
# Checking to see if there is a second replication set already
#--------------------------------------------------------------------
logwrite "Checking to see if there is a second replication set already" 
check=`psql -h $MASTERHOST -p $MASTERPORT -t -U $REPUSER -c "select max(sub_set) from _$CLUSTERNAME.sl_subscribe" $MASTERDBNAME`
check=${check// /}

if [ $check -eq "1" ]
then
	logwrite "Found only one replication set, continuing..." nl 
else
	logwrite "Another replication set was found. ABORTING!"
	echo "Another replication set was found. ABORTING!"
	exit
fi
unset check

echo "All initial checks were successful. Continuing..."

#--------------------------------------------------------------------
# Runs createns
#--------------------------------------------------------------------
#pg_dump -n $publish_schema --schema-only --exclude-table="$publish_schema.*" -f $REP_PS_TMPDIR/createns_$publish_schema.$publish_table.sql $MASTERDBNAME
logwrite "Executing: [./createns ns=$publish_schema nsgroup=user dbusr=$REPUSER >> $REP_PS_TMPDIR/createns_$publish_schema.$publish_table.sql]"
./createns ns=$publish_schema nsgroup=user dbusr=$REPUSER >> $REP_PS_TMPDIR/createns_$publish_schema.$publish_table.sql

#--------------------------------------------------------------------
# Runs createtabstruct
#--------------------------------------------------------------------
#pg_dump --schema-only -t "$publish_schema.$publish_table" -f $REP_PS_TMPDIR/createtabstruct_$publish_schema.$publish_table.sql $MASTERDBNAME
logwrite "Executing: [./createtabstructure in=$publish_schema.$publish_table out=$publish_schema.$publish_table owner=slony >> $REP_PS_TMPDIR/createtabstruct_$publish_schema.$publish_table.sql]"
./createtabstructure in=$publish_schema.$publish_table out=$publish_schema.$publish_table owner=slony >> $REP_PS_TMPDIR/createtabstruct_$publish_schema.$publish_table.sql

#--------------------------------------------------------------------
# Execute sql files on $SLAVEHOST and check for errors
#--------------------------------------------------------------------
logwrite "Executing [psql -h $SLAVEHOST -p $SLAVEPORT -U $REPUSER -f $REP_PS_TMPDIR/createns_$publish_schema.$publish_table.sql $SLAVEDBNAME > $REP_PS_TMPDIR/createns.log 2>&1]"
psql -h $SLAVEHOST -p $SLAVEPORT -U $REPUSER -f $REP_PS_TMPDIR/createns_$publish_schema.$publish_table.sql $SLAVEDBNAME > $REP_PS_TMPDIR/createns.log 2>&1

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
	rm -f $REP_PS_TMPDIR/createns.log $REP_PS_TMPDIR/createtabstruct.log $REP_PS_TMPDIR/createtabstruct_$publish_schema.$publish_table.sql $REP_PS_TMPDIR/createns_$publish_schema.$publish_table.sql
        exit
else
        logwrite "Createtabstruct output application on the slave was successful, continuing..." nl
fi
unset check

rm -f $REP_PS_TMPDIR/createns.log $REP_PS_TMPDIR/createtabstruct.log $REP_PS_TMPDIR/createtabstruct_$publish_schema.$publish_table.sql $REP_PS_TMPDIR/createns_$publish_schema.$publish_table.sql

#--------------------------------------------------------------------
# Checking to see if the series to be published exists on the slave
#--------------------------------------------------------------------
logwrite "Checking to see if the series to be published exists on the slave" 
check=`psql -t -h $SLAVEHOST -p $SLAVEPORT -U $REPUSER -c "select * from pg_tables where tablename = '$publish_table' and schemaname = '$publish_schema'" $SLAVEDBNAME`
if [ -n "$check" ]
then
	logwrite "The series $publish_schema.$publish_table exists on the slave, continuing..." nl
else
	logwrite "The series $publish_schema.$publish_table does not appear to exist on the slave. ABORTING!"nl
	echo "The series $publish_schema.$publish_table does not appear to exist on the slave. ABORTING!"
	exit
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
# Creates temporary script to create the new replication set
#--------------------------------------------------------------------
newpubfn=$REP_PS_TMPDIR/publish.$publish_schema.$publish_table
tempnewpubfn=$REP_PS_TMPDIR/publish.$publish_schema.$publish_table.temp
echo -n "#!" > $newpubfn
which bash >> $newpubfn
# The next cmd used to be
# cat $slony_config_file >> $newpubfn
cat $config_file >> $newpubfn
sed 's/<schema.table>/'$publish_schema.$publish_table'/g' subscribetemplate.sh >> $tempnewpubfn
sed 's/<id>/'$subid'/g' $tempnewpubfn >> $newpubfn
rm -f $tempnewpubfn
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
        exit
else
        logwrite "Subscription of the new set was successful, continuing..." nl 
fi
unset check

#rm -f $REP_PS_TMPDIR/subscribe.$publish_schema.$publish_table.log
#rm -f $newpubfn

#--------------------------------------------------------------------
# Create temporary merge script
#--------------------------------------------------------------------
mergefile=$REP_PS_TMPDIR/merge.$publish_schema.$publish_table.sh
echo > $mergefile
# The next cmd used to be
# cat $slony_config_file >> $mergefile
cat $config_file >> $mergefile

cat merge.sh >> $mergefile
chmod 777 $mergefile

#--------------------------------------------------------------------
# Checks the number of lag events, if zero, try to merge.
#--------------------------------------------------------------------
echo "Waiting until slony is synced to preform the merge. This may take a while."
sleeptimer=1
counter=0
#echo "counter is [$counter] and REP_PS_MERGETO is [$REP_PS_MERGETO]" #remove
while [[ "$counter" -le "$REP_PS_MERGETO" ]]
do
        # checking the number of lag events
        check=`psql -h $MASTERHOST -p $MASTERPORT -t -U $REPUSER -c "select st_lag_num_events from _jsoc.sl_status" $MASTERDBNAME`
        check=${check// /}
        logwrite "lag events is [$check]"
        if [ "$check" == "0" ]
        then
                echo "Lag events is zero, attempting merge..."
                logwrite "Lag events is zero, attempting merge..."

                $mergefile > $REP_PS_TMPDIR/merge.$publish_schema.$publish_table.log 2>&1
                check2=`cat $REP_PS_TMPDIR/merge.$publish_schema.$publish_table.log | grep ERROR`
		logwrite "check after merge attempt is [$check2]"

                if [ -n "$check2" ]
                then
                        echo "Merge failed, if lag events is still 0, trying again."
                else
                        logwrite "Merge was successful!"
                        echo "Publish was successful!"
                        break
                fi
                unset check2
                rm -f $REP_PS_TMPDIR/merge.$subscribe_schema.$subscribe_table.log

                #echo $counter
                counter=$(($counter + 1))
        fi
        sleep $sleeptimer
done

rm -f $REP_PS_TMPDIR/merge.$publish_schema.$publish_table.log
rm -f $mergefile

logwrite "$0 finished"
