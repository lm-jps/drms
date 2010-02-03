#!/bin/bash

#--------------------------------------------------------------------
# Publish Series: 
# Syntax: ./publish_series.sh <config_file> <schema of series to publish> <table of series to publish>
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
logfile=$logdir/$publish_schema.$publish_table.publish_series.log
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
logwrite "Executing: [psql -t -h $master_host -p $master_port -U $master_user -c \"select * from pg_tables where tablename = '$publish_table' and schemaname = '$publish_schema'\" $master_dbname]"
check=`psql -t -h $master_host -p $master_port -U $master_user -c "select * from pg_tables where tablename = '$publish_table' and schemaname = '$publish_schema'" $master_dbname`
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
check=`psql -t -h $master_host -p $master_port -U $master_user -c "select * from _$slony_clustername.sl_table where tab_relname = '$publish_table' and tab_nspname = '$publish_schema'" $master_dbname`
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
check=`psql -h $master_host -p $master_port -t -U $master_user -c "select max(sub_set) from _$slony_clustername.sl_subscribe" $master_dbname`
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
#pg_dump -n $publish_schema --schema-only --exclude-table="$publish_schema.*" -f $temp_dir/createns_$publish_schema.$publish_table.sql $master_dbname
logwrite "Executing: [./createns ns=$publish_schema nsgroup=user dbusr=$master_user >> $temp_dir/createns_$publish_schema.$publish_table.sql]"
./createns ns=$publish_schema nsgroup=user dbusr=$master_user >> $temp_dir/createns_$publish_schema.$publish_table.sql

#--------------------------------------------------------------------
# Runs createtabstruct
#--------------------------------------------------------------------
#pg_dump --schema-only -t "$publish_schema.$publish_table" -f $temp_dir/createtabstruct_$publish_schema.$publish_table.sql $master_dbname
logwrite "Executing: [./createtabstructure in=$publish_schema.$publish_table out=$publish_schema.$publish_table archive=$archive retention=$retention tapegroup=$tapegroup owner=slony >> $temp_dir/createtabstruct_$publish_schema.$publish_table.sql]"
./createtabstructure in=$publish_schema.$publish_table out=$publish_schema.$publish_table archive=$archive retention=$retention tapegroup=$tapegroup owner=slony >> $temp_dir/createtabstruct_$publish_schema.$publish_table.sql

#--------------------------------------------------------------------
# Execute sql files on $slave_host and check for errors
#--------------------------------------------------------------------
logwrite "Executing [psql -h $slave_host -p $slave_port -U $slave_user -f $temp_dir/createns_$publish_schema.$publish_table.sql $slave_dbname > $temp_dir/createns.log 2>&1]"
psql -h $slave_host -p $slave_port -U $slave_user -f $temp_dir/createns_$publish_schema.$publish_table.sql $slave_dbname > $temp_dir/createns.log 2>&1

logwrite "Executing [psql -h $slave_host -p $slave_port -U $slave_user -f $temp_dir/createtabstruct_$publish_schema.$publish_table.sql $slave_dbname > $temp_dir/createtabstruct.log 2>&1]"
psql -h $slave_host -p $slave_port -U $slave_user -f $temp_dir/createtabstruct_$publish_schema.$publish_table.sql $slave_dbname > $temp_dir/createtabstruct.log 2>&1
check=`cat $temp_dir/createtabstruct.log | grep ERROR`
#cp $temp_dir/createtabstruct.log $temp_dir/createtabstruct.debug #remove
if [ -n "$check" ]
then
        echo "ERROR: There was an error creating the tables on the slave. ABORTING"
        logwrite "ERROR: There was an error creating the tables on the slave. ABORTING"
	logwrite "ERROR: [$check]" nl
	#cp $temp_dir/createtabstruct_$publish_schema.$publish_table.sql $temp_dir/debug.error #remove
	rm -f $temp_dir/createns.log $temp_dir/createtabstruct.log $temp_dir/createtabstruct_$publish_schema.$publish_table.sql $temp_dir/createns_$publish_schema.$publish_table.sql
        exit
else
        logwrite "Createtabstruct output application on the slave was successful, continuing..." nl
fi
unset check

rm -f $temp_dir/createns.log $temp_dir/createtabstruct.log $temp_dir/createtabstruct_$publish_schema.$publish_table.sql $temp_dir/createns_$publish_schema.$publish_table.sql

#--------------------------------------------------------------------
# Checking to see if the series to be published exists on the slave
#--------------------------------------------------------------------
logwrite "Checking to see if the series to be published exists on the slave" 
check=`psql -t -h $slave_host -p $slave_port -U $slave_user -c "select * from pg_tables where tablename = '$publish_table' and schemaname = '$publish_schema'" $slave_dbname`
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
subid=`psql -h $master_host -p $master_port -U $master_user -t -c "select max(tab_id) from _$slony_clustername.sl_table" $master_dbname`
subid=${subid//" "/""}
logwrite "Found latest subscribed table ID is $subid, adding one" 
subid=$(($subid + 1))
logwrite "Using $subid as the next subscription id" 

#--------------------------------------------------------------------
# Creates temporary script to create the new replication set
#--------------------------------------------------------------------
newpubfn=$temp_dir/publish.$publish_schema.$publish_table
tempnewpubfn=$temp_dir/publish.$publish_schema.$publish_table.temp
echo -n "#!" > $newpubfn
which bash >> $newpubfn
cat $slony_config_file >> $newpubfn
sed 's/<schema.table>/'$publish_schema.$publish_table'/g' subscribetemplate.sh >> $tempnewpubfn
sed 's/<id>/'$subid'/g' $tempnewpubfn >> $newpubfn
rm -f $tempnewpubfn
chmod 777 $newpubfn
$newpubfn > $temp_dir/subscribe.$publish_schema.$publish_table.log 2>&1
logwrite "Executing: [cat $temp_dir/subscribe.$publish_schema.$publish_table.log | grep ERROR]"
check=`cat $temp_dir/subscribe.$publish_schema.$publish_table.log | grep ERROR`
#cp $temp_dir/subscribe.$publish_schema.$publish_table.log $temp_dir/subscribe.$publish_schema.$publish_table.log.debug #remove
cp $newpubfn $newpubfn.debug  #remove
logwrite "Result: [$check]"
if [ -n "$check" ]
then
        echo "ERROR: Subscription of the new set was not successful, aborting"
        logwrite "ERROR: Subscription of the new set was not successful, aborting"
	logwrite "$check"
	rm -f $temp_dir/subscribe.$publish_schema.$publish_table.log
	rm -f $newpubfn
        exit
else
        logwrite "Subscription of the new set was successful, continuing..." nl 
fi
unset check

#rm -f $temp_dir/subscribe.$publish_schema.$publish_table.log
#rm -f $newpubfn

#--------------------------------------------------------------------
# Create temporary merge script
#--------------------------------------------------------------------
mergefile=$temp_dir/merge.$publish_schema.$publish_table.sh
echo > $mergefile
cat $slony_config_file >> $mergefile
cat merge.sh >> $mergefile
chmod 777 $mergefile

#--------------------------------------------------------------------
# Preforms the merge set over and over till it returns successful
#--------------------------------------------------------------------
sleeptimer=30
counter=0
success=false
#echo "counter is [$counter] and merge_timeout is [$merge_timeout]" #remove
while [ "$counter" -le "$merge_timeout" ]
do
	echo "Attempting merge..."
	$mergefile > $temp_dir/merge.$publish_schema.$publish_table.log 2>&1
	check=`cat $temp_dir/merge.$publish_schema.$publish_table.log | grep ERROR`

	if [ -n "$check" ]
	then
		echo "Tables not yet synced, sleeping for $sleeptimer seconds..."
	else
	        logwrite "Merge was successful!" nl
		echo "Publish was successful!"
		success=true
		break
	fi
	unset check
	rm -f $temp_dir/merge.$subscribe_schema.$subscribe_table.log

	#echo $counter
	counter=$(($counter + 1))
	sleep $sleeptimer
done

if [ "$success" == "false" ]
then
	echo "Merge was unsuccessful after $merge_timeout of attempts"
	logwrite "Merge was unsuccessful after $merge_timeout of attempts" nl
fi

logwrite "$0 finished"
rm -f $temp_dir/merge.$publish_schema.$publish_table.log
rm -f $mergefile
