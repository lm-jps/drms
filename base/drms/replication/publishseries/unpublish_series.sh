#!/bin/bash

#trap "echo 'caught a signal'; exit" HUP INT TERM

# Need some code to prevent more than one instance from being run.

#--------------------------------------------------------------------
# Syntax check
#--------------------------------------------------------------------
if [[ $# -eq 3 ]]
then
	config_file=$1
	schema=`echo $2 | tr '[A-Z]' '[a-z]'`
	table=`echo $3 | tr '[A-Z]' '[a-z]'`
	
	if [[ -f $config_file ]]
	then
		. $config_file
		echo "Starting $0"
		echo "Using $config_file"
	else
		echo "ERROR: File $config_file does not exist, exiting"
		exit
	fi
else
	echo "ERROR: Usage: $0 <configuration file> <schema> <table>"
	exit 1;
fi


#--------------------------------------------------------------------
# Find the table ID of the table to drop
#--------------------------------------------------------------------
check=`$kPsqlCmd -h $MASTERHOST -U $REPUSER -p $MASTERPORT -t -c "select tab_id from _$CLUSTERNAME.sl_table where tab_nspname='$schema' AND tab_relname='$table'" $MASTERDBNAME`
tableID=${check// /}
slavetableID=$tableID
echo "Table ID is [$tableID]"

# if nothing found error out
if [[ "$tableID" == "" ]]
then
	echo "Could not find the published schema.table specified"
	exit 1;
fi

#--------------------------------------------------------------------
# Run slon drop table
#--------------------------------------------------------------------
echo "Dropping table from the replication set: cluster=$CLUSTERNAME, masterdb=$MASTERDBNAME, masterhost=$MASTERHOST, masterport=$MASTERPORT, repuser=$REPUSER, slavedb=$SLAVEDBNAME, slavehost=$SLAVEHOST, slaveport=$SLAVEPORT"
slonik <<_EOF_
cluster name = $CLUSTERNAME ;
node 1 admin conninfo = 'dbname=$MASTERDBNAME host=$MASTERHOST port=$MASTERPORT user=$REPUSER';
node 2 admin conninfo = 'dbname=$SLAVEDBNAME host=$SLAVEHOST port=$SLAVEPORT user=$REPUSER';

SET DROP TABLE (ORIGIN = 1, ID = $tableID);
_EOF_

echo "slonik cmd complete"

check=`$kPsqlCmd -h $MASTERHOST -U $REPUSER -p $MASTERPORT -t -c "select tab_id from _$CLUSTERNAME.sl_table where tab_nspname='$schema' AND tab_relname='$table'" $MASTERDBNAME`
tableID=${check// /}

if [[ "$tableID" == "" ]]
then
        echo "Table was droped from replication"
else
        echo "ERROR: Table was not droped from replication"
        exit 1;
fi

#--------------------------------------------------------------------
# Call delete_series to delete the series from the schema
#--------------------------------------------------------------------
maxatt=120
curratt=1
while [ $curratt -le $maxatt ]; do
    echo 'running $kPsqlCmd -h $SLAVEHOST -U $REPUSER -p $SLAVEPORT -t -c "select count(*) from _$CLUSTERNAME.sl_table where tab_id = $slavetableID" $SLAVEDBNAME'
    ret=`$kPsqlCmd -h $SLAVEHOST -U $REPUSER -p $SLAVEPORT -t -c "select count(*) from _$CLUSTERNAME.sl_table where tab_id = $slavetableID" $SLAVEDBNAME`
    set - $ret
    slCheck=$1
    echo "tables with id $slavetableID: $slCheck; Current attempt: $curratt"

    if [ $slCheck -lt 1 ]; then
        echo "Executing [$kModDir/delete_series -k $schema.$table JSOC_DBHOST=$kModOnSlaveHost]"
        $kModDir/delete_series -k $schema.$table JSOC_DBHOST=$kModOnSlaveHost

        # Check status
        if [ $? -ne 0 ]
        then
            echo "Error deleting series $schema.$table"
            exit 1;
        else
            echo "Successfully deleted $schema.$table"
            break
        fi
    fi

    sleep 1
    curratt=$(($curratt + 1))
done

# Can't do the following because subscription_update might be
# modifying the .lst files. But this shouldn't matter since, 
# after the unpublish, there won't get any information in the
# slony logs germane this this series anyway.
# 
#--------------------------------------------------------------------
# Remove the schema.table from each of the .lst files in $tables_dir
#--------------------------------------------------------------------
#echo "Removing $schema.$table from the subscribers list files"
#for x in `ls -1 $tables_dir`
#do
#	if [[ "${x: -4}" == ".lst" ]]
#	then
#		echo "Removing $schema.$table from $tables_dir/$x"
#		mv -f $tables_dir/$x $tables_dir/$x.bak
#		cat $tables_dir/$x.bak | grep -v "$schema.$table" > $tables_dir/$x
#		rm -f $tables_dir/$x.bak
#		echo "done"
#		echo
#	else
#		echo "Skipping [$x]"
#		echo
#		continue
#	fi
#done

${kRepDir}/editlstfile.pl ${kServerLockDir}/subscribelock.txt  $tables_dir $schema $table
if [ $? -ne 0 ]; then
	echo
	echo "--|---------------------------------------------------------------------"
	echo "--| Error, editlstfile.pl failed. "
	echo "--| Note that removing the table from SLONY replication succeeded!"
	echo "--|---------------------------------------------------------------------"
	echo
	exit 1;
else
	echo "table removed from all .lst files"
fi

#--------------------------------------------------------------------
# Must delete the series entries from the *.lst files. If a 
# remote site wants to subscribe to a series that isn't in 
# its *.lst file, but the series does exist at the remote site
# then it is up to the remote site to do a proper unsubscribe
# from the series (which deletes the series too) before
# subscribing to the series again. A user can get in this state
# in a few different ways - the series could have been 
# unpublished, then re-published; the user could do an 
# unsubscribe, but not delete the series. At any rate, if a user
# is in this situation, s/he needs to delete the series
# before subscribing to it, other wise the series content will
# not be synchronized with the forthcoming logs.
#--------------------------------------------------------------------

echo "$0 Done"

echo "Unsubscribe Complete"

exit 0;
