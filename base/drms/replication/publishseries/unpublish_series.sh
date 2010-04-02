#!/bin/bash
currDt=`date +"%m-%d-%Y %H:%M:%S"`

#--------------------------------------------------------------------
# Syntax check
#--------------------------------------------------------------------
if [[ $# -eq 3 ]]
then
	config_file=$1
	schema=$2
	table=$3
	
	if [[ -f $config_file ]]
	then
		. $config_file
		. toolbox
		echo "Starting $0"
		echo "Using $config_file"
	else
		echo "ERROR: File $config_file does not exist, exiting"
		exit
	fi
else
	echo "ERROR: Usage: $0 <configuration file> <schema> <table>"
	exit
fi

#--------------------------------------------------------------------
# Find the table ID of the table to drop
#--------------------------------------------------------------------
check=`$kPsqlCmd -h $MASTERHOST -U $REPUSER -p $MASTERPORT -t -c "select tab_id from _$CLUSTERNAME.sl_table where tab_nspname='$schema' AND tab_relname='$table'" $MASTERDBNAME`
tableID=${check// /}
echo "Table ID is [$tableID]"

# if nothing found error out
if [[ "$tableID" == "" ]]
then
	echo "Could not find the published schema.table specified"
	exit
fi

#--------------------------------------------------------------------
# Run slon drop table
#--------------------------------------------------------------------
echo "Dropping table from the replication set"
slonik <<_EOF_
cluster name = $CLUSTERNAME ;
node 1 admin conninfo = 'dbname=$MASTERDBNAME host=$MASTERHOST port=$MASTERPORT user=$REPUSER';
node 2 admin conninfo = 'dbname=$SLAVEDBNAME host=$SLAVEHOST port=$SLAVEPORT user=$REPUSER';

SET DROP TABLE (ORIGIN = 1, ID = $tableID);
_EOF_

check=`$kPsqlCmd -h $MASTERHOST -U $REPUSER -p $MASTERPORT -t -c "select tab_id from _$CLUSTERNAME.sl_table where tab_nspname='$schema' AND tab_relname='$table'" $MASTERDBNAME`
tableID=${check// /}

if [[ "$tableID" == "" ]]
then
        echo "Table was droped from replication"
else
        echo "ERROR: Table was not droped from replication"
        exit
fi
#--------------------------------------------------------------------
# Drop the table from the schema
#--------------------------------------------------------------------
echo -n "Dropping table $schema.$table on $SLAVEHOST.........."
check=`$kPsqlCmd -h $SLAVEHOST -U $REPUSER -p $SLAVEPORT -t -c "drop table $schema.$table cascade" $SLAVEDBNAME`
if [[ $? -eq "0" ]]
then
	echo "Success"
	echo "Drop result is [$check]"
else
	echo "Failed!"
	echo "Drop result is [$check]"
	exit
fi
unset check

#--------------------------------------------------------------------
# Delete the entries in the drms tables referencing the series thats been unpublished
#--------------------------------------------------------------------
# Delete from drms_keyword
echo -n "Delete from $schema.drms_keyword where seriesname='$schema.$table'.........."
check=`$kPsqlCmd -h $SLAVEHOST -U $REPUSER -p $SLAVEPORT -t -c "delete from $schema.drms_keyword where seriesname='$schema.$table'" $SLAVEDBNAME 2>&1`
if [[ $? -eq "0" ]]
then
	echo "Success"
	echo "Drop result is [$check]"
else
	echo "Failed!"
	echo "Drop result is [$check]"
	exit
fi
unset check

# Delete from drms_link
echo -n "Delete from $schema.drms_link where seriesname='$schema.table'.........."
check=`$kPsqlCmd -h $SLAVEHOST -U $REPUSER -p $SLAVEPORT -t -c "delete from $schema.drms_link where seriesname='$schema.$table'" $SLAVEDBNAME 2>&1`
if [[ $? -eq "0" ]]
then
	echo "Success"
	echo "Drop result is [$check]"
else
	echo "Failed!"
	echo "Drop result is [$check]"
	exit
fi
unset check

# Delete from drms_segment
echo -n "Delete from $schema.drms_segment where seriesname='$schema.table'.........."
check=`$kPsqlCmd -h $SLAVEHOST -U $REPUSER -p $SLAVEPORT -t -c "delete from $schema.drms_segment where seriesname='$schema.$table'" $SLAVEDBNAME 2>&1`
if [[ $? -eq "0" ]]
then
	echo "Success"
	echo "Drop result is [$check]"
else
	echo "Failed!"
	echo "Drop result is [$check]"
	exit
fi
unset check

echo "Finished dropping everything"
echo "$0 Done"

#--------------------------------------------------------------------
# Remove the schema.table from each of the .lst files in $tables_dir
#--------------------------------------------------------------------
echo "Removing $schema.$table from the subscribers list files"
for x in `ls -1 $tables_dir`
do
	if [[ "${x: -4}" == ".lst" ]]
	then
		echo "Removing $schema.$table from $tables_dir/$x"
		mv -f $tables_dir/$x $tables_dir/$x.bak
		cat $tables_dir/$x.bak | grep -v "$schema.$table" > $tables_dir/$x
		rm -f $tables_dir/$x.bak
		echo "done"
		echo
	else
		echo "Skipping [$x]"
		echo
		continue
	fi
done

echo "Done"

echo "Unsubscribe Complete"
