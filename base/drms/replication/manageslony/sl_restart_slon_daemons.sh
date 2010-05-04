#!/bin/bash

if [[ $# -eq 1 ]]
then
	config_file=$1

        if [[ -f $config_file ]]
        then
                . $config_file
	else
		echo "ERROR: File $config_file does not exist, exiting"
		exit 1
	fi
else
	echo "ERROR: Usage: $0 <configuration file>"
        exit 1
fi

$kRepDir/manageslony/sl_stop_slon_daemons.sh $config_file
wait

$kRepDir/manageslony/sl_start_slon_daemons.sh $config_file
wait

echo "Slon Daemons Restarted"
