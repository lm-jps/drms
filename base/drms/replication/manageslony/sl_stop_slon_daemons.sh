#!/bin/bash

# The configuration file is the instantiation of repserver.template.cfg

error=""
specific="no"
pid=-1

if [ $# -eq 1 ]
then
    # Must always be a config file
    conf="$1"
elif [ $# -eq 2 ]
then
    conf="$1"
    # User can also specify either the master or the slave to start up
    specific="$2"
else
    error="ERROR: Usage: $0 <server configuration file> [ master | slave ]"
fi

if [ ! $error == "" ]
then
    echo $error
    exit
fi

. "$conf"

if [ $specific == "master" -o $specific == "no" ]
then
    pid=`cat $kMSMasterPIDFile`
    echo "Killing $pid"
    kill $pid
fi

if [ $specific == "slave" -o $specific == "no" ]
then
    pid=`cat $kMSSlavePIDFile`
    echo "Killing $pid"
    kill $pid
fi
