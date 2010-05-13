#!/bin/bash

# The configuration file is the instantiation of repserver.template.cfg
function CleanExit {
  exitstat="$1"

  rm -f "$kLocalWorkingDir/slon_daemon_lock.txt"
  
  if [ -e "$kLocalWorkingDir/slon_daemon_lock.txt" ] 
  then
      logwrite "ERROR: Unable to remove subscribe lock file $kLocalWorkingDir/slon_daemon_lock.txt"
  fi

  exit $exitstat
}

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
    exit 1
fi

. "$conf"

slondaemonlockpath="$kServerLockDir/slon_daemon_lock.txt"

attempts=20
counter=0
killslave=0
killmaster=0

while [[ 1 == 1 ]]
do
    if ( set -o noclobber; echo "$$" > "$slondaemonlockpath") 2> /dev/null;
    then
        trap 'rm -f "$slondaemonlockpath"; exit 1' INT TERM HUP

        if [ $specific == "master" -o $specific == "no" -a -e $kMSMasterPIDFile ]
        then
	    pid=`cat $kMSMasterPIDFile`
	    echo "Killing $pid"
	    kill $pid
            killmaster=1
        fi
        
        if [ $specific == "slave" -o $specific == "no" -a -e $kMSSlavePIDFile ]
        then
	    pid=`cat $kMSSlavePIDFile`
	    echo "Killing $pid"
	    kill $pid
            killslave=1
        fi

        # Just in case it takes a while for the PID files to disappear...
        pidatt=30
        pidcnt=0
        foundmaster=1
        foundslave=1

        while [[ 1 == 1 ]]
        do
            if [ $killmaster -eq 1 -a $foundmaster -eq 1 ]
            then
                echo "Waiting for master to die"
                if [ ! -e $kMSMasterPIDFile ]
                then
                    echo "Master is dead"
                    foundmaster=0
                fi
            elif  [ $killslave -eq 1 -a $foundslave -eq 1 ]
            then
                echo "Waiting for slave to die"
                if [ ! -e $kMSSlavePIDFile ]
                then
                    echo "Slave is dead"
                    foundslave=0
                fi
            else
                break
            fi

            if [[ $pidcnt -eq $pidatt ]]
            then
                echo "failed to stop at least one daemon properly"
                echo "exiting!"
                CleanExit 1
            fi

            sleep 1
            pidcnt=$(( $pidcnt + 1 ))
        done

        rm -f $slondaemonlockpath

        if [ -e "$slondaemonlockpath" ]
        then
            echo "ERROR: Unable to remove subscribe lock file $slondaemonlockpath"
            exit 1
        fi

        trap - INT TERM HUP
        break
    else
        echo "Could not acquire the subscription lock [$slondaemonlockpath]"
        echo "Sleeping for one second"
    fi

    if [[ $counter -eq $attempts ]]
    then
        echo "maximum number of attempts reached"
        echo "exiting!"
        CleanExit 1
    fi

    sleep 1
    counter=$(( $counter + 1 ))
done
