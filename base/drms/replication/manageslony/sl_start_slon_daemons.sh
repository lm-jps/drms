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

# Obtain lock - need to do this to check for the existence of the PID files, which
# can be manipulated by other programs
slondaemonlockpath="$kServerLockDir/slon_daemon_lock.txt"

attempts=20
counter=0
startslave=0
startmaster=0

while [[ 1 == 1 ]]
do
    if ( set -o noclobber; echo "$$" > "$slondaemonlockpath") 2> /dev/null;
    then
        trap 'rm -f "$slondaemonlockpath"; exit 1' INT TERM HUP

        # Don't start the daemons if they are already running
        if [ ! -e $kMSMasterPIDFile ]
        then
            if [ $specific == "master" -o $specific == "no" ]
            then
                echo "Starting the master slon daemon"
                $kSlonCmd -p $kMSMasterPIDFile -s 60000 -t 300000 $CLUSTERNAME "dbname=$MASTERDBNAME port=$MASTERPORT host=$MASTERHOST user=$REPUSER"  > $kMSLogDir/slon.node1.log 2>&1 &
                startmaster=1
            fi
        fi

        if [ ! -e $kMSSlavePIDFile ]
        then
            if [ $specific == "slave" -o $specific == "no" ]
            then
                echo "Starting the slave slon daemon"
                $kSlonCmd -p $kMSSlavePIDFile -s 60000 -a $kPSLlogsSourceDir -t 300000 -x "$kMSOnSync" $CLUSTERNAME "dbname=$SLAVEDBNAME port=$SLAVEPORT host=$SLAVEHOST user=$REPUSER"  > $kMSLogDir/slon.node2.log 2>&1 &
                startslave=1
            fi
        fi

        # Just in case it takes a while for the PID files to appear...
        pidatt=30
        pidcnt=0
        foundmaster=0
        foundslave=0

        while [[ 1 == 1 ]]
        do
            if [ $startmaster -eq 1 -a $foundmaster -eq 0 ]
            then
                echo "Waiting for master to start"
                if [ -e $kMSMasterPIDFile ]
                then
                    echo "Master is running"
                    foundmaster=1
                fi
            elif  [ $startslave -eq 1 -a $foundslave -eq 0 ]
            then
                echo "Waiting for slave to start"
                if [ -e $kMSSlavePIDFile ]
                then
                    echo "Slave is running"
                    foundslave=1
                fi
            else
                break
            fi

            if [[ $pidcnt -eq $pidatt ]]
            then
                echo "failed to start at least one daemon properly"
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
