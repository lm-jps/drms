#!/bin/bash

. ../etc/slony.env

echo "Starting the slon daemons"

slon -p ../log/slon.node1.pidfile -t 300000 $CLUSTERNAME "dbname=$MASTERDBNAME port=$MASTERPORT host=$MASTERHOST user=$REPUSER"  > ../log/slon.node1.log 2>&1 &

slon -p ../log/slon.node2.pidfile -a /usr/local/pgsql/slon_logs -t 300000 $CLUSTERNAME "dbname=$SLAVEDBNAME port=$SLAVEPORT host=$SLAVEHOST user=$REPUSER"  > ../log/slon.node2.log 2>&1 &

