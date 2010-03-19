#!/bin/bash

. ../etc/slony.env

slonik <<_EOF_

cluster name = $CLUSTERNAME;

node 1 admin conninfo = 'dbname=$MASTERDBNAME host=$MASTERHOST port=$MASTERPORT user=$REPUSER';
node 2 admin conninfo = 'dbname=$SLAVEDBNAME host=$SLAVEHOST port=$SLAVEPORT user=$REPUSER';

merge set ( id = 1, add id = 2, origin = 1);

_EOF_
