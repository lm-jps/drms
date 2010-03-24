#!/bin/bash

if [ $# -eq 1 ]
then
    # Must always be a config file
    conf="$1"
else
    echo "ERROR: Usage: $0 <server configuration file>"
    exit 1
fi

. "$conf"

slonik <<_EOF_

cluster name = $CLUSTERNAME;

node 1 admin conninfo = 'dbname=$MASTERDBNAME host=$MASTERHOST port=$MASTERPORT user=$REPUSER';
node 2 admin conninfo = 'dbname=$SLAVEDBNAME host=$SLAVEHOST port=$SLAVEPORT user=$REPUSER';

init cluster ( id=1, comment = 'Server 1');

create set ( id=1, origin=1, comment = 'node1 REPLICATION SET' );

store node (id=2, comment = 'Server 2');

store path (server = 1, client = 2, conninfo='dbname=$MASTERDBNAME host=$MASTERHOST port=$MASTERPORT user=$REPUSER');
store path (server = 2, client = 1, conninfo='dbname=$SLAVEDBNAME host=$SLAVEHOST port=$SLAVEPORT user=$REPUSER');


store listen (origin=1, provider = 1, receiver =2);
store listen (origin=2, provider = 2, receiver =1);

_EOF_
