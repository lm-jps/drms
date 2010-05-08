#!/bin/bash

slonik <<_EOF_

cluster name = $CLUSTERNAME;

node 1 admin conninfo = 'dbname=$MASTERDBNAME host=$MASTERHOST port=$MASTERPORT user=$REPUSER';
node 2 admin conninfo = 'dbname=$SLAVEDBNAME host=$SLAVEHOST port=$SLAVEPORT user=$REPUSER';

create set ( id=<repsetid>, origin=1, comment = 'NEW TEMPORARY REPLICATION SET' );

set add table ( set id=<repsetid>, origin=1, id=<id>, fully qualified name = '<schema.table>');

subscribe set ( id = <repsetid>, provider = 1, receiver = 2, forward = no);

_EOF_
