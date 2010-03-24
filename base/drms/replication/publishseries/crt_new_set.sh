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
# ----
cluster name = $CLUSTERNAME ;

# Admin conninfo's are used by the slonik program to connect
# to the node databases.  So these are the PQconnectdb arguments
# that connect from the administrators workstation (where
# slonik is executed).
# ----
node 1 admin conninfo = 'dbname=$MASTERDBNAME host=$MASTERHOST port=$MASTERPORT user=$REPUSER';
node 2 admin conninfo = 'dbname=$SLAVEDBNAME host=$SLAVEHOST port=$SLAVEPORT user=$REPUSER';

# ----
# Node 2 subscribes set 1
# ----

drop set (id = 2, origin=1);
drop set (id = 3, origin=1);
create set (id = 2, origin=1, comment='add extra tables');
set add table ( set id=2, origin=1, id=18, fully qualified name = 'su_rsb.gong_rdvfitsc_dp');
set add table ( set id=2, origin=1, id=19, fully qualified name = 'su_rsb.gong_rdvpspec_dp');
set add table ( set id=2, origin=1, id=20, fully qualified name = 'su_rsb.gong_rdvtrack_dp');
_EOF_
