#!/bin/sh

# DO NOT USE!! publish.pl performs this task. It creates a NEW Slony replication set, adds a single table
# to this set, and then calls slonik subscribe set. When the slony server code was set-up, a single replication
# set (set ID == 1) was created. No tables were added to that set. 
#
# publish.pl creates a new, independent replication set, and adds a single table to this set. Asynchronously, 
# merge_rep_sets runs slonik merge set to move the tables from the new replication set to the original set
# created with installNetDRMS.py. merge set then deletes the new replication set (which is now empty).
#
# merge_rep_set is called from a cron task:
# # merge_rep_sets cron task for the script that merges any excess replication sets to the first
# * * * * * /home/jsoc/cvs/Development/JSOC/base/drms/replication/publishseries/merge_rep_sets /home/jsoc/cvs/Development/JSOC/proj/replication/etc/repserver.cfg >> /usr/local/pgsql/replication/live/log/cron.merge_rep_sets.log 2>&1

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
subscribe set ( id = 1, provider = 1, receiver = 2, forward = no);
_EOF_
