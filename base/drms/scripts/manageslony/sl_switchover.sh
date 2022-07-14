#!/bin/bash

# Load the env, may want to pass location via params later.
env_file="../etc/slony.env"
. $env_file

# Set the origin and dest based on current origin and dest.
if [[ "$SWITCHED" ]] && [[ "${SWITCHED}" == "1" ]]; then
    origin="2"
    dest="1"
else 
    origin="1"
    dest="2"
fi

cat <<EOF

Debug info: these are the slonik commands that will be run:

# ----
cluster name = $CLUSTERNAME ;

# Admin conninfo's are used by the slonik program to connect
# to the node databases.  So these are the PQconnectdb arguments
# that connect from the administrators workstation (where
# slonik is executed).
# ----
node 1 admin conninfo = 'dbname=$MASTERDBNAME host=$MASTERHOST port=$MASTERPORT user=$REPUSER ';
node 2 admin conninfo = 'dbname=$SLAVEDBNAME host=$SLAVEHOST port=$SLAVEPORT user=$REPUSER ';

lock set (id = 1, origin = $origin);
wait for event (origin = $origin, confirmed = $dest);
echo 'set locked';
move set (id = 1, old origin = $origin, new origin = $dest);
wait for event (origin = $origin, confirmed = $dest);
echo 'switchover complete';

End slonik.

EOF

slonik <<_EOF_
# ----
cluster name = $CLUSTERNAME ;

# Admin conninfo's are used by the slonik program to connect
# to the node databases.  So these are the PQconnectdb arguments
# that connect from the administrators workstation (where
# slonik is executed).
# ----
node 1 admin conninfo = 'dbname=$MASTERDBNAME host=$MASTERHOST port=$MASTERPORT user=$REPUSER ';
node 2 admin conninfo = 'dbname=$SLAVEDBNAME host=$SLAVEHOST port=$SLAVEPORT user=$REPUSER ';

lock set (id = 1, origin = $origin);
wait for event (origin = $origin, confirmed = $dest);
echo 'set locked';
move set (id = 1, old origin = $origin, new origin = $dest);
wait for event (origin = $origin, confirmed = $dest);
echo 'switchover complete';

_EOF_

if [[ -z "$SWITCHED" ]]; then
    echo "# SWITCHED stores whether or not the master and slave have been switched." >> $env_file
    echo "# 0 for false, 1 for true." >> $env_file
    echo "export SWITCHED=1" >> $env_file
elif [[ "${SWITCHED}" == "1" ]]; then
    switch="0"
    notswitch="1"
else 
    switch="1"
    notswitch="0"
fi

sed -i "s/SWITCHED=${notswitch}/SWITCHED=${switch}/g" $env_file
