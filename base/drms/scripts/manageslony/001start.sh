#!/bin/bash

. ../etc/slony.env


slonik <<_EOF_

cluster name = $CLUSTERNAME;

node 1 admin conninfo = 'dbname=$MASTERDBNAME host=$MASTERHOST port=$MASTERPORT user=$REPUSER';
node 2 admin conninfo = 'dbname=$SLAVEDBNAME host=$SLAVEHOST port=$SLAVEPORT user=$REPUSER';

init cluster ( id=1, comment = 'Server 1');

create set ( id=1, origin=1, comment = 'node1 REPLICATION SET' );

set add table ( set id=1, origin=1, id=1, fully qualified name = 'su_arta.lev0');
set add table ( set id=1, origin=1, id=2, fully qualified name = 'su_arta.vw_v_lev18');
set add table ( set id=1, origin=1, id=3, fully qualified name = 'su_arta.fd_v_lev18');
set add table ( set id=1, origin=1, id=4, fully qualified name = 'su_arta.fd_m_lev18');
set add table ( set id=1, origin=1, id=5, fully qualified name = 'su_arta.fd_m_96m_lev18');
set add table ( set id=1, origin=1, id=6, fully qualified name = 'mdi.fd_m_96m_lev18');
set add table ( set id=1, origin=1, id=7, fully qualified name = 'su_phil.vwvlev18_test');
set add table ( set id=1, origin=1, id=8, fully qualified name = 'su_phil.fdmtest');
set add table ( set id=1, origin=1, id=9, fully qualified name = 'sha.sngb_rot30n_96x20');
set add table ( set id=1, origin=1, id=10, fully qualified name = 'su_rsb.noaa_activeregions');
set add table ( set id=1, origin=1, id=11, fully qualified name = 'su_rsb.gong_rdvfitsc_dp');
set add table ( set id=1, origin=1, id=12, fully qualified name = 'su_rsb.gong_rdvpspec_dp');
set add table ( set id=1, origin=1, id=13, fully qualified name = 'lm_jps.lev1_test4k10s');
set add table ( set id=1, origin=1, id=14, fully qualified name = 'su_rsb.gong_rdvtrack_dp');

#set add sequence ( set id=1, origin=1, id=1, fully qualified name = 'su_arta.fd_m_96m_lev18_seq');
#set add sequence ( set id=1, origin=1, id=2, fully qualified name = 'su_arta.fd_m_lev18_seq');
#set add sequence ( set id=1, origin=1, id=3, fully qualified name = 'su_arta.fd_v_lev18_seq');
#set add sequence ( set id=1, origin=1, id=4, fully qualified name = 'su_arta.lev0_seq');
#set add sequence ( set id=1, origin=1, id=5, fully qualified name = 'su_arta.vw_v_lev18_seq');
#set add sequence ( set id=1, origin=1, id=6, fully qualified name = 'mdi.fd_m_96m_lev18_seq');
#set add sequence ( set id=1, origin=1, id=7, fully qualified name = 'su_phil.vwvlev18_test_seq');
#set add sequence ( set id=1, origin=1, id=8, fully qualified name = 'su_phil.fdmtest_seq');
#set add sequence ( set id=1, origin=1, id=9, fully qualified name = 'sha.sngb_rot30n_96x20_seq');
#set add sequence ( set id=1, origin=1, id=10, fully qualified name = 'su_rsb.noaa_activeregions_seq');
#set add sequence ( set id=1, origin=1, id=10, fully qualified name = 'su_rsb.gong_rdvfitsc_dp_seq');


store node (id=2, comment = 'Server 2');

store path (server = 1, client = 2, conninfo='dbname=$MASTERDBNAME host=$MASTERHOST port=$MASTERPORT user=$REPUSER');
store path (server = 2, client = 1, conninfo='dbname=$SLAVEDBNAME host=$SLAVEHOST port=$SLAVEPORT user=$REPUSER');


store listen (origin=1, provider = 1, receiver =2);
store listen (origin=2, provider = 2, receiver =1);

_EOF_
