#!/bin/bash


sl_stop_slon_daemons.sh /b/devtest/arta/JSOC/proj/replication/etc/repserver.cfg
wait

sl_start_slon_daemons.sh /b/devtest/arta/JSOC/proj/replication/etc/repserver.cfg
wait

echo "Slon Daemons Restarted"
