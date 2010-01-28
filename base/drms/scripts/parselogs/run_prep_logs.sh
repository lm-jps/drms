#!/bin/bash


for I in `ls /usr/local/pgsql/slon_logs/*.sql`
do
	fn=`basename $I`
	echo "[$fn]"
	./prep_slon_logs $I ../etc/proc_logs.cfg ../log/proc_logs.${fn}.cfg
done

