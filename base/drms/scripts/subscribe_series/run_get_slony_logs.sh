#!/bin/bash

if [ $# -ne 1 ] ; then
	echo "Usage: $0 <ssh-agent_rs file>"
	echo "       specify path/filename for the ssh-agent_rs file"
	exit
else
	rsFile="$1"
fi

exec < "$rsFile"
while read line; do
	set - $line
	if [ "$1" == "setenv" ]; then
		tag="$2"
		shift
		shift
		echo "export ${tag}=$*	"
		export ${tag}=$*
	fi
done

date

../get_slony_logs.pl jsoc mydb

