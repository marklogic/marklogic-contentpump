#!/bin/sh

usage="Usage: remove-mlcp-connector.sh [--hosts hostlistfile]"

# if no args specified, show usage
if [ $# -le 1 ]; then
	echo "removing on localhost"
	rm -rf /usr/lib/marklogic-contentpump-1.1
	rm -f /usr/bin/mlcp
	rm -rf /usr/lib/MarkLogic
	echo $usage
	exit 1
fi

if [ $# -gt 1 ]
then
    if [ "--hosts" = "$1" ]
    then
        shift
        hostsfile=$1
    else
    	echo $usage
    	exit 1
    fi
fi

#remove mlcp locally
rm -rf /usr/lib/marklogic-contentpump-1.1
rm -f /usr/bin/mlcp

for host in `cat $hostsfile`; do
	echo $host
	ssh -t $(whoami)@$host 'sudo rm -rf /usr/lib/MarkLogic'
done
