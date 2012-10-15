#!/bin/sh

usage="Usage: remove-mlcp-connector.sh [--hosts hostlistfile]"

# if no args specified, show usage
if [ $# -le 1 ]; then
	echo "removing on localhost"
	sudo rm -rf /usr/lib/marklogic-contentpump-1.1 /usr/lib/MarkLogic
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
sudo rm -rf /usr/lib/marklogic-contentpump-1.1

#remove connector and XCC
for host in `cat $hostsfile`; do
	echo $host
	ssh -t root@$host 'rm -rf /usr/lib/MarkLogic'
done
