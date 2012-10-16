#!/bin/sh

usage="Usage: remove-mlcp-connector.sh --hosts hostlistfile"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/

source $DIR/lib.sh
checkArgs "$1" "$2" "$usage"

#remove mlcp locally
sudo rm -rf /usr/lib/marklogic-contentpump-1.1

#remove connector and XCC
for host in `cat $hostsfile`; do
	echo $host
	ssh -t root@$host 'rm -rf /usr/lib/MarkLogic'
	ssh -t root@$host 'rm -rf /etc/profile.d/ml.sh'
done
