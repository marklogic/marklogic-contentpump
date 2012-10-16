#!/bin/sh

usage="Usage: install-mlcp-connector.sh --hosts hostlistfile"
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/../

source $DIR/utility/lib.sh
checkArgs "$1" "$2" "$usage"

unzip -o $DIR/MarkLogic-Connector-for-Hadoop-1.*.zip -d /tmp/mlconnector
unzip -o $DIR/MarkXCC.Java-6.*.zip -d /tmp/xcc

for host in `cat $hostsfile`; do
	echo $host
	ssh -t root@$host 'mkdir /usr/lib/MarkLogic'
	
	scp /tmp/mlconnector/lib/marklogic-mapreduce-1.2.jar root@$host:/usr/lib/MarkLogic/
	scp /tmp/mlconnector/lib/commons-modeler-2.0.1.jar root@$host:/usr/lib/MarkLogic/
	scp /tmp/xcc/lib/marklogic-xcc-6.1.jar root@$host:/usr/lib/MarkLogic/
	scp $DIR/utility/ml.sh root@$host:/etc/profile.d/
done 

rm -rf /tmp/mlconnector
rm -rf /tmp/xcc

unzip -o $DIR/marklogic-contentpump-1.*-bin.zip -d /usr/lib/

