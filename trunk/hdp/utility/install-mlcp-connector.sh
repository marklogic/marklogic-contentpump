#!/bin/sh

usage="Usage: install-mlcp-connector.sh --hosts hostlistfile"

# if no args specified, show usage
if [ $# -le 1 ]; then
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
    fi
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/../

unzip -o $DIR/MarkLogic-Connector-for-Hadoop-1.*.zip -d /tmp/mlconnector
unzip -o $DIR/MarkXCC.Java-6.*.zip -d /tmp/xcc

for host in `cat $hostsfile`; do
	echo $host
	ssh -t root@$host 'mkdir /usr/lib/MarkLogic'
	
	scp /tmp/mlconnector/lib/marklogic-mapreduce-1.1.jar root@$host:/tmp/
	ssh -t root@$host 'mv /tmp/marklogic-mapreduce-1.1.jar /usr/lib/MarkLogic/'
	scp /tmp/mlconnector/lib/commons-modeler-2.0.1.jar root@$host:/tmp/
	ssh -t root@$host 'mv /tmp/commons-modeler-2.0.1.jar /usr/lib/MarkLogic/'
	
	scp /tmp/xcc/lib/marklogic-xcc-6.0.jar root@$host:/tmp/
	ssh -t root@$host 'mv /tmp/marklogic-xcc-6.0.jar /usr/lib/MarkLogic/'
	
	ssh -t root@$host 'echo "export HADOOP_DIST=HDP" >>/etc/profile'
done 

rm -rf /tmp/mlconnector
rm -rf /tmp/xcc

unzip -o $DIR/marklogic-contentpump-1.*-bin.zip -d /usr/lib/

