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


unzip -o ../MarkLogic-Connector-for-Hadoop-1.*.zip -d /tmp/mlconnector
unzip -o ../MarkXCC.Java-6.*.zip -d /tmp/xcc

for host in `cat $hostsfile`; do
	echo $host
	ssh -t $(whoami)@$host 'sudo mkdir /usr/lib/MarkLogic'
	
	scp /tmp/mlconnector/lib/marklogic-mapreduce-1.1.jar $(whoami)@$host:/tmp/
	ssh -t $(whoami)@$host 'sudo mv /tmp/marklogic-mapreduce-1.1.jar /usr/lib/MarkLogic/'
	
	scp /tmp/xcc/lib/marklogic-xcc-6.0.jar $(whoami)@$host:/tmp/
	ssh -t $(whoami)@$host 'sudo mv /tmp/marklogic-xcc-6.0.jar /usr/lib/MarkLogic/'
done 

rm -rf /tmp/mlconnector
rm -rf /tmp/xcc

sudo unzip -o ../marklogic-contentpump-1.*-bin.zip -d /usr/lib/
sudo cp mlcp /usr/bin/

