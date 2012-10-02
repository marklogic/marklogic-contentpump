#!/bin/sh

usage="Usage: update-hadoop-classpath.sh --hosts hostlistfile"

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

for host in `cat $hostsfile`; do
	echo $host
	scp hadoop-env.sh $(whoami)@$host:/tmp/
	ssh -t $(whoami)@$host 'sudo mv /etc/hadoop/conf/hadoop-env.sh /etc/hadoop/conf/hadoop-env.sh.bak'
	ssh -t $(whoami)@$host 'sudo mv /tmp/hadoop-env.sh /etc/hadoop/conf/'
	
	#bounce mapred and hdfs
	
done 

