#!/bin/sh

usage="Usage: recover-hadoop-classpath.sh --hosts hostlistfile"

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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/

for host in `cat $hostsfile`; do
	echo $host
	ssh -t $(whoami)@$host 'sudo mv /etc/hadoop/conf/hadoop-env.sh.bak /etc/hadoop/conf/hadoop-env.sh'
done 


#bounce mapred and hdfs
sh $DIR/MLHDP-stop-script.sh --hosts $hostsfile
sh $DIR/MLHDP-start-script.sh --hosts $hostsfile
