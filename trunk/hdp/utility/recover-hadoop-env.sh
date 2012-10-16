#!/bin/sh

usage="Usage: recover-hadoop-env.sh --hosts hostlistfile"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/

source $DIR/lib.sh
checkArgs "$1" "$2" "$usage"

for host in `cat $hostsfile`; do
	echo $host
	ssh -t $(whoami)@$host 'sudo mv /etc/hadoop/conf/hadoop-env.sh.bak /etc/hadoop/conf/hadoop-env.sh'
done 


#bounce mapred and hdfs
sh $DIR/mlhdp-stop.sh --hosts $hostsfile
sh $DIR/mlhdp-start.sh --hosts $hostsfile
