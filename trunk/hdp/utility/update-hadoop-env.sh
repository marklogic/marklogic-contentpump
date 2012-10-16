#!/bin/sh

usage="Usage: update-hadoop-env.sh --hosts hostlistfile"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/

source $DIR/lib.sh
checkArgs "$1" "$2" "$usage"


for host in `cat $hostsfile`; do
	echo $host
	scp $DIR/hadoop-env.sh root@$host:/tmp/
	ssh -t root@$host 'mv /etc/hadoop/conf/hadoop-env.sh /etc/hadoop/conf/hadoop-env.sh.bak'
	ssh -t root@$host 'mv /tmp/hadoop-env.sh /etc/hadoop/conf/'
done 


#bounce mapred and hdfs
echo "bounce mapred and hdfs"
sh $DIR/mlhdp-stop.sh --hosts $hostsfile
sh $DIR/mlhdp-start.sh --hosts $hostsfile
