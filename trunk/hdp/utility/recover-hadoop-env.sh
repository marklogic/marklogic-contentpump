#!/bin/bash

usage="Usage: recover-hadoop-env.sh --hosts hostlistfile"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/

source $DIR/lib.sh
checkArgs "$1" "$2" "$usage"

for host in `cat $hostsfile`; do
	echo $host
	rssh "root" "$host" "mv /etc/hadoop/conf/hadoop-env.sh.bak /etc/hadoop/conf/hadoop-env.sh"
done 

#bounce mapred and hdfs
$DIR/hdp-stop.sh --hosts $hostsfile
$DIR/hdp-start.sh --hosts $hostsfile
