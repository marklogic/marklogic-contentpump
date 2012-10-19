#!/bin/bash

#run as root, and make sure root has passwordless ssh setup on each node

usage="Usage: hdp-start.sh --hosts hostlistfile"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/

source $DIR/lib.sh
checkArgs "$1" "$2" "$usage"

#start HDFS
su - hdfs -c "/usr/lib/hadoop/bin/hadoop-daemon.sh --config /etc/hadoop/conf start namenode"
su - hdfs -c "/usr/lib/hadoop/bin/hadoop-daemon.sh --config /etc/hadoop/conf start secondarynamenode"
for line in `cat $hostsfile`; do
  echo $line
  ssh root@$line 'su - hdfs -c "/usr/lib/hadoop/bin/hadoop-daemon.sh --config /etc/hadoop/conf start datanode"'
done

#start MapReduce

su - mapred -c "/usr/lib/hadoop/bin/hadoop-daemon.sh --config /etc/hadoop/conf start jobtracker"
for line in `cat $hostsfile`; do
  echo $line
  ssh root@$line 'su - mapred -c "/usr/lib/hadoop/bin/hadoop-daemon.sh --config /etc/hadoop/conf start tasktracker"'
done
su - mapred -c "/usr/lib/hadoop/bin/hadoop-daemon.sh --config /etc/hadoop/conf start historyserver"
