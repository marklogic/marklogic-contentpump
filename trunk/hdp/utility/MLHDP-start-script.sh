#!/bin/bash

#run as root, and make sure root has passwordless ssh setup on each node

usage="Usage: MLHDP-start-script.sh --hosts hostlistfile"

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
