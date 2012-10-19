#!/bin/bash

usage="Usage: remove-mlcp-connector.sh --hosts hostlistfile"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/

source $DIR/lib.sh
source $DIR/../MANIFEST
checkArgs "$1" "$2" "$usage"

#remove mlcp locally
sudo rm -rf /opt/marklogic-contentpump-$mlcp_version

#stop jobtracker, allow failure(in case jobtracker already stopped)
su - mapred -c "/usr/lib/hadoop/bin/hadoop-daemon.sh --config /etc/hadoop/conf stop jobtracker"

for host in `cat $hostsfile`; do
	echo $host
	#stop tasktracker, allow failure(in case tasktracker already stopped)
	ssh root@$host 'su - mapred -c "/usr/lib/hadoop/bin/hadoop-daemon.sh --config /etc/hadoop/conf stop tasktracker"'
	
	#remove connector and XCC
	rssh "root" "$host" "rm -rf /usr/lib/MarkLogic"
	
	#remove env variable (not critical, allow failure)
	ssh root@$host "rm -rf /etc/profile.d/ml.sh"
	
	#recover hadoop-env
	rssh "root" "$host" "mv /etc/hadoop/conf/.hadoop-env.sh.bak /etc/hadoop/conf/hadoop-env.sh"
done

#start jobtracker
su - mapred -c "/usr/lib/hadoop/bin/hadoop-daemon.sh --config /etc/hadoop/conf start jobtracker"

#check if previous command ran successfully
checkStatus

for host in `cat $hostsfile`; do
	#start tasktracker, allow failure
	ssh root@$host 'su - mapred -c "/usr/lib/hadoop/bin/hadoop-daemon.sh --config /etc/hadoop/conf start tasktracker"'
done

echo "DONE removing mlcp and connector"