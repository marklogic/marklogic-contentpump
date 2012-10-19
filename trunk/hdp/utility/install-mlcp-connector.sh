#!/bin/bash
#should be run as root on the 
usage="Usage: install-mlcp-connector.sh --hosts hostlistfile"
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/../

source $DIR/utility/lib.sh
source $DIR/MANIFEST
checkArgs "$1" "$2" "$usage"

unzip -o -q $DIR/$connector_zip -d /tmp/mlconnector
unzip -o -q $DIR/$xcc_zip -d /tmp/xcc

#stop jobtracker
su - mapred -c "/usr/lib/hadoop/bin/hadoop-daemon.sh --config /etc/hadoop/conf stop jobtracker"

for host in `cat $hostsfile`; do
	echo $host
	#stop tasktracker, allow failure(in case tasktracker already stopped)
	ssh root@$host 'su - mapred -c "/usr/lib/hadoop/bin/hadoop-daemon.sh --config /etc/hadoop/conf stop tasktracker"'	
	
	#propagate xcc and connector jars
	rssh "root" "$host" 'mkdir /usr/lib/MarkLogic'
	rscp "/tmp/mlconnector/lib/marklogic-mapreduce-$connector_version.jar" "root" "$host" "/usr/lib/MarkLogic/"
	rscp "/tmp/mlconnector/lib/$commons_modeler" "root" "$host" "/usr/lib/MarkLogic/"
	rscp "/tmp/xcc/lib/marklogic-xcc-$xcc_version.jar" "root" "$host" "/usr/lib/MarkLogic/"
	
	#add a script that sets env variable
	rscp "$DIR/utility/hadist.sh" "root" "$host:/etc/profile.d/"
	
	#backup update hadoop-env.sh
	rssh "root" "$host" 'mv /etc/hadoop/conf/hadoop-env.sh /etc/hadoop/conf/.hadoop-env.sh.bak'
	rscp "$DIR/utility/hadoop-env.sh" "root" "$host" "/etc/hadoop/conf/"
done 

#start jobtracker
su - mapred -c "/usr/lib/hadoop/bin/hadoop-daemon.sh --config /etc/hadoop/conf start jobtracker"

#check if previous command ran successfully
checkStatus

for host in `cat $hostsfile`; do
	#start tasktracker, allow failure
	ssh root@$host 'su - mapred -c "/usr/lib/hadoop/bin/hadoop-daemon.sh --config /etc/hadoop/conf start tasktracker"'
done

rm -rf /tmp/mlconnector
rm -rf /tmp/xcc

unzip -o -q $DIR/$mlcp_zip -d /opt/

echo "DONE installing mlcp and connector"


