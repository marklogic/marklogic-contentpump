#!/bin/sh

while true; do
    read -p "Have you modified gsInstaller.properties?" yn
    case $yn in
        [Yy] ) echo "install..."; 
        	cd HDP-gsInstaller-1.1.0.15/gsInstaller; sh gsPreRequisites.sh; sh createUsers.sh; sh gsInstaller.sh; break;;
        [Nn] ) echo "Please modify gsInstaller.properties before running install.sh"; exit;;
        * ) echo "Please answer y or n.";;
    esac
done

cd ../..
echo "Now install mlcp and connector"

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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/

#sh $DIR/utility/install-mlcp-connector.sh --hosts $hostsfile

echo "Now updating hadoop classpath"

#sh $DIR/utility/update-hadoop-classpath.sh --hosts $hostsfile

echo "done"


