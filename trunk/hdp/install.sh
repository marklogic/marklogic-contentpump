#!/bin/sh

usage="Usage: install.sh --hosts hostlistfile"

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
    	exit 1
    fi
fi

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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/

chmod 755 -R install.sh utility/
$DIR/utility/install-mlcp-connector.sh --hosts $hostsfile

echo "Now updating hadoop env"

$DIR/utility/update-hadoop-env.sh --hosts $hostsfile

echo "done"


