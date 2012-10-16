#! /bin/bash

checkArgs() {
	# if no args specified, show usage

	if [ $# -le 2 ]; then
		echo ${@: -1}
		exit 1
	fi
	
	if [ $# -gt 1 ]
	then
	    if [ "--hosts" = "$1" ]
	    then
	        if [ -e "$2" ]
	        then
	        	hostsfile=$2
	        else
	        	echo "file not found: $2"
	        	exit 1
	        fi
	    else
	    	echo ${@: -1}
	    	exit 1
	    fi
	fi
}