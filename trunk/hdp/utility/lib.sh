#!/bin/bash

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

checkStatus() {
	if [ $? -ne 0 ]; then
		echo "Operation failed."
		exit 1
	fi
}

rssh() {
	if [ $# -lt 3 ]; then
		echo "rssh expects 3 arguments"
		exit 1
	fi
	ssh -t $1@$2 $3
	checkStatus
}

rscp() {
	if [ $# -lt 4 ]; then
		echo "rscp expects 4 arguments"
		exit 1
	fi
	scp $1 $2@$3:$4
	checkStatus
}
