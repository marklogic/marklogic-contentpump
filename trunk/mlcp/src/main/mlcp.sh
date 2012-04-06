#!/bin/sh
#mlcp.sh should be run under bin

VMARGS="-DCONTENTPUMP_HOME=../lib -DCONTENTPUMP_VERSION=1.0"
TARGET=../lib/marklogic-contentpump-1.0.jar
LIB_HOME=../lib
CLASSPATH=$LIB_HOME/commons-cli-1.2.jar
CLASSPATH=$CLASSPATH:$LIB_HOME/commons-logging-1.1.1.jar
CLASSPATH=$CLASSPATH:$LIB_HOME/marklogic-mapreduce-1.1.jar
CLASSPATH=$CLASSPATH:$LIB_HOME/marklogic-xcc-5.1.jar
CLASSPATH=$CLASSPATH:$LIB_HOME/hadoop-core-0.20.2.jar
CLASSPATH=$CLASSPATH:$LIB_HOME/marklogic-contentpump-1.0.jar
CLASSPATH=$CLASSPATH:$LIB_HOME/commons-codec-1.3.jar
echo $CLASSPATH

java -cp $CLASSPATH $VMARGS com.marklogic.contentpump.ContentPump $*
