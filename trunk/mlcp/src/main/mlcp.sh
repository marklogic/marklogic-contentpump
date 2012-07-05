#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/../
VMARGS="-DCONTENTPUMP_HOME=$DIR/lib -DCONTENTPUMP_VERSION=1.0" 
LIB_HOME=$DIR/lib
n=0
for file in ${LIB_HOME}/*.jar
do
  if [ $n -eq 0 ]
  then
    CLASSPATH=$file
    n=1
  else
    CLASSPATH=${CLASSPATH}":"$file
  fi
done
CLASSPATH=$DIR/conf:$CLASSPATH
java -cp $CLASSPATH $VMARGS com.marklogic.contentpump.ContentPump $*
