#!/bin/bash
unset CLASSPATH
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/../
VMARGS="-DCONTENTPUMP_HOME=$DIR/lib" 
LIB_HOME=$DIR/lib
for file in ${LIB_HOME}/*.jar
do
  if [ ! -z "$CLASSPATH" ]; then
    CLASSPATH=${CLASSPATH}":"$file
  else
    CLASSPATH=$file
  fi
done
CLASSPATH=$DIR/conf:$CLASSPATH
java -cp $CLASSPATH $VMARGS $JVM_OPTS com.marklogic.contentpump.ContentPump $*
