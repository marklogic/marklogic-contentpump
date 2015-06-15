#!/bin/bash
unset CLASSPATH
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/../
LIB_HOME=$DIR/lib

for file in "${LIB_HOME}"/*.jar
do
  if [ ! -z "$CLASSPATH" ]; then
    CLASSPATH=${CLASSPATH}":"$file
  else
    CLASSPATH=$file
  fi
done
CLASSPATH=$DIR/conf:$CLASSPATH
java -cp "$CLASSPATH" -DCONTENTPUMP_HOME="$DIR/lib" -Dfile.encoding=UTF-8 $JVM_OPTS -Djava.library.path="$DIR/lib/native" com.marklogic.contentpump.ContentPump $*
