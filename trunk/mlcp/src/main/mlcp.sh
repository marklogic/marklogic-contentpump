#!/bin/bash
unset CLASSPATH
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/../
VMARGS="-DCONTENTPUMP_HOME=$DIR/lib" 
LIB_HOME=$DIR/lib
if [[ $HADOOP_MAPREDUCE_VERSION == "1" ]] ; then
    EXCLUS=*hadoop-*-2.0.4-*
else
    EXCLUS=*hadoop-*-2.0.0-*
fi
echo $EXCLUS
for file in ${LIB_HOME}/*.jar
do
  if [[ $file == ${EXCLUS} ]] ; then
    continue
  fi
  if [ ! -z "$CLASSPATH" ]; then
    CLASSPATH=${CLASSPATH}":"$file
  else
    CLASSPATH=$file
  fi
done
CLASSPATH=$DIR/conf:$CLASSPATH
java -cp $CLASSPATH $VMARGS $JVM_OPTS com.marklogic.contentpump.ContentPump $*
