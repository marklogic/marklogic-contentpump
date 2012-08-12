#!/bin/bash
unset CLASSPATH
if [ ! -z "$HADOOP_DIST" ] && [ "$HADOOP_DIST" = "HDP" ]; then
  if [ -z "$HADOOP_INSTALL_DIR" ]; then
    HADOOP_INSTALL_DIR=/usr/lib/hadoop
  fi
  if [ -z "$HADOOP_CONF_DIR" ]; then
    HADOOP_CONF_DIR=/etc/hadoop/conf
    export HADOOP_CONF_DIR
  fi
  CLASSPATH=$HADOOP_INSTALL_DIR/hadoop-core-1.0.3.jar
  CLASSPATH=$CLASSPATH:$HADOOP_INSTALL_DIR/lib/commons-configuration-1.6.jar
  CLASSPATH=$CLASSPATH:$HADOOP_INSTALL_DIR/lib/commons-lang-2.4.jar
  CLASSPATH=$CLASSPATH:$HADOOP_INSTALL_DIR/lib/jackson-core-asl-1.8.8.jar
  CLASSPATH=$CLASSPATH:$HADOOP_INSTALL_DIR/lib/jackson-mapper-asl-1.8.8.jar
  HDP=true
fi
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/../
VMARGS="-DCONTENTPUMP_HOME=$DIR/lib -DCONTENTPUMP_VERSION=1.0" 
LIB_HOME=$DIR/lib
for file in ${LIB_HOME}/*.jar
do
  if [ "$HDP" = "true" ] && [[ "$file" =~ .*hadoop-core-0.20.2.jar ]]; then 
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
