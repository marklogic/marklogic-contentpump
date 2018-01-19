#!/bin/bash
unset CLASSPATH
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/../
VMARGS="-Dfile.encoding=UTF-8"
LIB_HOME=$DIR/lib
BUNDLE_ARTIFACT="apache"

if [ -f "$DIR/BUNDLE_ARTIFACT" ]
then
  BUNDLE_ARTIFACT=$(cat "$DIR/BUNDLE_ARTIFACT")
fi

for file in "${LIB_HOME}"/*.jar
do
  if [ ! -z "$CLASSPATH" ]; then
    CLASSPATH=${CLASSPATH}":"$file
  else
    CLASSPATH=$file
  fi
done
CLASSPATH=$DIR/conf:$CLASSPATH
java -cp "$CLASSPATH" -DCONTENTPUMP_HOME="$DIR/lib/" -DBUNDLE_ARTIFACT=$BUNDLE_ARTIFACT $VMARGS $JVM_OPTS -Djava.library.path="$DIR/lib/native" com.marklogic.contentpump.ContentPump "$@"
