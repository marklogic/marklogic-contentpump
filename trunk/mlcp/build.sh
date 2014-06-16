#!/bin/sh

# This script makes two mlcp binary bundles for each Hadoop version and put the
# output in deliverable directory.

# install XCC jar
mvn install:install-file -DgroupId=com.marklogic -DartifactId=marklogic-xcc -Dversion=8.0 -Dfile=../xcc/buildtmp/java/marklogic-xcc-8.0.jar -Dpackaging=jar
# install connector jars
mvn install:install-file -DgroupId=com.marklogic -DartifactId=marklogic-mapreduce2 -Dversion=2.1 -Dfile=../mapreduce/buildtmp/java/marklogic-mapreduce2-2.1.jar -Dpackaging=jar

# prepare deliverable directory
rm -rf deliverable
mkdir deliverable

# build mlcp-Hadoop2  
mvn clean
mvn package 
cp target/mlcp-Hadoop2*.zip deliverable
