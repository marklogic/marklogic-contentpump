#!/bin/sh

# This script makes two mlcp binary bundles for each Hadoop version and put the
# output in deliverable directory.

# install XCC jar
mvn install:install-file -DgroupId=com.marklogic -DartifactId=marklogic-xcc -Dversion=8.0 -Dfile=../xcc/buildtmp/java/marklogic-xcc-8.0.jar -Dpackaging=jar
# install connector jars
mvn install:install-file -DgroupId=com.marklogic -DartifactId=marklogic-mapreduce1 -Dversion=2.1 -Dfile=../mapreduce/buildtmp/java/marklogic-mapreduce1-2.1.jar -Dpackaging=jar
mvn install:install-file -DgroupId=com.marklogic -DartifactId=marklogic-mapreduce2 -Dversion=2.1 -Dfile=../mapreduce/buildtmp/java/marklogic-mapreduce2-2.1.jar -Dpackaging=jar
# install hadoop-deps poms
mvn install:install-file -DgroupId=com.marklogic -DartifactId=hadoop-deps -Dversion=1 -Dfile=hadoop-deps/pom-v1.xml -Dpackaging=pom
mvn install:install-file -DgroupId=com.marklogic -DartifactId=hadoop-deps -Dversion=2 -Dfile=hadoop-deps/pom-v2.xml -Dpackaging=pom
# prepare deliverable directory
rm -rf deliverable
mkdir deliverable
# build mlcp-Hadoop1 
mvn clean
mvn package
cp target/mlcp-Hadoop1* deliverable
# build mlcp-Hadoop2  
mvn clean
mvn package -Dhadoop-deps.version=2
cp target/mlcp-Hadoop2-*-bin.zip deliverable
