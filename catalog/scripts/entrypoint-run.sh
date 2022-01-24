#!/bin/bash

export CLASSPATH=`echo $CLASSPATH| sed 's/\/slf4j\-log4j12\-1\.7\.30\.jar//g'`

export HADOOP_VERSION=3.3.1
export METASTORE_VERSION=3.1.2
export AWS_SDK_VERSION=1.11.901

export JAVA_HOME=/usr/local/openjdk-8
export HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
export HADOOP_CLASSPATH=${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar:${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-${HADOOP_VERSION}.jar
export HIVE_HOME=/opt/apache-hive-metastore-${METASTORE_VERSION}-bin

${HIVE_HOME}/bin/start-metastore
