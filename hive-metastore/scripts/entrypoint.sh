#!/bin/sh

export JAVA_HOME=/usr/local/openjdk-8
export HADOOP_HOME=/opt/hadoop-3.3.1
#for aws s3; export HADOOP_CLASSPATH=${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar:${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-3.2.0.jar

/opt/apache-hive-metastore-3.0.0-bin/bin/schematool -initSchema -dbType postgresql
/opt/apache-hive-metastore-3.0.0-bin/bin/start-metastore
