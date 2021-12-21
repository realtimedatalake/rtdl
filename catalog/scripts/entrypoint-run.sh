#!/bin/sh

export HADOOP_VERSION=3.3.1
export METASTORE_VERSION=3.1.2

export JAVA_HOME=/usr/local/openjdk-8
export HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
export HIVE_HOME=/opt/apache-hive-metastore-${METASTORE_VERSION}-bin

${HIVE_HOME}/bin/start-metastore
