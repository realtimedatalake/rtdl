#!/bin/bash
/etc/init.d/ssh start

# Start HDFS
/root/hadoop-2.9.2/sbin/start-all.sh


# Prepare Postgres for Hive Metastore
su postgres -c '/usr/lib/postgresql/10/bin/pg_ctl -D /var/lib/postgresql/10/main2 start'
sleep 2
psql -U postgres -c "CREATE USER hiveuser WITH PASSWORD 'hiveuser';"
psql -U postgres -c "CREATE DATABASE metastore;"
/root/apache-hive-2.3.6-bin/bin/schematool -dbType postgres -initSchema


# Setup the HDFS directories needed for Hive
/root/hadoop-2.9.2/bin/hdfs dfs -mkdir /tmp
/root/hadoop-2.9.2/bin/hdfs dfs -mkdir -p /user/hive/warehouse
/root/hadoop-2.9.2/bin/hdfs dfs -chmod 777 /tmp
/root/hadoop-2.9.2/bin/hdfs dfs -chmod 777 /user/hive/warehouse


# Start Presto
export PRESTO_HOME=/root/presto-server-318

use_query_max_memory=${QUERY_MAX_MEMORY:-"1GB"}
use_query_max_memory_per_node=${QUERY_MAX_MEMORY_PER_NODE:-"512MB"}
use_query_max_total_memory_per_node=${QUERY_MAX_TOTAL_MEMORY_PER_NODE:-"1GB"}
use_jvm_heap=${JAVA_HEAP_SIZE:-"2G"}
use_heap_headroom=${HEAP_HEADROOM_PER_NODE:-"0.6GB"}

sed "s/{QUERY_MAX_MEMORY}/$use_query_max_memory/" $PRESTO_HOME/etc/config.properties.template | \
    sed "s/{QUERY_MAX_MEMORY_PER_NODE}/$use_query_max_memory_per_node/" | \
    sed "s/{QUERY_MAX_TOTAL_MEMORY_PER_NODE}/$use_query_max_total_memory_per_node/" | \
    sed "s/{HEAP_HEADROOM_PER_NODE}/$use_heap_headroom/" \
    > $PRESTO_HOME/etc/config.properties

sed "s/{JAVA_HEAP_SIZE}/$use_jvm_heap/" $PRESTO_HOME/etc/jvm.config.template \
    > $PRESTO_HOME/etc/jvm.config

sed "s~{AWS_ACCESS_KEY}~$AWS_ACCESS_KEY_ID~" $PRESTO_HOME/etc/catalog/hive.properties.template | \
    sed "s~{AWS_SECRET_ACCESS_KEY}~$AWS_SECRET_ACCESS_KEY~" \
    > $PRESTO_HOME/etc/catalog/hive.properties

$PRESTO_HOME/bin/launcher.py start

# Start Hive Metastore
nohup /root/apache-hive-2.3.6-bin/bin/hive --service metastore &
#/root/apache-hive-2.3.6-bin/bin/hive --service metastore

# Start Hive Service (this keeps running in the foreground)
sleep 10    # wait for metastore to start first
/root/apache-hive-2.3.6-bin/bin/hiveserver2
