#!/bin/bash

BASE_DIR=$(dirname "$0")
HADOOP_HOME=$1

HOST=$(hostname)

cp $BASE_DIR/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
sed s/DEFAULT_FS/hdfs:\/\/$HOST:9000/ $BASE_DIR/core-site.xml.template > $HADOOP_HOME/etc/hadoop/core-site.xml
sed -i 's/\${JAVA_HOME}/\/usr\/lib\/jvm\/default-java\//' $HADOOP_HOME/etc/hadoop/hadoop-env.sh

$HADOOP_HOME/bin/hdfs namenode -format
yes | $HADOOP_HOME/sbin/start-dfs.sh

