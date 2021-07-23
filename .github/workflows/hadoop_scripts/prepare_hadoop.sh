#!/bin/bash
set -e

BASE_DIR=$(dirname "$0")
HADOOP_HOME=$1

HOST=$(hostname -I | awk '{print $1}')

FS="hdfs://${HOST}:9000"
cp $BASE_DIR/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
sed s/DEFAULT_FS/${FS//\//\\/}/ $BASE_DIR/core-site.xml.template > $HADOOP_HOME/etc/hadoop/core-site.xml
sed -i 's/\${JAVA_HOME}/\/usr\/lib\/jvm\/default-java\//' $HADOOP_HOME/etc/hadoop/hadoop-env.sh

$HADOOP_HOME/bin/hdfs namenode -format
HADOOP_SSH_OPTS="-o StrictHostKeyChecking=no" $HADOOP_HOME/sbin/start-dfs.sh

