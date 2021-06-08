#!/bin/bash
set -e

BASE_DIR=$(dirname "$0")
HADOOP_HOME=$1

HOST=$(hostname -I | awk '{print $1}')

FS="hdfs://${HOST}:9000"
cp $BASE_DIR/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
sed s/DEFAULT_FS/${FS//\//\\/}/ $BASE_DIR/core-site.xml.template > $HADOOP_HOME/etc/hadoop/core-site.xml
sed -i 's/\${JAVA_HOME}/\/usr\/lib\/jvm\/default-java\//' $HADOOP_HOME/etc/hadoop/hadoop-env.sh

if grep "$(cat ~/.ssh/id_rsa.pub)" ~/.ssh/known_hosts; then
    echo "Already in known hosts."
else
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/known_hosts
fi

$HADOOP_HOME/bin/hdfs namenode -format
yes | $HADOOP_HOME/sbin/start-dfs.sh

