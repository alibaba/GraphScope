#!/usr/bin/env bash
#
# load_data tools

set -e
set -o pipefail

SCRIPT="$0"
while [ -h "$SCRIPT" ] ; do
  ls=`ls -ld "$SCRIPT"`
  # Drop everything prior to ->
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    SCRIPT="$link"
  else
    SCRIPT=`dirname "$SCRIPT"`/"$link"
  fi
done

LOAD_TOOL_HOME=`dirname "$SCRIPT"`
LOAD_TOOL_HOME=`cd "$LOAD_TOOL_HOME"; pwd`
LOAD_TOOL_HOME=`dirname "$LOAD_TOOL_HOME"`
JAR_FILE="$(echo "$LOAD_TOOL_HOME"/lib/*.jar | tr ' ' ':')"

if [ "$1" = "hadoop-build" ]; then
  hadoop_build_config=$2
  if [ -z "$hadoop_build_config" ]; then
    echo "no valid hadoop build config file"
    exit 1
  fi
  exec hadoop jar $JAR_FILE com.alibaba.maxgraph.dataload.databuild.OfflineBuild $hadoop_build_config
else
  if [ ! -z "$JAVA_HOME" ]; then
    JAVA="$JAVA_HOME/bin/java"
  fi
  if [ ! -x "$JAVA" ]; then
    echo "no valid JAVA_HOME" >&2
    exit 1
  fi
  exec "$JAVA" -cp $JAR_FILE com.alibaba.maxgraph.dataload.LoadTool "$@"
fi
