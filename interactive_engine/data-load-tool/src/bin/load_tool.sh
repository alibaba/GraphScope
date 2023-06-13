#!/usr/bin/env bash
#
# load_data tools

set -eo pipefail

SCRIPT="$0"
while [ -h "$SCRIPT" ] ; do
  ls=$(ls -ld "$SCRIPT")
  # Drop everything prior to ->
  link=$(expr "$ls" : '.*-> \(.*\)$')
  if expr "$link" : '/.*' > /dev/null; then
    SCRIPT="$link"
  else
    SCRIPT=$(dirname "$SCRIPT")/"$link"
  fi
done

LOAD_TOOL_HOME=$(dirname "$SCRIPT")
LOAD_TOOL_HOME=$(cd "$LOAD_TOOL_HOME"; pwd)
LOAD_TOOL_HOME=$(dirname "$LOAD_TOOL_HOME")


usage() {
cat <<EOF
  A script to launch data loading.

  Usage: load_tool.sh build/ingest/commit <config-file>
EOF
}

COMMAND=$1
CONFIG=$2

if [ "$COMMAND" = "-h" ] || [ "$COMMAND" = "--help" ]; then
  usage
  exit 0
fi

check_arguments() {
  if [ -z "$CONFIG" ]; then
    echo "No valid config file"
    usage
    exit 1
  fi

  JAR_FILE=$(find "$LOAD_TOOL_HOME"/.. -maxdepth 2 -type f -iname "data-load-tool-*.jar" | head -1)

  if [[ -z "${JAR_FILE}" ]]; then
      echo "Error: Could not find data-load-tool-*.jar within the $LOAD_TOOL_HOME"
      exit 1
  fi
}

if [ "$COMMAND" = "build" ]; then
  check_arguments
  exec hadoop jar "$JAR_FILE" com.alibaba.graphscope.groot.dataload.databuild.OfflineBuild "$CONFIG"
elif [ "$COMMAND" = "ingest" ] || [ "$COMMAND" = "commit" ]; then
  check_arguments
  exec java -cp "$JAR_FILE" com.alibaba.graphscope.groot.dataload.LoadTool -c "$COMMAND" -f "$CONFIG"
else
  usage
fi
