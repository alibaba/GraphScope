#!/bin/bash

source "`dirname "$0"`"/maxgraph-env

MAXGRAPH_JAVA_OPTS="-server $MAXGRAPH_JAVA_OPTS"

exec "$JAVA" \
  $MAXGRAPH_JAVA_OPTS \
  -Dlogback.configurationFile="$MAXGRAPH_CONF_DIR/logback.xml" \
  -Dconfig.file="$MAXGRAPH_CONF_FILE" \
  -Dlog.dir="$LOG_DIR" \
  -Dlog.name="$LOG_NAME" \
  -cp "$MAXGRAPH_CLASSPATH" com.alibaba.maxgraph.v2.AdminTools \
  "$@"
