#!/bin/bash

source "`dirname "$0"`"/maxgraph-env

exec "$JAVA" -server \
  -Dlogback.configurationFile="$MAXGRAPH_CONF_DIR/logback.xml" \
  -Dconfig.file="$MAXGRAPH_CONF_FILE" \
  -Dlog.dir="$LOG_DIR" \
  -Dlog.name="$LOG_NAME" \
  -cp "$MAXGRAPH_CLASSPATH" com.alibaba.maxgraph.v2.MaxNode \
  "$@" > "$LOG_DIR"/"$LOG_NAME".out 2>&1 <&- &

retval=$?
pid=$!
[ $retval -eq 0 ] || exit $retval

if ! ps -p $pid > /dev/null ; then
    exit 1
fi
exit 0