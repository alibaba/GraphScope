#!/bin/bash

source "`dirname "$0"`"/maxgraph-env

JVM_OPTIONS="-Djava.awt.headless=true -Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8"
JVM_OPTIONS="-XX:+UseG1GC -XX:ConcGCThreads=2 -XX:ParallelGCThreads=5 -XX:MaxGCPauseMillis=50 $JVM_OPTIONS"
JVM_OPTIONS="-XX:InitiatingHeapOccupancyPercent=20 -XX:-OmitStackTraceInFastThrow $JVM_OPTIONS"
JVM_OPTIONS="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${LOG_DIR}/${MAXGRAPH_LOG_NAME}.hprof $JVM_OPTIONS"
JVM_OPTIONS="-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution $JVM_OPTIONS"
JVM_OPTIONS="-XX:+PrintGCApplicationStoppedTime -Xloggc:${LOG_DIR}/${MAXGRAPH_LOG_NAME}.gc.log -XX:+UseGCLogFileRotation $JVM_OPTIONS"
JVM_OPTIONS="-XX:NumberOfGCLogFiles=32 -XX:GCLogFileSize=64m $JVM_OPTIONS"

MAXGRAPH_JAVA_OPTS="-server $JVM_OPTIONS $MAXGRAPH_JAVA_OPTS"

exec "$JAVA" \
  $MAXGRAPH_JAVA_OPTS \
  -Dlogback.configurationFile="$MAXGRAPH_CONF_DIR/logback.xml" \
  -Dconfig.file="$MAXGRAPH_CONF_FILE" \
  -Dlog.dir="$LOG_DIR" \
  -Dlog.name="$LOG_NAME" \
  -cp "$MAXGRAPH_CLASSPATH" com.alibaba.maxgraph.v2.MaxGraph \
  "$@" > "$LOG_DIR"/"$LOG_NAME".out 2>&1 <&- &

retval=$?
pid=$!
[ $retval -eq 0 ] || exit $retval

if ! ps -p $pid > /dev/null ; then
    exit 1
fi
exit 0
