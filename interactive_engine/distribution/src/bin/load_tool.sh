#!/bin/bash

source "`dirname "$0"`"/maxgraph-env

exec "$JAVA" -cp "$MAXGRAPH_HOME/lib/data_load_tools-0.0.1-SNAPSHOT.jar" com.alibaba.maxgraph.dataload.LoadTool "$@"

