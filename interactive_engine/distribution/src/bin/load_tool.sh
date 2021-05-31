#!/bin/bash

source "`dirname "$0"`"/maxgraph-env

exec "$JAVA" -jar "$MAXGRAPH_HOME/lib/data_load_tools-0.0.1-SNAPSHOT.jar" "$@"