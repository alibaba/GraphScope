#!/bin/bash

BASE_DIR=$(dirname "$0")

/tmp/hadoop-2.10.1/bin/hadoop jar $LOADER_DIR/data-load-tool-0.0.1-SNAPSHOT.jar com.alibaba.graphscope.groot.dataload.databuild.OfflineBuild $BASE_DIR/databuild.config

