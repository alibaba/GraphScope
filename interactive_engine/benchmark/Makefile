OPT?=poc

CUR_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

ifeq ($(JAVA_HOME),)
    java:=java
else
    java:=$(JAVA_HOME)/bin/java
endif

UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

config.path:=config/interactive-benchmark.properties
QUIET_OPT := --quiet

build:
	cd $(CUR_DIR) && mvn clean package ${QUIET_OPT} && \
	cd target && \
	tar zxvf gaia-benchmark-0.0.1-SNAPSHOT-dist.tar.gz > /dev/null

clean:
	cd $(CUR_DIR) && mvn clean

run:
	cd $(CUR_DIR) && $(java) \
	  -cp "$(CUR_DIR)/target/gaia-benchmark-0.0.1-SNAPSHOT/lib/*" \
	  com.alibaba.graphscope.gaia.benchmark.InteractiveBenchmark ${config.path}

collect:
	cd $(CUR_DIR) && $(java) \
	  -cp "$(CUR_DIR)/target/gaia-benchmark-0.0.1-SNAPSHOT/lib/*" \
	  com.alibaba.graphscope.gaia.benchmark.CollectResult ${config.path}

.PHONY: build run
