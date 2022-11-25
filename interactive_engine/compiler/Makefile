OPT?=poc

CUR_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

ifeq ($(JAVA_HOME),)
    java:=java
else
    java:=$(JAVA_HOME)/bin/java
endif

OSFLAG  :=
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

graph.schema:=

rpc.target:=start_rpc_server

ifeq ($(UNAME_S),Darwin)
    ifeq ($(UNAME_M),arm64)
        OSFLAG += -Dos.detected.classifier=osx-x86_64
    endif
endif

build:
	cd $(CUR_DIR)/.. && \
		mvn clean install -DskipTests -Pexperimental --quiet $(OSFLAG) && \
		cd $(CUR_DIR)/../executor/ir/integrated && cargo build --release --bin $(rpc.target)

clean:
	cd $(CUR_DIR)/../executor/engine/pegasus/clients/java/client && mvn clean && \
	cd $(CUR_DIR)/../executor/ir && cargo clean && \
	cd $(CUR_DIR) && mvn clean

test:
	cd $(CUR_DIR)/../executor/ir && cargo test && \
	cd $(CUR_DIR)/../executor/ir && cargo test --features with_v6d && \
	cd $(CUR_DIR) && mvn test

# start rpc server
# make run
gremlin_test:
	mvn test -Dtest=com.alibaba.graphscope.integration.IrGremlinTest $(OSFLAG)

submit:
	cd $(CUR_DIR) && $(java) \
	  -cp ".:./target/libs/*:./target/compiler-1.0-SNAPSHOT.jar" \
	  -Djna.library.path=../executor/ir/target/release \
	  com.alibaba.graphscope.common.SubmitPlanServiceMain \
	  $(OPT)

run:
	cd $(CUR_DIR) && $(java) \
	  -cp ".:./target/libs/*:./target/compiler-1.0-SNAPSHOT.jar" \
	  -Djna.library.path=../executor/ir/target/release \
	  -Dgraph.schema=${graph.schema} \
	  -Dpegasus.hosts=${pegasus.hosts} \
	  -Dpegasus.server.num=${pegasus.server.num} \
	  com.alibaba.graphscope.gremlin.service.GraphServiceMain

# start rpc server
# make run graph.schema:=../executor/ir/core/resource/ldbc_schema.json 
ldbc_test:
	mvn test -Dtest=com.alibaba.graphscope.integration.ldbc.IrLdbcTest $(OSFLAG)

# start rpc server
# make run graph.schema:=../executor/ir/core/resource/ldbc_schema.json
pattern_test:
	mvn test -Dtest=com.alibaba.graphscope.integration.pattern.IrPatternTest $(OSFLAG)

.PHONY: build run
