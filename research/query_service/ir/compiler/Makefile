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

GRAPH:=modern

ifeq ($(UNAME_S),Darwin)
    ifeq ($(UNAME_M),arm64)
        OSFLAG += -Dos.detected.classifier=osx-x86_64
    endif
endif

build:
	cd $(CUR_DIR)/../../../engine/pegasus/clients/java/client && \
		mvn clean install -DskipTests $(OSFLAG) && \
		cd $(CUR_DIR)/../core && cargo build --release && \
		cd ../integrated && cargo build --release --bin start_rpc_server && \
		cd $(CUR_DIR) && mvn clean package -DskipTests $(OSFLAG)

clean:
	cd $(CUR_DIR)/../../../engine/pegasus/clients/java/client && mvn clean && \
	cd $(CUR_DIR)/../ && cargo clean && \
	cd $(CUR_DIR) && mvn clean

test:
	cd $(CUR_DIR)/../ && cargo test && \
	cd $(CUR_DIR) && mvn test

gremlin_test:
	mvn test -Dtest=com.alibaba.graphscope.integration.IrGremlinTest $(OSFLAG)

submit:
	cd $(CUR_DIR) && $(java) \
	  -cp ".:./target/libs/*:./target/ir-compiler-1.0-SNAPSHOT.jar" \
	  -Djna.library.path=../target/release \
	  com.alibaba.graphscope.common.SubmitPlanServiceMain \
	  $(OPT)

run:
	cd $(CUR_DIR) && $(java) \
	  -cp ".:./target/libs/*:./target/ir-compiler-1.0-SNAPSHOT.jar" \
	  -Djna.library.path=../target/release \
	  -Dgraph.schema=../core/resource/${GRAPH}_schema.json \
	  com.alibaba.graphscope.gremlin.service.GraphServiceMain

# start rpc server
# make run GRAPH=ldbc
ldbc_test:
	mvn test -Dtest=com.alibaba.graphscope.integration.ldbc.IrLdbcTest $(OSFLAG)

.PHONY: build run
