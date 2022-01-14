OPT?=poc

CUR_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

build:
	cd $(CUR_DIR)/../../../engine/pegasus/clients/java/client && \
		mvn clean install -Dos.detected.classifier=osx-x86_64 && \
		cd $(CUR_DIR)/../core && cargo build --release && \
		cd $(CUR_DIR) && mvn clean package 

run_server:
	cd $(CUR_DIR) && java -cp ".:./target/libs/*:./target/ir-compiler-1.0-SNAPSHOT.jar" -Djna.library.path=../target/release com.alibaba.graphscope.gremlin.service.GraphServiceMain

run_test:
	mvn clean test -Dtest=com.alibaba.graphscope.integration.IrGremlinTest

.PHONY: build run
