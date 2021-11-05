OPT?=poc

CUR_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

build:
	cd $(CUR_DIR)/../core && cargo build --release && cp ./target/release/libir_core.dylib /usr/local/lib && cd $(CUR_DIR) && mvn clean package

run:
	cd $(CUR_DIR) && java -cp ".:./target/libs/*:./target/ir-compiler-1.0-SNAPSHOT.jar" com.alibaba.graphscope.common.SubmitPlanServiceMain $(OPT)

.PHONY: build run
