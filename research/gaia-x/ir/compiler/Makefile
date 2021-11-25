OPT?=poc

CUR_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

build:
	cd $(CUR_DIR)/../core && cargo build --release && cd $(CUR_DIR) && mvn clean package

run:
	cd $(CUR_DIR) && java -cp ".:./target/libs/*:./target/ir-compiler-1.0-SNAPSHOT.jar" -Djna.library.path=../core/target/release com.alibaba.graphscope.common.SubmitPlanServiceMain $(OPT)

.PHONY: build run
