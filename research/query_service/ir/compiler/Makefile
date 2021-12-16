OPT?=poc

CUR_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

build:
	cd $(CUR_DIR)/../../../engine/pegasus/clients/java/client && mvn clean install && cd $(CUR_DIR)/../core && cargo build --release && cd $(CUR_DIR) && mvn clean package -DskipTests

run:
	cd $(CUR_DIR) && java -cp ".:./target/libs/*:./target/ir-compiler-1.0-SNAPSHOT.jar" -Djna.library.path=../target/release com.alibaba.graphscope.common.SubmitPlanServiceMain $(OPT)

server:
	cd $(CUR_DIR) && java -cp ".:./target/libs/*:./target/ir-compiler-1.0-SNAPSHOT.jar" -Djna.library.path=../target/release com.alibaba.graphscope.gremlin.service.GraphServiceMain

.PHONY: build run
