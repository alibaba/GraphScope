OPT?=poc

CUR_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

ifeq ($(JAVA_HOME),)
    java:=java
else
    java:=$(JAVA_HOME)/bin/java
endif

UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

graph.schema:=
graph.store:=

rpc.target:=start_rpc_server_csr

target.revision:=0.0.1-SNAPSHOT

QUIET_OPT := --quiet

query:=
physical:=
procedure:=
extra:=
config.path:=conf/ir.compiler.properties

build:
	cd $(CUR_DIR)/.. && \
		mvn clean install -DskipTests -Drevision=${target.revision} -Pexperimental ${QUIET_OPT} && \
		cd $(CUR_DIR)/../executor/ir/integrated && cargo build --release --bin $(rpc.target)

clean:
	cd $(CUR_DIR)/../executor/engine/pegasus/clients/java/client && mvn clean && \
	cd $(CUR_DIR)/../executor/ir && cargo clean && \
	cd $(CUR_DIR) && mvn clean

test:
	cd $(CUR_DIR)/../executor/ir && cargo test && \
	cd $(CUR_DIR)/../executor/ir && cargo test --features with_v6d && \
	cd $(CUR_DIR)/../executor/ir && cargo test --features with_global_query && \
	cd $(CUR_DIR) && mvn test

# start rpc server
# make run
gremlin_test:
	mvn test -Dtest=com.alibaba.graphscope.gremlin.integration.standard.IrGremlinTest

run:
	cd $(CUR_DIR) && $(java) \
	  -cp ".:./target/libs/*:./target/compiler-0.0.1-SNAPSHOT.jar" \
	  -Djna.library.path=../executor/ir/target/release \
	  -Dgraph.schema=${graph.schema} \
	  -Dgraph.store=${graph.store} \
	  -Dpegasus.hosts=${pegasus.hosts} \
	  com.alibaba.graphscope.GraphServer ${config.path}

# make physical_plan config.path='<path to the config file>'
# 					 query='<query in string>'
# 					 physical='<path to the physical output file>'
# 					 procedure='<path_to_procedure_file>'
# 					 extra='<extra_key_value_config_pairs>'
physical_plan:
	cd $(CUR_DIR) && $(java) \
		-cp ".:./target/libs/*:./target/compiler-0.0.1-SNAPSHOT.jar" \
		-Djna.library.path=../executor/ir/target/release \
		-Dgraph.schema=${graph.schema} \
		com.alibaba.graphscope.common.ir.tools.GraphPlanner "${config.path}" "$(query)" "$(physical)" "$(procedure)" "$(extra)"

# start rpc server
# make run graph.schema:=../executor/ir/core/resource/ldbc_schema.json 
ldbc_test:
	# todo: need to rewrite gremlin ldbc queries
	# mvn test -Dtest=com.alibaba.graphscope.gremlin.integration.ldbc.IrLdbcTest
	mvn test -Dtest=com.alibaba.graphscope.cypher.integration.ldbc.IrLdbcTest

# start rpc server
# make run graph.schema:=../executor/ir/core/resource/ldbc_schema.json
pattern_test:
	mvn test -Dtest=com.alibaba.graphscope.gremlin.integration.pattern.IrPatternTest

cypher_test:
	mvn test -Dtest=com.alibaba.graphscope.cypher.integration.movie.MovieTest

.PHONY: build run
