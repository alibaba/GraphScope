MKFILE_PATH				:= $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR				:= $(dir $(MKFILE_PATH))
ANALYTICAL_DIR			:= $(WORKING_DIR)/analytical_engine
INTERACTIVE_DIR			:= $(WORKING_DIR)/interactive_engine
LEARNING_DIR			:= $(WORKING_DIR)/learning_engine/graph-learn
ANALYTICAL_BUILD_DIR	:= $(ANALYTICAL_DIR)/build
LEARNING_BUILD_DIR		:= $(LEARNING_DIR)/graphlearn/cmake-build
CLIENT_DIR				:= $(WORKING_DIR)/python
COORDINATOR_DIR			:= $(WORKING_DIR)/coordinator
K8S_DIR					:= $(WORKING_DIR)/k8s
DOCS_DIR				:= $(WORKING_DIR)/docs

VERSION					?= 0.18.0

BUILD_TYPE				?= release

# analytical engine build options
NETWORKX				?= ON

# testing build option
BUILD_TEST				?= OFF


# INSTALL_PREFIX is environment variable, but if it is not set, then set default value
ifeq ($(INSTALL_PREFIX),)
    INSTALL_PREFIX := /opt/graphscope
endif

UNAME := $(shell uname)
ifeq ($(UNAME),Linux)
	NUMPROC := $(shell grep -c ^processor /proc/cpuinfo)
	SUFFIX := so
endif
ifeq ($(UNAME),Darwin)
	NUMPROC := $(shell sysctl -n hw.ncpu)
	SUFFIX := dylib
endif


## Common
.PHONY: all graphscope install clean

all: learning client coordinator analytical interactive
graphscope: all

install: analytical-install interactive-install learning-install client coordinator
    # client
	pip3 install --user --editable $(CLIENT_DIR)
	rm -rf $(CLIENT_DIR)/*.egg-info
    # coordinator
	pip3 install --user --editable $(COORDINATOR_DIR)
	rm -rf $(COORDINATOR_DIR)/*.egg-info

	echo "Run the following command to correctly set environment variable"
	echo "export GRAPHSCOPE_HOME=$(INSTALL_PREFIX)"

clean:
	rm -rf $(ANALYTICAL_BUILD_DIR) $(ANALYTICAL_DIR)/proto
	cd $(ANALYTICAL_DIR)/java && mvn clean

	cd $(INTERACTIVE_DIR) && mvn clean || true
	# TODO: use maven clean to clean ir target
	rm -rf $(INTERACTIVE_DIR)/executor/ir/target

	rm -rf $(LEARNING_BUILD_DIR) $(LEARNING_DIR)/proto/*.h $(LEARNING_DIR)/proto/*.cc

	cd $(CLIENT_DIR) && python3 setup.py clean --all

	cd $(COORDINATOR_DIR) && python3 setup.py clean --all

## Modules
.PHONY: client coordinator analytical interactive learning
.PHONY: analytical-java

client: learning
	cd $(CLIENT_DIR) && \
	pip3 install -r requirements.txt -r requirements-dev.txt --user && \
	python3 setup.py build_ext --inplace --user

coordinator: client
	cd $(COORDINATOR_DIR) && \
	pip3 install -r requirements.txt -r requirements-dev.txt --user && \
	python3 setup.py build_builtin

# We deliberately make $(ENGINE) depends on a file, and $(ENGINE)-install depends on $(ENGINE),
# so that when we execute `make $(ENGINE)-install` after `make $(ENGINE)`, it will not
# rebuild $(ENGINE) from scratch.
# If we doesn't make gxe depends on a file (as a PHONY), then make will never
# know if $(ENGINE) is up-to-date or not, so it will always rebuild gxe.
# Note: `$(ENGINE)` stands for `analytical`, `interactive` and `learning`.

.PHONY: analytical-install interactive-install learning-install
.PHONY: analytical-java-install

analytical-install: analytical
	$(MAKE) -C $(ANALYTICAL_BUILD_DIR) install
	install -d $(INSTALL_PREFIX)/lib/cmake/graphscope-analytical/cmake
	if [ -d "${INSTALL_PREFIX}/lib64/cmake/graphscope-analytical" ]; then \
		install $(INSTALL_PREFIX)/lib64/cmake/graphscope-analytical/*.cmake $(INSTALL_PREFIX)/lib/cmake/graphscope-analytical; \
		install $(INSTALL_PREFIX)/lib64/cmake/graphscope-analytical/cmake/* $(INSTALL_PREFIX)/lib/cmake/graphscope-analytical/cmake; \
	fi

analytical: $(ANALYTICAL_BUILD_DIR)/grape_engine

$(ANALYTICAL_BUILD_DIR)/grape_engine:
	mkdir -p $(ANALYTICAL_BUILD_DIR) && \
	cd $(ANALYTICAL_BUILD_DIR) && \
	cmake -DCMAKE_INSTALL_PREFIX=$(INSTALL_PREFIX) \
		-DNETWORKX=$(NETWORKX) \
		-DBUILD_TESTS=${BUILD_TEST} \
		-DENABLE_JAVA_SDK=OFF .. && \
	$(MAKE) -j$(NUMPROC)

analytical-java-install: analytical-java
	$(MAKE) -C $(ANALYTICAL_BUILD_DIR) install
	install -d $(INSTALL_PREFIX)/lib/cmake/graphscope-analytical/cmake
	if [ -d "${INSTALL_PREFIX}/lib64/cmake/graphscope-analytical" ]; then \
		install $(INSTALL_PREFIX)/lib64/cmake/graphscope-analytical/*.cmake $(INSTALL_PREFIX)/lib/cmake/graphscope-analytical; \
		install $(INSTALL_PREFIX)/lib64/cmake/graphscope-analytical/cmake/* $(INSTALL_PREFIX)/lib/cmake/graphscope-analytical/cmake; \
	fi

analytical-java: $(ANALYTICAL_BUILD_DIR)/graphx_runner

$(ANALYTICAL_BUILD_DIR)/graphx_runner:
	mkdir -p $(ANALYTICAL_BUILD_DIR) && \
	cd $(ANALYTICAL_BUILD_DIR) && \
	cmake -DCMAKE_INSTALL_PREFIX=$(INSTALL_PREFIX) \
		-DNETWORKX=$(NETWORKX) \
		-DBUILD_TESTS=${BUILD_TEST} \
		-DENABLE_JAVA_SDK=ON .. && \
	$(MAKE) -j$(NUMPROC)

interactive-install: interactive
	mkdir -p $(INSTALL_PREFIX)
	tar -xf $(INTERACTIVE_DIR)/assembly/target/graphscope.tar.gz --strip-components 1 -C $(INSTALL_PREFIX)
interactive: $(INTERACTIVE_DIR)/assembly/target/graphscope.tar.gz

$(INTERACTIVE_DIR)/assembly/target/graphscope.tar.gz:
	cd $(INTERACTIVE_DIR) && \
	mvn package -DskipTests -Drust.compile.mode=$(BUILD_TYPE) -P graphscope,graphscope-assembly --quiet

learning-install: learning
	mkdir -p $(INSTALL_PREFIX)
	$(MAKE) -C $(LEARNING_BUILD_DIR) install
learning: $(LEARNING_DIR)/graphlearn/built/lib/libgraphlearn_shared.$(SUFFIX)

$(LEARNING_DIR)/graphlearn/built/lib/libgraphlearn_shared.$(SUFFIX):
	git submodule update --init
	cd $(LEARNING_DIR) && git submodule update --init third_party/pybind11
	mkdir -p $(LEARNING_BUILD_DIR)
	cd $(LEARNING_BUILD_DIR) && \
	cmake -DCMAKE_INSTALL_PREFIX=$(INSTALL_PREFIX) \
		-DKNN=OFF \
		-DWITH_VINEYARD=ON \
		-DTESTING=${BUILD_TEST} .. && \
	$(MAKE) -j$(NUMPROC)

## wheels
.PHONY: prepare-client graphscope-docs

prepare-client:
	cd $(CLIENT_DIR) && \
	pip3 install -r requirements.txt --user && \
	pip3 install -r requirements-dev.txt --user && \
	python3 setup.py build_proto

graphscope-docs: prepare-client
	$(MAKE) -C $(DOCS_DIR)/ html


## Tests
.PHONY: test unittest minitest k8stest

test: unittest minitest k8stest

unittest:
	cd $(CLIENT_DIR) && \
	python3 -m pytest --cov=graphscope --cov-config=.coveragerc --cov-report=xml --cov-report=term -s -v ./graphscope/tests/unittest

minitest:
	pip3 install tensorflow==2.5.2 "pandas<1.5.0"
	cd $(CLIENT_DIR) && \
	python3 -m pytest --cov=graphscope --cov-config=.coveragerc --cov-report=xml --cov-report=term -s -v ./graphscope/tests/minitest

k8stest:
	pip3 install tensorflow==2.5.2 "pandas<1.5.0"
	cd $(CLIENT_DIR) && \
	python3 -m pytest --cov=graphscope --cov-config=.coveragerc --cov-report=xml --cov-report=term -s -v ./graphscope/tests/kubernetes

