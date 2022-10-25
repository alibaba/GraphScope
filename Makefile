MKFILE_PATH				:= $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR				:= $(dir $(MKFILE_PATH))
GAE_DIR					:= $(WORKING_DIR)/analytical_engine
GIE_DIR					:= $(WORKING_DIR)/interactive_engine
GLE_DIR					:= $(WORKING_DIR)/learning_engine/graph-learn
GAE_BUILD_DIR			:= $(GAE_DIR)/build
GLE_BUILD_DIR			:= $(GLE_DIR)/cmake-build
CLIENT_DIR				:= $(WORKING_DIR)/python
COORDINATOR_DIR			:= $(WORKING_DIR)/coordinator
K8S_DIR					:= $(WORKING_DIR)/k8s
DOCS_DIR				:= $(WORKING_DIR)/docs

VERSION					?= 0.18.0

BUILD_TYPE				?= release

# GAE build options
NETWORKX				?= ON

# testing build option
BUILD_TEST				?= OFF

# build java sdk option
ENABLE_JAVA_SDK			?= ON

# PREFIX is environment variable, but if it is not set, then set default value
ifeq ($(INSTALL_PREFIX),)
    INSTALL_PREFIX := /opt/graphscope
endif

UNAME := $(shell uname)
ifeq ($(UNAME),Linux)
	NUMPROC := $(shell grep -c ^processor /proc/cpuinfo)
	SUFFIX := so
endif
ifeq ($(UNAME),Darwin)
	NUMPROC := $(shell sysctl hw.ncpu | awk '{print $2}')
	SUFFIX := dylib
endif


## Common
.PHONY: all graphscope install clean

# all: graphscope
# graphscope: gle client coordinator gae gie
all: gle client coordinator gae gie
graphscope: all

install: gae-install gie-install gle-install client coordinator
    # client
	pip3 install --user --editable $(CLIENT_DIR)
	rm -rf $(CLIENT_DIR)/*.egg-info
    # coordinator
	pip3 install --user --editable $(COORDINATOR_DIR)
	rm -rf $(COORDINATOR_DIR)/*.egg-info

	echo "Run the following command to correctly set environment variable"
	echo "export GRAPHSCOPE_HOME=$(INSTALL_PREFIX)"

clean:
	rm -rf $(GAE_BUILD_DIR) $(GAE_DIR)/proto
	cd $(GAE_DIR)/java && mvn clean

	cd $(GIE_DIR) && mvn clean
    # TODO: use maven clean to clean ir target
	rm -rf $(GIE_DIR)/executor/ir/target

	rm -rf $(GLE_BUILD_DIR) $(GLE_DIR)/proto/*.h $(GLE_DIR)/proto/*.cc

	cd $(CLIENT_DIR) && python3 setup.py clean --all

	cd $(COORDINATOR_DIR) && python3 setup.py clean --all

## Modules
.PHONY: client coordinator gae gie gle

client: gle
	cd $(CLIENT_DIR) && \
	pip3 install -r requirements.txt -r requirements-dev.txt --user && \
	python3 setup.py build_ext --inplace --user

coordinator: client
	cd $(COORDINATOR_DIR) && \
	pip3 install -r requirements.txt -r requirements-dev.txt --user && \
	python3 setup.py build_builtin

.PHONY: gae-install gie-install gle-install

gae-install: gae
	$(MAKE) -C $(GAE_BUILD_DIR) install
	install $(K8S_DIR)/kube_ssh $(INSTALL_PREFIX)/bin/
	install -d $(INSTALL_PREFIX)/lib/cmake/graphscope-analytical/cmake
	install $(INSTALL_PREFIX)/lib64/cmake/graphscope-analytical/* $(INSTALL_PREFIX)/lib/cmake/graphscope-analytical
	install $(INSTALL_PREFIX)/lib64/cmake/graphscope-analytical/cmake/* $(INSTALL_PREFIX)/lib/cmake/graphscope-analytical/cmake

gae: $(GAE_BUILD_DIR)/grape_engine

$(GAE_BUILD_DIR)/grape_engine:
	mkdir -p $(GAE_BUILD_DIR) && \
	cd $(GAE_BUILD_DIR) && \
	cmake -DCMAKE_INSTALL_PREFIX=$(INSTALL_PREFIX) \
		-DNETWORKX=$(NETWORKX) \
		-DBUILD_TESTS=${BUILD_TEST} \
		-DENABLE_JAVA_SDK=${ENABLE_JAVA_SDK} .. && \
	$(MAKE) -j$(NUMPROC)

gie-install: gie
	tar -xf $(GIE_DIR)/assembly/target/graphscope.tar.gz --strip-components 1 -C $(INSTALL_PREFIX)
gie: $(GIE_DIR)/assembly/target/graphscope.tar.gz

$(GIE_DIR)/assembly/target/graphscope.tar.gz:
    # frontend/executor
	cd $(GIE_DIR) && \
	mvn package -DskipTests -Drust.compile.mode=$(BUILD_TYPE) -P graphscope,graphscope-assembly --quiet

gle-install: gle
	$(MAKE) -C $(GLE_BUILD_DIR) install
gle: $(GLE_DIR)/built/lib/libgraphlearn_shared.$(SUFFIX)

$(GLE_DIR)/built/lib/libgraphlearn_shared.$(SUFFIX):
	git submodule update --init
	cd $(GLE_DIR) && git submodule update --init third_party/pybind11
	mkdir -p $(GLE_BUILD_DIR)
	cd $(GLE_BUILD_DIR) && \
	cmake -DCMAKE_INSTALL_PREFIX=$(INSTALL_PREFIX) \
		-DWITH_VINEYARD=ON \
		-DTESTING=${BUILD_TEST} .. && \
	$(MAKE) -j$(NUMPROC)

## wheels
.PHONY: graphscope-py3-package graphscope-client-py3-package prepare-client graphscope-docs

graphscope-py3-package:
	$(MAKE) -C $(K8S_DIR) graphscope-py3-package

graphscope-client-py3-package:
	 $(MAKE) -C $(K8S_DIR) graphscope-client-py3-package

prepare-client:
	cd $(CLIENT_DIR) && \
	pip3 install -r requirements.txt --user && \
	pip3 install -r requirements-dev.txt --user && \
	python3 setup.py build_proto

graphscope-docs: prepare-client
	$(MAKE) -C $(DOCS_DIR)/ html


## Images
.PHONY: graphscope-image jupyter-image dataset-image graphscope-store-image push

graphscope-image:
	$(MAKE) -C $(K8S_DIR) graphscope-image VERSION=$(VERSION)

jupyter-image:
	$(MAKE) -C $(K8S_DIR) jupyter-image VERSION=$(VERSION)

dataset-image:
	$(MAKE) -C $(K8S_DIR) dataset-image VERSION=$(VERSION)

graphscope-store-image:
	$(MAKE) -C $(K8S_DIR) graphscope-store-image VERSION=$(VERSION)

push:
	$(MAKE) -C $(K8S_DIR) push


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
