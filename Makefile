
MKFILE_PATH 			:= $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR 			:= $(dir $(MKFILE_PATH))
GAE_DIR                 := $(WORKING_DIR)/analytical_engine
GIE_DIR					:= $(WORKING_DIR)/interactive_engine
GLE_DIR					:= $(WORKING_DIR)/learning_engine/graph-learn
GAE_BUILD_DIR			:= $(GAE_DIR)/build
GLE_BUILD_DIR			:= $(GLE_DIR)/build
CLIENT_DIR              := $(WORKING_DIR)/python
COORDINATOR_DIR         := $(WORKING_DIR)/coordinator

VERSION                     ?= 0.18.0

BUILD_TYPE                  ?= release

# GAE build options
NETWORKX                    ?= ON

# testing build option
BUILD_TEST                  ?= OFF

# build java sdk option
ENABLE_JAVA_SDK             ?= ON

# PREFIX is environment variable, but if it is not set, then set default value
ifeq ($(INSTALL_PREFIX),)
    INSTALL_PREFIX := /opt/graphscope
endif

## Common
.PHONY: all, graphscope, install, clean

all: graphscope

graphscope: gle client gae gie coordinator

install: graphscope
	# client
	pip3 install --user --editable $(CLIENT_DIR)
	
	# gae
	$(MAKE) -C $(GAE_BUILD_DIR) install

	# gie
	tar -xf $(GIE_DIR)/assembly/target/graphscope.tar.gz --strip-components 1 -C $(INSTALL_PREFIX)

	# gle
	$(MAKE) -C $(GLE_BUILD_DIR) install

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
.PHONY: client, coordinator, gae, gie, gle

client: gle
	cd $(CLIENT_DIR)
	pip3 install -r requirements.txt -r requirements-dev.txt --user
	python3 setup.py build_ext --inplace --user

coordinator: client
	cd $(COORDINATOR_DIR)
	pip3 install -r requirements.txt -r requirements-dev.txt --user
	python3 setup.py build_builtin

gae:
	mkdir -p $(GAE_BUILD_DIR)
	cd $(GAE_BUILD_DIR)
	cmake -DCMAKE_INSTALL_PREFIX=$(INSTALL_PREFIX) -DNETWORKX=$(NETWORKX) -DBUILD_TESTS=${BUILD_TEST} -DENABLE_JAVA_SDK=${ENABLE_JAVA_SDK} ..
	$(MAKE) -j1

gie:
	# frontend/executor
	cd $(GIE_DIR)
	mvn package -DskipTests -Drust.compile.mode=$(BUILD_TYPE) -P graphscope,graphscope-assembly --quiet

gle:
	cd $(WORKING_DIR)
	git submodule update --init
	cd $(GLE_DIR)
	git submodule update --init third_party/pybind11
	mkdir -p $(GLE_BUILD_DIR)
	cd $(GLE_BUILD_DIR)
	cmake -DCMAKE_INSTALL_PREFIX=$(INSTALL_PREFIX) -DWITH_VINEYARD=ON -DTESTING=${BUILD_TEST} ..
	$(MAKE) -j`nproc`

## wheels
.PHONY: graphscope-py3-package, graphscope-client-py3-package, prepare-client, graphscope-docs

graphscope-py3-package:
	$(MAKE) -C $(WORKING_DIR)/k8s/ graphscope-py3-package

graphscope-client-py3-package:
	 $(MAKE) -C $(WORKING_DIR)/k8s/ graphscope-client-py3-package

prepare-client:
	cd $(CLIENT_DIR)
	pip3 install -r requirements.txt --user
	pip3 install -r requirements-dev.txt --user
	python3 setup.py build_proto

graphscope-docs: prepare-client
	$(MAKE) -C $(WORKING_DIR)/docs/ html


## Images
.PHONY: graphscope-image, jupyter-image, dataset-image, graphscope-store-image, push

graphscope-image:
	$(MAKE) -C $(WORKING_DIR)/k8s/ graphscope-image VERSION=$(VERSION)

jupyter-image:
	$(MAKE) -C $(WORKING_DIR)/k8s/ jupyter-image VERSION=$(VERSION)

dataset-image:
	$(MAKE) -C $(WORKING_DIR)/k8s/ dataset-image VERSION=$(VERSION)

graphscope-store-image:
	$(MAKE) -C $(WORKING_DIR)/k8s/ graphscope-store-image VERSION=$(VERSION)

push:
	$(MAKE) -C $(WORKING_DIR)/k8s/ push


## Tests
.PHONY: test, unittest, minitest, k8stest

test: unittest minitest k8stest

unittest:
	cd $(CLIENT_DIR)
	python3 -m pytest --cov=graphscope --cov-config=.coveragerc --cov-report=xml --cov-report=term -s -v ./graphscope/tests/unittest

minitest:
	cd $(CLIENT_DIR)
	pip3 install tensorflow==2.5.2 "pandas<1.5.0"
	python3 -m pytest --cov=graphscope --cov-config=.coveragerc --cov-report=xml --cov-report=term -s -v ./graphscope/tests/minitest

k8stest:
	cd $(CLIENT_DIR)
	pip3 install tensorflow==2.5.2 "pandas<1.5.0"
	python3 -m pytest --cov=graphscope --cov-config=.coveragerc --cov-report=xml --cov-report=term -s -v ./graphscope/tests/kubernetes
