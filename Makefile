MKFILE_PATH 			:= $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR 			:= $(dir $(MKFILE_PATH))
OS                      := $(shell uname -s)

VERSION                     ?= 0.1.0
INSTALL_PREFIX              ?= /opt/graphscope

BUILD_TYPE                  ?= release

# GAE build options
NETWORKX                    ?= ON

# client build options
WITH_LEARNING_ENGINE        ?= ON

# testing build option
BUILD_TEST                  ?= OFF

.PHONY: all
all: graphscope

.PHONY: graphscope
graphscope: install

.PHONY: gsruntime-image
gsruntime:
	$(MAKE) -C $(WORKING_DIR)/k8s/ gsruntime-image VERSION=$(VERSION)

.PHONY: gsvineyard-image
gsvineyard:
	$(MAKE) -C $(WORKING_DIR)/k8s/ gsvineyard-image VERSION=$(VERSION)

.PHONY: graphscope-image
graphscope-image:
	$(MAKE) -C $(WORKING_DIR)/k8s/ graphscope-image VERSION=$(VERSION)

.PHONY: graphscope-store-image
graphscope-store-image:
	$(MAKE) -C $(WORKING_DIR)/k8s/ graphscope-store-image VERSION=$(VERSION)

.PHONY: push
push:
	$(MAKE) -C $(WORKING_DIR)/k8s/ push

.PHONY: install
install: gle client gae gie coordinator

.PHONY: client
client: gle
	cd $(WORKING_DIR)/python && \
	pip3 install -r requirements.txt -r requirements-dev.txt --user && \
	export WITH_LEARNING_ENGINE=$(WITH_LEARNING_ENGINE) && \
	python3 setup.py install --user --prefix=

.PHONY: coordinator
coordinator: client
	cd $(WORKING_DIR)/coordinator && \
	pip3 install -r requirements.txt -r requirements-dev.txt --user && \
	python3 setup.py install --user --prefix=

.PHONY: gae
gae:
	mkdir -p $(WORKING_DIR)/analytical_engine/build
	cd $(WORKING_DIR)/analytical_engine/build && \
	cmake -DCMAKE_INSTALL_PREFIX=$(INSTALL_PREFIX) -DNETWORKX=$(NETWORKX) -DBUILD_TESTS=${BUILD_TEST} .. && \
	make -j`nproc` && \
	sudo make install
ifneq ($(INSTALL_PREFIX), /usr/local)
	sudo rm -fr /usr/local/include/graphscope && \
	sudo ln -sf $(INSTALL_PREFIX)/bin/* /usr/local/bin/ && \
	sudo ln -sfn $(INSTALL_PREFIX)/include/graphscope /usr/local/include/graphscope && \
	sudo ln -sf ${INSTALL_PREFIX}/lib/*so* /usr/local/lib && \
	sudo ln -sf ${INSTALL_PREFIX}/lib/*dylib* /usr/local/lib && \
	if [ -d "${INSTALL_PREFIX}/lib64/cmake/graphscope-analytical" ]; then \
		sudo rm -fr /usr/local/lib64/cmake/graphscope-analytical; \
		sudo ln -sfn ${INSTALL_PREFIX}/lib64/cmake/graphscope-analytical /usr/local/lib64/cmake/graphscope-analytical; \
		sudo mkdir -p ${INSTALL_PREFIX}/lib/cmake; \
		sudo cp -r ${INSTALL_PREFIX}/lib64/cmake/* ${INSTALL_PREFIX}/lib/cmake/; \
	else \
		sudo ln -sfn ${INSTALL_PREFIX}/lib/cmake/graphscope-analytical /usr/local/lib/cmake/graphscope-analytical; \
	fi
endif

.PHONY: gie
gie:
	# coordinator/frontend/graphmanager
	cd $(WORKING_DIR)/interactive_engine && \
	mvn clean package -DskipTests -Pjava-release
	# executor
	cd $(WORKING_DIR)/interactive_engine/executor && \
	rustup component add rustfmt && \
	if [ x"release" = x"${BUILD_TYPE}" ]; then \
		cargo build --all --release; \
	else \
		cargo build --all; \
	fi
	# install
	mkdir -p $(WORKING_DIR)/.install_prefix && \
	tar -xf $(WORKING_DIR)/interactive_engine/assembly/target/maxgraph-assembly-0.0.1-SNAPSHOT.tar.gz --strip-components 1 -C $(WORKING_DIR)/.install_prefix && \
	cp $(WORKING_DIR)/interactive_engine/executor/target/$(BUILD_TYPE)/executor $(WORKING_DIR)/.install_prefix/bin/executor && \
	cp $(WORKING_DIR)/interactive_engine/executor/target/$(BUILD_TYPE)/gaia_executor $(WORKING_DIR)/.install_prefix/bin/gaia_executor && \
	sudo cp -r $(WORKING_DIR)/.install_prefix/* $(INSTALL_PREFIX) && \
	rm -fr $(WORKING_DIR)/.install_prefix

.PHONY: gle
gle:
ifeq ($(WITH_LEARNING_ENGINE), ON)
ifeq ($(OS), Linux)
	cd ${WORKING_DIR} && \
	git submodule update --init && \
	cd $(WORKING_DIR)/learning_engine/graph-learn && \
	git submodule update --init third_party/pybind11 && \
	mkdir -p cmake-build && cd cmake-build && \
	cmake -DCMAKE_INSTALL_PREFIX=$(INSTALL_PREFIX) -DWITH_VINEYARD=ON -DTESTING=${BUILD_TEST} .. && \
	make -j`nproc` && \
	sudo make install
ifneq ($(INSTALL_PREFIX), /usr/local)
	sudo ln -sf ${INSTALL_PREFIX}/lib/*so* /usr/local/lib
endif
endif
endif

.PHONY: prepare-client
prepare-client:
	cd $(WORKING_DIR)/python && \
	pip3 install -r requirements.txt --user && \
	pip3 install -r requirements-dev.txt --user && \
	python3 setup.py build_proto

.PHONY: graphscope-docs
graphscope-docs: prepare-client
	$(MAKE) -C $(WORKING_DIR)/docs/ html
