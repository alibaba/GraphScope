MKFILE_PATH 			:= $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR 			:= $(dir $(MKFILE_PATH))
NUM_PROC                := $( $(command -v nproc &> /dev/null) && echo $(nproc) || echo $(sysctl -n hw.physicalcpu) )

VERSION                     ?= 0.1.0
INSTALL_PREFIX              ?= /opt/graphscope

# GAE build options
NETWORKX                    ?= OFF

# client build options
WITH_LEARNING_ENGINE        ?= ON

# testing build option
BUILD_TEST                  ?= OFF

.PHONY: all
all: graphscope

.PHONY: gsruntime
gsruntime:
	$(MAKE) -C $(WORKING_DIR)/k8s/ gsruntime VERSION=$(VERSION)

.PHONY: gsvineyard
gsvineyard:
	$(MAKE) -C $(WORKING_DIR)/k8s/ gsvineyard VERSION=$(VERSION)

.PHONY: graphscope
graphscope:
	$(MAKE) -C $(WORKING_DIR)/k8s/ graphscope VERSION=$(VERSION)

.PHONY: interactive_manager
interactive_manager:
	$(MAKE) -C $(WORKING_DIR)/k8s/ manager VERSION=$(VERSION)

.PHONY: graphscope-store
graphscope-store:
	$(MAKE) -C $(WORKING_DIR)/k8s/ graphscope-store VERSION=$(VERSION)

.PHONY: push
push:
	$(MAKE) -C $(WORKING_DIR)/k8s/ push

.PHONY: install
install: gle client coordinator gae gie

.PHONY: client
client: gle
	cd $(WORKING_DIR)/python && \
	pip3 install -r requirements.txt -r requirements-dev.txt --user && \
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
	make -j$(NUM_PROC) && \
	sudo make install
ifneq ($(INSTALL_PREFIX), /usr/local)
	sudo ln -sf $(INSTALL_PREFIX)/bin/* /usr/local/bin/ && \
	sudo ln -sfn $(INSTALL_PREFIX)/include/graphscope /usr/local/include/graphscope && \
	sudo ln -sf ${INSTALL_PREFIX}/lib/*so* /usr/local/lib && \
	sudo ln -sf ${INSTALL_PREFIX}/lib/*dylib* /usr/local/lib && \
	if [ -d "${INSTALL_PREFIX}/lib64/cmake/graphscope-analytical" ]; then \
		sudo ln -sfn ${INSTALL_PREFIX}/lib64/cmake/graphscope-analytical /usr/local/lib64/cmake/graphscope-analytical; \
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
	cd $(WORKING_DIR)/interactive_engine/src/executor && \
	rustup component add rustfmt && \
	cargo build --all
	# install
	mkdir -p $(WORKING_DIR)/.install_prefix && \
	tar -xf $(WORKING_DIR)/interactive_engine/src/assembly/target/maxgraph-assembly-0.0.1-SNAPSHOT.tar.gz --strip-components 1 -C $(WORKING_DIR)/.install_prefix && \
	cp $(WORKING_DIR)/interactive_engine/src/executor/target/debug/executor $(WORKING_DIR)/.install_prefix/bin/executor && \
	cp $(WORKING_DIR)/interactive_engine/src/executor/target/debug/gaia_executor $(WORKING_DIR)/.install_prefix/bin/gaia_executor && \
	sudo cp -r $(WORKING_DIR)/.install_prefix/* $(INSTALL_PREFIX) && \
	rm -fr $(WORKING_DIR)/.install_prefix

.PHONY: gle
gle:
ifeq ($(WITH_LEARNING_ENGINE), ON)
	cd ${WORKING_DIR} && \
	git submodule update --init && \
	cd $(WORKING_DIR)/learning_engine/graph-learn && \
	git submodule update --init third_party/pybind11 && \
	mkdir -p cmake-build && cd cmake-build && \
	cmake -DCMAKE_INSTALL_PREFIX=$(INSTALL_PREFIX) -DWITH_VINEYARD=ON -DTESTING=${BUILD_TEST} .. && \
	make -j$(NUM_PROC) && \
	sudo make install
ifneq ($(INSTALL_PREFIX), /usr/local)
	sudo ln -sf ${INSTALL_PREFIX}/lib/*so* /usr/local/lib
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
