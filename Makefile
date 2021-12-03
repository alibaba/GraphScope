
MKFILE_PATH 			:= $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR 			:= $(dir $(MKFILE_PATH))

VERSION                     ?= 0.1.0
INSTALL_PREFIX              ?= /opt/graphscope

BUILD_TYPE                  ?= release

# GAE build options
NETWORKX                    ?= ON

# testing build option
BUILD_TEST                  ?= OFF

# build java sdk option
ENABLE_JAVA_SDK             ?= OFF

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

# bulld graphscope image from source code without wheel package
.PHONY: graphscope-dev-image
graphscope-dev-image:
	$(MAKE) -C $(WORKING_DIR)/k8s/ graphscope-dev-image VERSION=$(VERSION)

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
	python3 setup.py build_ext --inplace --user

.PHONY: coordinator
coordinator:
	cd $(WORKING_DIR)/coordinator && \
	pip3 install -r requirements.txt -r requirements-dev.txt --user && \
	python3 setup.py build_builtin
	if [ ! -d "/var/log/graphscope" ]; then \
		sudo mkdir /var/log/graphscope; \
	fi
	sudo chown -R `id -u`:`id -g` /var/log/graphscope

.PHONY: gae
gae:
	mkdir -p $(WORKING_DIR)/analytical_engine/build
	cd $(WORKING_DIR)/analytical_engine/build && \
	cmake -DCMAKE_INSTALL_PREFIX=$(INSTALL_PREFIX) -DNETWORKX=$(NETWORKX) -DBUILD_TESTS=${BUILD_TEST} -DENABLE_JAVA_SDK=${ENABLE_JAVA_SDK} .. && \
	make -j`nproc` && \
	sudo make install && \
	sudo cp -r $(WORKING_DIR)/k8s/kube_ssh $(INSTALL_PREFIX)/bin/
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
ifeq (${ENABLE_JAVA_SDK}, ON)
	cd $(WORKING_DIR)/analytical_engine/java && \
	mvn clean install -DskipTests --quiet && \
	sudo cp ${WORKING_DIR}/analytical_engine/java/grape-runtime/target/native/libgrape-jni.so ${INSTALL_PREFIX}/lib/ && \
	sudo cp ${WORKING_DIR}/analytical_engine/java/grape-runtime/target/grape-runtime-0.1-shaded.jar ${INSTALL_PREFIX}/lib/ && \
	sudo mkdir -p ${INSTALL_PREFIX}/conf/ && \
	sudo cp ${WORKING_DIR}/analytical_engine/java/compile-commands.txt ${INSTALL_PREFIX}/conf/
	sudo cp ${WORKING_DIR}/analytical_engine/java/grape_jvm_opts ${INSTALL_PREFIX}/conf/
endif

.PHONY: gie
gie:
	# coordinator/frontend/graphmanager
	cd $(WORKING_DIR)/interactive_engine && \
	mvn clean package -DskipTests -Pjava-release --quiet
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
	cd ${WORKING_DIR} && \
	git submodule update --init && \
	cd $(WORKING_DIR)/learning_engine/graph-learn && \
	git submodule update --init third_party/pybind11 && \
	mkdir -p cmake-build && cd cmake-build && \
	cmake -DCMAKE_INSTALL_PREFIX=$(INSTALL_PREFIX) -DWITH_VINEYARD=ON -DTESTING=${BUILD_TEST} .. && \
	make -j`nproc` && \
	sudo make install
ifneq ($(INSTALL_PREFIX), /usr/local)
	sudo ln -sf ${INSTALL_PREFIX}/lib/*so* /usr/local/lib && \
	sudo ln -sf ${INSTALL_PREFIX}/lib/*dylib* /usr/local/lib
endif

# wheels
.PHONY: graphscope-py3-package
graphscope-py3-package:
	$(MAKE) -C $(WORKING_DIR)/k8s/ graphscope-py3-package

.PHONY: graphscope-client-py3-package
graphscope-client-py3-package:
	 $(MAKE) -C $(WORKING_DIR)/k8s/ graphscope-client-py3-package

.PHONY: prepare-client
prepare-client:
	cd $(WORKING_DIR)/python && \
	pip3 install -r requirements.txt --user && \
	pip3 install -r requirements-dev.txt --user && \
	python3 setup.py build_proto

.PHONY: graphscope-docs
graphscope-docs: prepare-client
	$(MAKE) -C $(WORKING_DIR)/docs/ html

.PHONY: test
test: unittest minitest k8stest

.PHONY: unittest
unittest:
	cd $(WORKING_DIR)/python && \
	python3 -m pytest -s -v ./graphscope/tests/unittest

.PHONY: minitest
minitest:
	cd $(WORKING_DIR)/python && \
	pip3 install tensorflow==2.5.2 && \
	python3 -m pytest -s -v ./graphscope/tests/minitest

.PHONY: k8stest
k8stest:
	cd $(WORKING_DIR)/python && \
	pip3 install tensorflow==2.5.2 && \
	python3 -m pytest -s -v ./graphscope/tests/kubernetes
