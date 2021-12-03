MKFILE_PATH 			:= $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR 			:= $(dir $(MKFILE_PATH))
SHORT_SHA   			:= $(shell git rev-parse --short HEAD)

ifeq ("$(DOCKER_ORG)","")
$(warning WARNING: No docker user found, using graphscope)
DOCKER_ORG       		= graphscope
endif

.PHONY: submodule gsruntime gsvineyard graphscope push clean

ifeq ($(REGISTRY),)
    REGISTRY := registry.cn-hongkong.aliyuncs.com
endif

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S), Linux)
    XARGS_EMPTY_FLAG := --no-run-if-empty
endif

# Default null
TARGET_SERVER_PACKAGE      :=
TARGET_CLIENT_PACKAGE      :=
ifeq ($(UNAME_S), Linux)
    TARGET_CLIENT_PACKAGE := graphscope-client-manylinux2014-py3
	TARGET_SERVER_PACKAGE := graphscope-manylinux2014-py3
else
	TARGET_CLIENT_PACKAGE := graphscope-client-darwin-py3
	TARGET_SERVER_PACKAGE := graphscope-darwin-py3
endif

USE_CACHE := false
ifeq ($(USE_CACHE), false)
    CACHE_MOUNT_OPTS =
else
    CACHE_MOUNT_OPTS = -v ${HOME}/.m2:/home/graphscope/.m2 \
					   -v ${HOME}/.cache/sccache:/home/graphscope/.cache/sccache \
					   -v ${HOME}/.cargo/git:/home/graphscope/.cargo/git \
					   -v ${HOME}/.cargo/registry:/home/graphscope/.cargo/registry
endif

VERSION ?= latest
VINEYARD_VERSION ?= v0.3.11
PROFILE ?= release


BUILD_PROGRESS  	= auto

submodule:
	cd ../ && git submodule update --init

gsruntime-image:
	docker build --progress=$(BUILD_PROGRESS) \
				 -t ${REGISTRY}/$(DOCKER_ORG)/graphscope-runtime:${VERSION} \
				 --network=host \
				 -f $(WORKING_DIR)/manylinux2014.Dockerfile .

gsvineyard-image:
	docker build --progress=$(BUILD_PROGRESS) \
				 -t ${REGISTRY}/$(DOCKER_ORG)/graphscope-vineyard:${VERSION} \
				 -f $(WORKING_DIR)/gsvineyard.Dockerfile .

graphscope-image: submodule
	docker build --progress=$(BUILD_PROGRESS) \
				 --build-arg CI=${CI} \
				 -t graphscope/graphscope:${SHORT_SHA} \
				 --network=host \
				 -f $(WORKING_DIR)/graphscope.Dockerfile ..

graphscope-dev-image: submodule
	docker build --progress=$(BUILD_PROGRESS) \
				 -t graphscope/graphscope:${SHORT_SHA} \
				 --network=host \
				 -f $(WORKING_DIR)/graphscope-dev.Dockerfile ..

graphscope-store-image:
	docker pull ${REGISTRY}/$(DOCKER_ORG)/graphscope-vineyard:${VINEYARD_VERSION}
	docker build --progress=$(BUILD_PROGRESS) \
                 --build-arg profile=${PROFILE} \
                 -t graphscope/graphscope-store:${SHORT_SHA} \
                 --network=host \
                 -f $(WORKING_DIR)/graphscope-store.Dockerfile ..

graphscope-jupyter-manylinux2014-py3:
	docker run --rm -it -v $(WORKING_DIR)/..:/work \
		   ${REGISTRY}/$(DOCKER_ORG)/graphscope-runtime:${VERSION} \
		   bash -c 'pip3 install graphscope-client -U && \
					cd /work/python/jupyter/graphscope && \
					python3 setup.py bdist_wheel'

graphscope-py3-package: $(TARGET_SERVER_PACKAGE)
graphscope-client-py3-package: $(TARGET_CLIENT_PACKAGE)

graphscope-darwin-py3:
	sudo cp -rf /opt/vineyard/* /usr/local/
	# Avoid copy vineyard library with the same basename by:
	# 	1) reinstall vineyard to /usr/local and
	# 	2) rm /opt/vineyard/lib
	cd /tmp && git clone -b ${VINEYARD_VERSION} https://github.com/alibaba/libvineyard.git --depth=1 && \
		cd libvineyard && git submodule update --init && \
		mkdir -p build && cd build && \
		cmake .. -DCMAKE_INSTALL_PREFIX=/usr/local && \
		sudo make install -j`nproc` && \
		sudo rm -fr /opt/vineyard/lib
	sudo rm -fr /tmp/libvineyard /tmp/libgrape-lite
	# build graphscope
	cd $(WORKING_DIR)/../ && \
		make install && \
		python3 $(WORKING_DIR)/precompile.py
	# build and delocate wheel
	cd $(WORKING_DIR)/../coordinator && \
		export WITH_EXTRA_DATA=ON && \
		rm -fr build dist/*.whl || true && \
		sudo strip /opt/graphscope/bin/grape_engine && \
		sudo strip /opt/graphscope/bin/executor && \
		sudo strip /opt/graphscope/bin/gaia_executor && \
		export DYLD_LIBRARY_PATH=/usr/local/lib:$$DYLD_LIBRARY_PATH && \
		package_name=gs-lib python3 setup.py bdist_wheel --plat=macosx_10_9_x86_64 && \
		rm -fr build && \
		package_name=gs-include python3 setup.py bdist_wheel --plat=macosx_10_9_x86_64 && \
		rm -fr build && \
		package_name=gs-engine python3 setup.py bdist_wheel --plat=macosx_10_9_x86_64 && \
		rm -fr build && \
		package_name=gs-jython python3 setup.py bdist_wheel --plat=macosx_10_9_x86_64 && \
		rm -fr build && \
		package_name=gs-coordinator python3 setup.py bdist_wheel --plat=macosx_10_9_x86_64 && \
		rm -fr build && \
		package_name=graphscope python3 setup.py bdist_wheel --plat=macosx_10_9_x86_64 && \
		pip3 install delocate && \
		for wheel in `ls dist/*.whl`; do \
			delocate-listdeps -a -d $$wheel; \
			delocate-wheel -w dist/wheelhouse -v $$wheel && rm $$wheel; \
		done

graphscope-manylinux2014-py3:
	docker pull ${REGISTRY}/$(DOCKER_ORG)/graphscope-vineyard:${VINEYARD_VERSION}
	docker run --rm -v $(WORKING_DIR)/..:/work \
		$(CACHE_MOUNT_OPTS) \
		${REGISTRY}/$(DOCKER_ORG)/graphscope-vineyard:${VINEYARD_VERSION} \
		bash -c 'source ~/.bashrc && \
			curl -L https://github.com/mozilla/sccache/releases/download/v0.2.15/sccache-v0.2.15-x86_64-unknown-linux-musl.tar.gz | tar xz && \
			sudo mv sccache-v0.2.15-x86_64-unknown-linux-musl/sccache /usr/local/bin/ && \
			sudo chmod +x /usr/local/bin/sccache && \
			export SCCACHE_DIR=/home/graphscope/.cache/sccache && \
			export RUSTC_WRAPPER=/usr/local/bin/sccache && \
			sccache --start-server && \
			cd /work && \
			make install && \
			python3 /work/k8s/precompile.py && \
			sccache --show-stats && \
			export WITH_EXTRA_DATA=ON && \
			cd /work/coordinator && \
			rm -fr build dist/*.whl && \
			sudo strip /opt/graphscope/bin/grape_engine && \
			sudo strip /opt/graphscope/bin/executor && \
			sudo strip /opt/graphscope/bin/gaia_executor && \
			sudo strip /opt/graphscope/lib/*.so && \
			strip /tmp/gs/builtin/*/*.so && \
			package_name=gs-lib python3 setup.py bdist_wheel && \
			rm -fr build && \
			package_name=gs-include python3 setup.py bdist_wheel && \
			rm -fr build && \
			package_name=gs-engine python3 setup.py bdist_wheel && \
			rm -fr build && \
			package_name=gs-jython python3 setup.py bdist_wheel && \
			rm -fr build && \
			package_name=gs-coordinator python3 setup.py bdist_wheel && \
			rm -fr build && \
			package_name=graphscope python3 setup.py bdist_wheel && \
			cd dist && \
			for wheel in `ls ./*.whl`; do \
				auditwheel repair $$wheel --plat=manylinux2014_x86_64 && rm $$wheel; \
			done'

graphscope-client-darwin-py3:
	cd $(WORKING_DIR)/../learning_engine/graph-learn && \
		git submodule update --init third_party/pybind11 && \
		rm -rf cmake-build && \
		mkdir -p cmake-build && \
		cd cmake-build && \
		cmake -DWITH_VINEYARD=ON -DTESTING=OFF .. && \
		make graphlearn_shared -j`nproc` && \
		export DYLD_LIBRARY_PATH=$(WORKING_DIR)/../learning_engine/graph-learn/built/lib:/usr/local/lib:$$DYLD_LIBRARY_PATH && \
		cd $(WORKING_DIR)/../python && \
		pip3 install -U pip numpy==1.18.5 delocate grpcio grpcio_tools wheel && \
		rm -fr build dist/*.whl || true && \
		python3 setup.py bdist_wheel --plat=macosx_10_9_x86_64 && \
		for wheel in `ls dist/*.whl`; do \
			delocate-wheel -w dist/wheelhouse -v $$wheel && rm $$wheel; \
		done

graphscope-client-manylinux2014-py3:
	docker pull ${REGISTRY}/$(DOCKER_ORG)/graphscope-vineyard:${VINEYARD_VERSION}
	docker run --rm -v $(WORKING_DIR)/..:/work \
		${REGISTRY}/$(DOCKER_ORG)/graphscope-vineyard:${VINEYARD_VERSION} \
		bash -c 'cd /work/learning_engine/graph-learn && \
			git submodule update --init third_party/pybind11 && \
			rm -rf cmake-build && \
			mkdir -p cmake-build && \
			cd cmake-build && \
			cmake -DWITH_VINEYARD=ON .. && \
			make graphlearn_shared -j`nproc` && \
			export LD_LIBRARY_PATH=/work/learning_engine/graph-learn/built/lib:$$LD_LIBRARY_PATH && \
			for py in cp36-cp36m cp37-cp37m cp38-cp38 cp39-cp39 ; do \
				cd /work/python; \
				export PATH=/opt/python/$$py/bin:$$PATH; \
				pip3 install -U pip numpy==1.18.5 auditwheel grpcio grpcio_tools wheel ; \
				rm -rf build; \
				rm -rf dist/*.whl; \
				python3 setup.py bdist_wheel; \
				cd dist; \
				auditwheel repair ./*.whl; \
			done'

jupyter:
	docker build --progress=$(BUILD_PROGRESS) \
				 -t ${REGISTRY}/$(DOCKER_ORG)/jupyter:${VERSION} \
				 --network=host \
				 -f $(WORKING_DIR)/jupyter.Dockerfile .

push:
	docker push ${REGISTRY}/$(DOCKER_ORG)/graphscope-runtime:${VERSION}
	docker push ${REGISTRY}/$(DOCKER_ORG)/graphscope-vineyard:${VERSION}
	docker push ${REGISTRY}/$(DOCKER_ORG)/graphscope:${VERSION}

clean:
	docker ps -qa | xargs $(XARGS_EMPTY_FLAG) docker rm -f
	docker images -f "dangling=true" -q | xargs $(XARGS_EMPTY_FLAG) docker rmi -f
