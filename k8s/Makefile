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

VERSION ?= latest
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
				 --build-arg profile=${PROFILE} \
				 -t graphscope/graphscope:${SHORT_SHA} \
				 --network=host \
				 -f $(WORKING_DIR)/graphscope.Dockerfile ..

graphscope-store-image:
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
	sudo rm -fr /tmp/libgrape-lite /tmp/libvineyard
	sudo mkdir -p /opt/vineyard
	# install libgrape-lite
	cd /tmp && git clone https://github.com/alibaba/libgrape-lite.git --depth=1 && \
		cd libgrape-lite && mkdir build && cd build && \
		cmake .. -DCMAKE_INSTALL_PREFIX=/opt/vineyard && \
		sudo make -j`nproc` install
	# install vineyard
	cd /tmp && git clone -b v0.3.6 https://github.com/alibaba/libvineyard.git --depth=1 && \
		cd libvineyard && git submodule update --init && \
		mkdir -p build && cd build && \
		cmake .. -DCMAKE_PREFIX_PATH=/opt/vineyard \
			-DCMAKE_INSTALL_PREFIX=/opt/vineyard \
			-DBUILD_VINEYARD_SERVER=OFF \
			-DBUILD_SHARED_LIBS=ON && \
		sudo make install -j`nproc` && \
	sudo cp -rf /opt/vineyard/* /usr/local/
	# Avoid copy vineyard library with the same basename by:
	# 	1) reinstall vineyard to /usr/local and
	# 	2) rm /opt/vineyard/lib
	cd /tmp/libvineyard/build && \
		cmake .. -DCMAKE_INSTALL_PREFIX=/usr/local && \
		sudo make install -j`nproc` && \
		sudo rm -fr /opt/vineyard/lib
	sudo rm -fr /tmp/libvineyard /tmp/libgrape-lite
	# build graphscope
	cd $(WORKING_DIR)/../ && \
		make install WITH_LEARNING_ENGINE=OFF && \
		sudo install_name_tool -add_rpath /usr/local/lib /opt/graphscope/bin/executor && \
		sudo install_name_tool -add_rpath /usr/local/lib /opt/graphscope/bin/gaia_executor && \
		python3 $(WORKING_DIR)/precompile.py
	# build and delocate wheel
	cd $(WORKING_DIR)/../coordinator && \
		export WITH_EXTRA_DATA=ON && \
		rm -fr build dist/*.whl || true && \
		python3 setup.py bdist_wheel --plat=macosx_10_9_x86_64 && \
		pip3 install delocate && \
		for wheel in `ls dist/*.whl`; do \
			delocate-listdeps -a -d $$wheel; \
			delocate-wheel -w dist/wheelhouse -v $$wheel; \
		done

graphscope-manylinux2014-py3:
	docker run --rm -v $(WORKING_DIR)/..:/work \
		${REGISTRY}/$(DOCKER_ORG)/graphscope-runtime:${VERSION} \
		bash -c 'source ~/.bashrc && sudo mkdir -p /opt/vineyard && \
			cd /tmp && \
			git clone https://github.com/alibaba/libgrape-lite.git --depth=1 && \
			cd libgrape-lite && mkdir build && cd build && \
			cmake .. -DCMAKE_INSTALL_PREFIX=/opt/vineyard && \
			sudo make -j`nproc` install && \
			cd /tmp && \
			git clone -b v0.3.6 https://github.com/alibaba/libvineyard.git --depth=1 && \
			cd libvineyard && \
			git submodule update --init && \
			mkdir -p build && cd build && \
			cmake .. -DCMAKE_PREFIX_PATH=/opt/vineyard \
				-DCMAKE_INSTALL_PREFIX=/opt/vineyard \
				-DBUILD_SHARED_LIBS=ON && \
			sudo make install -j`nproc` && \
			cd /tmp/libvineyard && \
			python3 setup.py bdist_wheel && \
			cd dist && \
			auditwheel repair --plat=manylinux2014_x86_64 ./*.whl && \
			sudo mkdir -p /opt/vineyard/dist && \
			sudo cp -f wheelhouse/* /opt/vineyard/dist && \
			pip3 install wheelhouse/*.whl && \
			cd /tmp/libvineyard/modules/io && \
			python3 setup.py bdist_wheel && \
			sudo cp -f dist/* /opt/vineyard/dist && \
			pip3 install dist/* && \
			sudo cp -rf /opt/vineyard/* /usr/local/ && \
			cd /tmp && \
			sudo rm -fr /tmp/libvineyard /tmp/libgrape-lite && \
			cd /work && \
			sudo yum install -y clang-devel && \
			export LIBCLANG_PATH=$$(dirname $$(python3 -c "import clang; print(clang.__file__)"))/native && \
			make install && \
			python3 /work/k8s/precompile.py && \
			export WITH_EXTRA_DATA=ON && \
			cd /work/coordinator && \
			rm -fr build dist/*.whl && \
			sudo strip /opt/graphscope/bin/executor && \
			sudo strip /opt/graphscope/bin/gaia_executor && \
			python3 setup.py bdist_wheel && \
			cd dist && \
			auditwheel repair ./*.whl --plat=manylinux2014_x86_64'

graphscope-client-darwin-py3:
	cd $(WORKING_DIR)/../python && \
		rm -fr build dist/*.whl || true && \
		export MACOSX_DEPLOYMENT_TARGET=10.9 && \
		python3 setup.py bdist_wheel --plat=macosx_10_9_x86_64

graphscope-client-manylinux2014-py3:
	docker run --rm -v $(WORKING_DIR)/..:/work \
		${REGISTRY}/$(DOCKER_ORG)/graphscope-runtime:${VERSION} \
		bash -c 'sudo mkdir -p /opt/vineyard && \
			cd /tmp && \
			git clone https://github.com/alibaba/libgrape-lite.git --depth=1 && \
			cd libgrape-lite && mkdir build && cd build && \
			cmake .. -DCMAKE_INSTALL_PREFIX=/opt/vineyard && \
			sudo make -j`nproc` install && \
			cd /tmp && \
			git clone -b v0.3.6 https://github.com/alibaba/libvineyard.git --depth=1 && \
			cd libvineyard && \
			git submodule update --init && \
			mkdir -p build && cd build && \
			cmake .. -DCMAKE_PREFIX_PATH=/opt/vineyard \
				-DCMAKE_INSTALL_PREFIX=/opt/vineyard \
				-DBUILD_SHARED_LIBS=OFF \
				-DBUILD_VINEYARD_MIGRATION=OFF && \
			sudo make install vineyard_client_python -j`nproc` && \
			sudo cp -rf /opt/vineyard/* /usr/local/ && \
			sudo rm -fr /tmp/libvineyard /tmp/libgrape-lite && \
			cd /work/learning_engine/graph-learn && \
			git submodule update --init third_party/pybind11 && \
			rm -rf cmake-build && \
			mkdir -p cmake-build && \
			cd cmake-build && \
			cmake -DWITH_VINEYARD=ON .. && \
			make graphlearn_shared -j`nproc` && \
			export LD_LIBRARY_PATH=/work/learning_engine/graph-learn/built/lib:$$LD_LIBRARY_PATH && \
			export WITH_LEARNING_ENGINE=ON && \
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
