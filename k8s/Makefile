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
ifeq ($(UNAME_S),Linux)
    XARGS_EMPTY_FLAG := --no-run-if-empty
endif

VERSION ?= latest
PROFILE ?= release

BUILD_PROGRESS  	= auto

submodule:
	cd ../ && git submodule update --init

gsruntime:
	docker build --progress=$(BUILD_PROGRESS) \
				 -t ${REGISTRY}/$(DOCKER_ORG)/graphscope-runtime:${VERSION} \
				 -f $(WORKING_DIR)/gsruntime.Dockerfile .

gsvineyard:
	docker build --progress=$(BUILD_PROGRESS) \
			     --no-cache \
				 -t ${REGISTRY}/$(DOCKER_ORG)/graphscope-vineyard:${VERSION} \
				 -f $(WORKING_DIR)/gsvineyard.Dockerfile .

graphscope: submodule
	docker build --progress=$(BUILD_PROGRESS) \
				 --build-arg profile=${PROFILE} \
				 -t graphscope/graphscope:${SHORT_SHA} \
				 --network=host \
				 -f $(WORKING_DIR)/graphscope.Dockerfile ..

graphscope-store:
	docker build --progress=$(BUILD_PROGRESS) \
                 --build-arg profile=${PROFILE} \
                 -t graphscope/graphscope-store:${SHORT_SHA} \
                 --network=host \
                 -f $(WORKING_DIR)/graphscope-store.Dockerfile ..

graphscope-manylinux2014:
	docker build --progress=$(BUILD_PROGRESS) \
				 -t ${REGISTRY}/$(DOCKER_ORG)/graphscope-manylinux2014:${VERSION} \
				 --network=host \
				 -f $(WORKING_DIR)/manylinux2014.Dockerfile .

graphscope-jupyter-manylinux2014-py2-py3:
	docker run --rm -it -v $(WORKING_DIR)/..:/work \
		   ${REGISTRY}/$(DOCKER_ORG)/graphscope-manylinux2014:${VERSION} \
		   /usr/bin/scl enable devtoolset-8 'sh -c "export PATH=$$PATH:/opt/python/cp36-cp36m/bin && \
					pip3 install graphscope -U && \
					cd /work/python/jupyter/graphscope && \
					python3 setup.py bdist_wheel"'

graphscope-manylinux2014-py2-py3:
	docker run --rm -it -v $(WORKING_DIR)/..:/work \
			${REGISTRY}/$(DOCKER_ORG)/graphscope-manylinux2014:${VERSION} \
			bash -c 'export PATH=$$PATH:/opt/python/cp39-cp39/bin && \
				pip3 install libclang parsec && \
				mkdir -p /opt/vineyard && \
				cd /tmp && \
				git clone https://github.com/alibaba/libgrape-lite.git --depth=1 && \
				cd libgrape-lite && mkdir build && cd build && \
				cmake .. -DCMAKE_INSTALL_PREFIX=/opt/vineyard && \
				make -j install && \
				cd /tmp && \
				git clone -b v0.3.1 https://github.com/alibaba/libvineyard.git --depth=1 && \
				cd libvineyard && \
				git submodule update --init && \
				mkdir -p build && cd build && \
				cmake .. -DCMAKE_PREFIX_PATH=/opt/vineyard \
					-DCMAKE_INSTALL_PREFIX=/opt/vineyard \
					-DBUILD_SHARED_LIBS=ON \
					-DBUILD_VINEYARD_IO_OSS=ON && \
				make install vineyard_client_python -j && \
				cd /tmp/libvineyard && \
				python3 setup.py bdist_wheel && \
				cd dist && \
				auditwheel repair --plat=manylinux2014_x86_64 ./*.whl && \
				mkdir -p /opt/vineyard/dist && \
				cp -f wheelhouse/* /opt/vineyard/dist && \
				pip3 install wheelhouse/*.whl && \
				cd /tmp/libvineyard/modules/io && \
				python3 setup.py bdist_wheel && \
				cp -f dist/* /opt/vineyard/dist && \
				pip3 install dist/* && \
				cp -rf /opt/vineyard/* /usr/local/ && \
				cd /tmp && \
				rm -fr /tmp/libvineyard /tmp/libgrape-lite && \
				cd /work && \
				make install && \
				python3 /work/k8s/precompile.py && \
				export WITH_EXTRA_DATA=ON && \
				cd /work/coordinator && \
				rm -fr build dist/*.whl && \
				python3 setup.py bdist_wheel && \
				cd dist && \
				auditwheel repair ./*.whl --plat=manylinux2014_x86_64'

graphscope-client-manylinux2014-py3:
	docker run --rm -it -v $(WORKING_DIR)/..:/work \
			${REGISTRY}/$(DOCKER_ORG)/graphscope-manylinux2014:${VERSION} \
			bash -c 'export PATH=/opt/python/cp39-cp39/bin:$$PATH && \
				pip3 install libclang parsec && \
				mkdir -p /opt/vineyard && \
				cd /tmp && \
				git clone https://github.com/alibaba/libgrape-lite.git --depth=1 && \
				cd libgrape-lite && mkdir build && cd build && \
				cmake .. -DCMAKE_INSTALL_PREFIX=/opt/vineyard && \
				make -j install && \
				cd /tmp && \
				git clone -b v0.3.1 https://github.com/alibaba/libvineyard.git --depth=1 && \
				cd libvineyard && \
				git submodule update --init && \
				mkdir -p build && cd build && \
				cmake .. -DCMAKE_PREFIX_PATH=/opt/vineyard \
					-DCMAKE_INSTALL_PREFIX=/opt/vineyard \
					-DBUILD_SHARED_LIBS=OFF \
					-DBUILD_VINEYARD_IO_OSS=ON \
					-DBUILD_VINEYARD_MIGRATION=OFF && \
				make install vineyard_client_python -j && \
				cp -rf /opt/vineyard/* /usr/local/ && \
				rm -fr /tmp/libvineyard /tmp/libgrape-lite && \
				cd /work/learning_engine/graph-learn && \
				git submodule update --init third_party/pybind11 && \
				rm -rf cmake-build && \
				mkdir -p cmake-build && \
				cd cmake-build && \
				cmake -DWITH_VINEYARD=ON .. && \
				make graphlearn_shared -j && \
				export LD_LIBRARY_PATH=/work/learning_engine/graph-learn/built/lib:$$LD_LIBRARY_PATH && \
				export WITH_LEARNING_ENGINE=ON && \
				for py in cp36-cp36m cp37-cp37m cp38-cp38 cp39-cp39 cp310-cp310; do \
					cd /work/python; \
					export PATH=/opt/python/$$py/bin:$$PATH; \
					pip3 install numpy==1.18.5; \
					rm -rf build; \
					rm -rf dist/*.whl; \
					python3 setup.py bdist_wheel; \
					cd dist; \
					auditwheel repair ./*.whl; \
				done'

push:
	docker push ${REGISTRY}/$(DOCKER_ORG)/graphscope-runtime:${VERSION}
	docker push ${REGISTRY}/$(DOCKER_ORG)/graphscope-vineyard:${VERSION}
	docker push ${REGISTRY}/$(DOCKER_ORG)/graphscope:${VERSION}

clean:
	docker ps -qa | xargs $(XARGS_EMPTY_FLAG) docker rm -f
	docker images -f "dangling=true" -q | xargs $(XARGS_EMPTY_FLAG) docker rmi -f
