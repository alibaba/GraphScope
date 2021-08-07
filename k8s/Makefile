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
manager:
	docker build --target manager \
				 -t graphscope/maxgraph_standalone_manager:${SHORT_SHA} \
				 -f $(WORKING_DIR)/manager.Dockerfile ..

graphscope-store:
	docker build --progress=$(BUILD_PROGRESS) \
                 --build-arg profile=${PROFILE} \
                 -t graphscope/graphscope-store:${SHORT_SHA} \
                 --network=host \
                 -f $(WORKING_DIR)/graphscope-store.Dockerfile ..

graphscope-manylinux2010:
	docker build --progress=$(BUILD_PROGRESS) \
				 -t ${REGISTRY}/$(DOCKER_ORG)/graphscope-manylinux2010:${VERSION} \
				 --network=host \
				 -f $(WORKING_DIR)/manylinux2010.Dockerfile .

graphscope-jupyter-manylinux2010-py2-py3:
	docker run --rm -it -v $(WORKING_DIR)/..:/work \
		   ${REGISTRY}/$(DOCKER_ORG)/graphscope-manylinux2010:${VERSION} \
		   /usr/bin/scl enable devtoolset-8 'sh -c "export PATH=$$PATH:/opt/python/cp36-cp36m/bin && \
					pip3 install graphscope -U && \
					cd /work/python/jupyter/graphscope && \
					python3 setup.py bdist_wheel"'

graphscope-manylinux2010-py36:
	docker run --rm -it -v $(WORKING_DIR)/..:/work \
		   ${REGISTRY}/$(DOCKER_ORG)/graphscope-manylinux2010:${VERSION} \
		   /usr/bin/scl enable devtoolset-8 'sh -c "export PATH=$$PATH:/opt/python/cp36-cp36m/bin && \
          cd /work/learning_engine/graph-learn && \
					git submodule update --init third_party/pybind11 && \
				  rm -rf cmake-build-py36 && \
				  mkdir -p cmake-build-py36 && \
				  cd cmake-build-py36 && \
				  cmake .. && \
				  make graphlearn_shared -j && \
				  export LD_LIBRARY_PATH=/work/learning_engine/graph-learn/built/lib:$$LD_LIBRARY_PATH && \
				  export WITH_LEARNING_ENGINE=ON && \
				  cd /work/python && \
				  rm -rf build && \
				  rm -rf dist/*.whl && \
				  python3 setup.py bdist_wheel && \
				  cd dist && \
				  auditwheel repair ./*.whl"'

graphscope-manylinux2010-py37:
	docker run --rm -it -v $(WORKING_DIR)/..:/work \
		   ${REGISTRY}/$(DOCKER_ORG)/graphscope-manylinux2010:${VERSION} \
		   /usr/bin/scl enable devtoolset-8 'sh -c "export PATH=$$PATH:/opt/python/cp37-cp37m/bin && \
          cd /work/learning_engine/graph-learn && \
					git submodule update --init third_party/pybind11 && \
				  rm -rf cmake-build-py36 && \
				  mkdir -p cmake-build-py36 && \
				  cd cmake-build-py36 && \
				  cmake .. && \
				  make graphlearn_shared -j && \
				  export LD_LIBRARY_PATH=/work/learning_engine/graph-learn/built/lib:$$LD_LIBRARY_PATH && \
				  export WITH_LEARNING_ENGINE=ON && \
				  cd /work/python && \
				  rm -rf build && \
				  rm -rf dist/*.whl && \
				  python3 setup.py bdist_wheel && \
				  cd dist && \
				  auditwheel repair ./*.whl"'

graphscope-manylinux2010-py38:
	docker run --rm -it -v $(WORKING_DIR)/..:/work \
		   ${REGISTRY}/$(DOCKER_ORG)/graphscope-manylinux2010:${VERSION} \
		   /usr/bin/scl enable devtoolset-8 'sh -c "export PATH=$$PATH:/opt/python/cp38-cp38/bin && \
          cd /work/learning_engine/graph-learn && \
					git submodule update --init third_party/pybind11 && \
				  rm -rf cmake-build-py36 && \
				  mkdir -p cmake-build-py36 && \
				  cd cmake-build-py36 && \
				  cmake .. && \
				  make graphlearn_shared -j && \
				  export LD_LIBRARY_PATH=/work/learning_engine/graph-learn/built/lib:$$LD_LIBRARY_PATH && \
				  export WITH_LEARNING_ENGINE=ON && \
				  cd /work/python && \
				  rm -rf build && \
				  rm -rf dist/*.whl && \
				  python3 setup.py bdist_wheel && \
				  cd dist && \
				  auditwheel repair ./*.whl"'

graphscope-manylinux2010-py39:
	docker run --rm -it -v $(WORKING_DIR)/..:/work \
		   ${REGISTRY}/$(DOCKER_ORG)/graphscope-manylinux2010:${VERSION} \
		   /usr/bin/scl enable devtoolset-8 'sh -c "export PATH=$$PATH:/opt/python/cp39-cp39/bin && \
          cd /work/learning_engine/graph-learn && \
					git submodule update --init third_party/pybind11 && \
				  rm -rf cmake-build-py36 && \
				  mkdir -p cmake-build-py36 && \
				  cd cmake-build-py36 && \
				  cmake .. && \
				  make graphlearn_shared -j && \
				  export LD_LIBRARY_PATH=/work/learning_engine/graph-learn/built/lib:$$LD_LIBRARY_PATH && \
				  export WITH_LEARNING_ENGINE=ON && \
				  cd /work/python && \
				  rm -rf build && \
				  rm -rf dist/*.whl && \
				  python3 setup.py bdist_wheel && \
				  cd dist && \
				  auditwheel repair ./*.whl"'

push:
	docker push ${REGISTRY}/$(DOCKER_ORG)/graphscope-runtime:${VERSION}
	docker push ${REGISTRY}/$(DOCKER_ORG)/graphscope-vineyard:${VERSION}
	docker push ${REGISTRY}/$(DOCKER_ORG)/graphscope:${VERSION}

clean:
	docker ps -qa | xargs $(XARGS_EMPTY_FLAG) docker rm -f
	docker images -f "dangling=true" -q | xargs $(XARGS_EMPTY_FLAG) docker rmi -f
