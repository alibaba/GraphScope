MKFILE_PATH            := $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR            := $(dir $(MKFILE_PATH))
SHORT_SHA              := $(shell git rev-parse --short HEAD)
VINEYARD_VERSION_FILE  := ${WORKING_DIR}/../../V6D_VERSION

.PHONY: graphscope-jupyter-manylinux2014-py3 graphscope-py3-package graphscope-client-py3-package
.PHONY: graphscope-darwin-py3 graphscope-client-darwin-py3
.PHONY: graphscope-manylinux2014-py3-nodocker graphscope-manylinux2014-py3
.PHONY: graphscope-client-manylinux2014-py3-nodocker graphscope-client-manylinux2014-py3

ifeq ($(REGISTRY),)
    REGISTRY := registry.cn-hongkong.aliyuncs.com
endif


UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S), Linux)
	XARGS_EMPTY_FLAG := --no-run-if-empty
endif

PLATFORM          := $(shell uname -m)
# can be: x86_64, arm64
ARCH              := $(subst x86_64,amd64,$(subst aarch64,arm64,$(PLATFORM)))
MACOS_WHEEL_VERSION := 12_0

# docker build arguments
BUILD_PROGRESS		= auto

# pip installation arguments
PIP_ARGS 			= --timeout=1000 --no-cache-dir

# Default null
TARGET_SERVER_PACKAGE      :=
TARGET_CLIENT_PACKAGE      :=
ifeq ($(UNAME_S), Linux)
	TARGET_SERVER_PACKAGE := graphscope-manylinux2014-py3
	TARGET_CLIENT_PACKAGE := graphscope-client-manylinux2014-py3
else
	TARGET_SERVER_PACKAGE := graphscope-darwin-py3
	TARGET_CLIENT_PACKAGE := graphscope-client-darwin-py3
endif

GRAPHSCOPE_HOME ?= /usr/local
INSTALL_PREFIX ?= /opt/graphscope

VERSION ?= latest
VINEYARD_VERSION ?= $(shell cat $(VINEYARD_VERSION_FILE))
PROFILE ?= release
CI ?= false

graphscope:
	cd $(WORKING_DIR)/../.. && \
	git submodule update --init && \
	docker build --build-arg CI=${CI} \
				 -t graphscope/graphscope:${SHORT_SHA} \
				 -f $(WORKING_DIR)/graphscope.Dockerfile .

graphscope-jupyter-manylinux2014-py3:
	docker run --rm -it -v $(WORKING_DIR)/../..:/work \
		   ${REGISTRY}/graphscope/graphscope-dev:wheel-${VINEYARD_VERSION}-${ARCH} \
		   bash -c 'pip3 install graphscope-client -U && \
					cd /work/python/jupyter/graphscope && \
					python3 setup.py bdist_wheel'

graphscope-py3-package: $(TARGET_SERVER_PACKAGE)
graphscope-client-py3-package: $(TARGET_CLIENT_PACKAGE)


MAKE_COMMAND=make INSTALL_PREFIX=${INSTALL_PREFIX}
MAKE_INSTALL_COMMAND=make install INSTALL_PREFIX=${INSTALL_PREFIX}

graphscope-darwin-py3:
	# build graphscope
	cd $(WORKING_DIR)/../.. && \
		python3 -m pip install ${PIP_ARGS} --upgrade setuptools && \
		export INSTALL_PREFIX=/opt/graphscope && \
		for _ in {1..5}; do if $(MAKE_COMMAND); then succeed=1; break; else succeed=0; fi; done && \
		if [[ $$succeed == 1 ]]; then echo "Build succeed"; else echo "Build failed"; exit 1; fi && \
		for _ in {1..5}; do if $(MAKE_INSTALL_COMMAND); then succeed=1; break; else succeed=0; fi; done && \
		if [[ $$succeed == 1 ]]; then echo "Install succeed"; else echo "Install failed"; exit 1; fi && \
		(sudo cp -rs ${INSTALL_PREFIX}/* /usr/local/ || true) && \
		python3 $(WORKING_DIR)/../utils/precompile.py --graph --app
	# build and delocate wheel
	cd $(WORKING_DIR)/../../coordinator && \
		export WITH_EXTRA_DATA=ON && \
		if [[ "${PLATFORM}" == "arm64" ]]; then export GRAPHSCOPE_HOME=/opt/homebrew; else export GRAPHSCOPE_HOME=/usr/local; fi && \
		rm -rf build dist/*.whl || true && \
		sudo strip -s $(WORKING_DIR)/../../analytical_engine/exported_symbols_osx.lds ${INSTALL_PREFIX}/bin/grape_engine || true && \
		sudo strip ${INSTALL_PREFIX}/bin/gaia_executor && \
		export DYLD_LIBRARY_PATH=/usr/local/lib:$$DYLD_LIBRARY_PATH && \
		install_name_tool -add_rpath /usr/local/lib ${INSTALL_PREFIX}/bin/gaia_executor && \
		package_name=gs-include python3 setup.py bdist_wheel --plat=macosx_${MACOS_WHEEL_VERSION}_${PLATFORM} && \
		rm -rf build && \
		package_name=gs-apps python3 setup.py bdist_wheel --plat=macosx_${MACOS_WHEEL_VERSION}_${PLATFORM} && \
		rm -rf build && \
		package_name=gs-engine python3 setup.py bdist_wheel --plat=macosx_${MACOS_WHEEL_VERSION}_${PLATFORM} && \
		rm -rf build && \
		package_name=gs-coordinator python3 setup.py bdist_wheel --plat=macosx_${MACOS_WHEEL_VERSION}_${PLATFORM} && \
		rm -rf build && \
		package_name=graphscope python3 setup.py bdist_wheel --plat=macosx_${MACOS_WHEEL_VERSION}_${PLATFORM} && \
		pip3 install delocate==0.10.7 && \
		for wheel in `ls dist/*.whl`; do \
			delocate-listdeps -a -d $$wheel; \
			delocate-wheel -w dist/wheelhouse -v $$wheel && rm $$wheel; \
		done

graphscope-manylinux2014-py3-nodocker:
	# TODO(siyuan) Remove after newer wheel image are released
	sudo sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-* && \
	sudo sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-* && \
	sudo yum install java-11-openjdk-devel -y && \
	sudo yum remove java-1.8.0-openjdk-devel java-1.8.0-openjdk java-1.8.0-openjdk-headless -y && \
	cd $(WORKING_DIR)/../.. && \
		if [[ "${PLATFORM}" == "aarch64" ]]; then \
			export AUDITWHEEL_PLAT=manylinux2014_${PLATFORM}; \
			python3 -m pip install ${PIP_ARGS} grpcio==1.49.1 --no-binary grpcio; \
			python3 -m pip install ${PIP_ARGS} grpcio-tools==1.49.1 --no-binary grpcio-tools; \
		fi && \
		sudo rm -rf ./learning_engine/graph-learn/graphlearn/built && \
		for _ in {1..5}; do if $(MAKE_COMMAND); then succeed=1; break; else succeed=0; fi; done && \
		if [[ $$succeed == 1 ]]; then echo "Build succeed"; else echo "Build failed"; exit 1; fi && \
		for _ in {1..5}; do if $(MAKE_INSTALL_COMMAND); then succeed=1; break; else succeed=0; fi; done && \
		if [[ $$succeed == 1 ]]; then echo "Install succeed"; else echo "Install failed"; exit 1; fi && \
		if [ ${INSTALL_PREFIX} != ${GRAPHSCOPE_HOME} ]; then \
			sudo cp -rsn ${INSTALL_PREFIX}/* ${GRAPHSCOPE_HOME}/; \
		fi && \
		python3 $(WORKING_DIR)/../utils/precompile.py --graph --app && \
	cd $(WORKING_DIR)/../../coordinator && \
		export WITH_EXTRA_DATA=ON && \
		export GRAPHSCOPE_HOME=${GRAPHSCOPE_HOME} && \
		export INSTALL_PREFIX=${INSTALL_PREFIX} && \
		rm -rf build dist/*.whl && \
		sudo strip ${INSTALL_PREFIX}/bin/grape_engine && \
		sudo strip ${INSTALL_PREFIX}/bin/gaia_executor && \
		sudo strip ${INSTALL_PREFIX}/lib/*.so && \
		(strip /tmp/gs/builtin/*/*.so || true) && \
		package_name=gs-include python3 setup.py bdist_wheel && \
		rm -rf build && \
		package_name=gs-apps python3 setup.py bdist_wheel && \
		rm -rf build && \
		package_name=gs-engine python3 setup.py bdist_wheel && \
		rm -rf build && \
		package_name=gs-coordinator python3 setup.py bdist_wheel && \
		rm -rf build && \
		package_name=graphscope python3 setup.py bdist_wheel && \
		cd dist && \
		for wheel in `ls ./*.whl`; do \
			(auditwheel repair $$wheel --plat=manylinux2014_${PLATFORM} && rm $$wheel) || true; \
		done

graphscope-manylinux2014-py3:
	docker pull ${REGISTRY}/graphscope/graphscope-dev:wheel-${VINEYARD_VERSION}-${ARCH}
	docker run --rm -v $(WORKING_DIR)/../..:/work \
		${REGISTRY}/graphscope/graphscope-dev:wheel-${VINEYARD_VERSION}-${ARCH} \
		bash -c '. ~/.graphscope_env && \
			sudo chown -R graphscope /work && \
			cd /work/k8s/internal && \
			make graphscope-manylinux2014-py3-nodocker GRAPHSCOPE_HOME=${GRAPHSCOPE_HOME} INSTALL_PREFIX=${INSTALL_PREFIX}'

graphscope-client-manylinux2014-py3-nodocker:
	set -euxo pipefail && \
	cd $(WORKING_DIR)/../../python && \
		python3 -m pip install ${PIP_ARGS} "networkx<=3.0" --user && \
		python3 -m pip install ${PIP_ARGS} -r requirements.txt -r requirements-dev.txt --user && \
	cd $(WORKING_DIR)/../../learning_engine/graph-learn && \
	git submodule update --init third_party/pybind11 && \
	cd graphlearn && \
	rm -rf cmake-build && \
	mkdir -p cmake-build && \
	cd cmake-build && \
	export GRAPHSCOPE_HOME=${GRAPHSCOPE_HOME} && \
	cmake -DKNN=OFF -DWITH_VINEYARD=ON -DTESTING=OFF .. && \
	make graphlearn_shared -j`nproc` && \
	export LD_LIBRARY_PATH=$(WORKING_DIR)/../../learning_engine/graph-learn/graphlearn/built/lib:$$LD_LIBRARY_PATH && \
	if [[ "${PLATFORM}" == "aarch64" ]]; then export AUDITWHEEL_PLAT=manylinux2014_${PLATFORM}; fi && \
	for py in cp37-cp37m cp38-cp38 cp39-cp39 cp310-cp310 cp311-cp311; do \
		cd $(WORKING_DIR)/../../python; \
		export PATH=/opt/python/$$py/bin:$$PATH; \
		python3 -m pip install ${PIP_ARGS} -U pip; \
		python3 -m pip install ${PIP_ARGS} "torch<=2.2.1" --index-url https://download.pytorch.org/whl/cpu --user; \
		if [[ "$$py" == "cp311-cp311" ]]; then \
			if [[ "${PLATFORM}" == "aarch64" ]]; then \
				python3 -m pip install ${PIP_ARGS} grpcio==1.49.1 --no-binary grpcio; \
				python3 -m pip install ${PIP_ARGS} grpcio-tools==1.49.1 --no-binary grpcio-tools; \
			else \
				python3 -m pip install ${PIP_ARGS} "grpcio" "grpcio-tools"; \
			fi; \
			python3 -m pip install ${PIP_ARGS} mypy-protobuf "numpy==1.23.2" "pandas" wheel "auditwheel==5.2.0"; \
		elif [[ "$$py" == "cp310-cp310" ]]; then \
			if [[ "${PLATFORM}" == "aarch64" ]]; then \
				python3 -m pip install ${PIP_ARGS} grpcio==1.49.1 --no-binary grpcio; \
				python3 -m pip install ${PIP_ARGS} grpcio-tools==1.49.1 --no-binary grpcio-tools; \
			else \
				python3 -m pip install ${PIP_ARGS} "grpcio>=1.49" "grpcio-tools>=1.49"; \
			fi; \
			python3 -m pip install ${PIP_ARGS} mypy-protobuf "numpy==1.21.2" "pandas" wheel "auditwheel==5.2.0"; \
		elif [[ "$$py" == "cp39-cp39" ]]; then \
			if [[ "${PLATFORM}" == "aarch64" ]]; then \
				python3 -m pip install ${PIP_ARGS} grpcio==1.49.1 --no-binary grpcio; \
				python3 -m pip install ${PIP_ARGS} grpcio-tools==1.49.1 --no-binary grpcio-tools; \
			else \
				python3 -m pip install ${PIP_ARGS} "grpcio>=1.49" "grpcio-tools>=1.49"; \
			fi; \
			python3 -m pip install ${PIP_ARGS} mypy-protobuf "numpy==1.19.3" "pandas" wheel "auditwheel==5.2.0"; \
		else \
			if [[ "${PLATFORM}" == "aarch64" ]]; then \
				python3 -m pip install ${PIP_ARGS} grpcio==1.49.1 --no-binary grpcio; \
				python3 -m pip install ${PIP_ARGS} grpcio-tools==1.49.1 --no-binary grpcio-tools; \
			else \
				python3 -m pip install ${PIP_ARGS} "grpcio>=1.49" "grpcio-tools>=1.49"; \
			fi; \
			python3 -m pip install ${PIP_ARGS} mypy-protobuf "numpy" "pandas" wheel "auditwheel==5.2.0"; \
		fi; \
		sudo rm -rf build; \
		sudo rm -rf dist/*.whl; \
		python3 setup.py build_gltorch_ext; \
		python3 setup.py bdist_wheel; \
		cd dist; \
		auditwheel repair ./*.whl --plat=manylinux2014_${PLATFORM} --exclude libtorch_cpu.so --exclude libc10.so --exclude libtorch_python.so --exclude libtorch.so; \
	done

graphscope-client-darwin-py3:
	cd $(WORKING_DIR)/../../python && \
		python3 -m pip install ${PIP_ARGS} "torch<=2.2.1" "networkx<=3.0" --index-url https://download.pytorch.org/whl/cpu --user && \
		python3 -m pip install ${PIP_ARGS} -r requirements.txt -r requirements-dev.txt --user && \
	cd $(WORKING_DIR)/../../learning_engine/graph-learn && \
		(git submodule update --init third_party/pybind11 || true) && \
		export GRAPHSCOPE_HOME=${GRAPHSCOPE_HOME} && \
		cd graphlearn && \
		sudo rm -rf cmake-build && \
		sudo rm -rf src/generated && \
		mkdir -p cmake-build && \
		cd cmake-build && \
		cmake -DKNN=OFF -DWITH_VINEYARD=ON -DTESTING=OFF .. && \
		make graphlearn_shared -j`nproc` && \
		export DYLD_LIBRARY_PATH=$(WORKING_DIR)/../../learning_engine/graph-learn/graphlearn/built/lib:/usr/local/lib:${GRAPHSCOPE_HOME}/lib:$$DYLD_LIBRARY_PATH && \
		export DYLD_LIBRARY_PATH=$$(dirname $$(python3 -c "import torch; print(torch.__file__)"))/lib:$$DYLD_LIBRARY_PATH && \
		cd $(WORKING_DIR)/../../python && \
		py=$$(python3 -V 2>&1 | awk '{print $$2}' | awk -F '.' '{print $$1$$2}') && \
		pip3 install ${PIP_ARGS} -U pip && \
		if [[ "$$py" == "311" ]]; then \
			pip3 install ${PIP_ARGS} mypy-protobuf "numpy==1.23.2" "pandas" "grpcio>=1.49" "grpcio-tools>=1.49" delocate wheel; \
		elif [[ "$$py" == "310" ]]; then \
			pip3 install ${PIP_ARGS} mypy-protobuf "numpy==1.21.2" "pandas" "grpcio>=1.49" "grpcio-tools>=1.49" delocate wheel; \
		elif [[ "$$py" == "39" ]]; then \
			if [[ "${PLATFORM}" == "arm64" ]]; then \
				pip3 install ${PIP_ARGS} mypy-protobuf "numpy==1.21.0" "pandas" "grpcio>=1.49" "grpcio-tools>=1.49" delocate wheel; \
			else \
				pip3 install ${PIP_ARGS} mypy-protobuf "numpy==1.19.3" "pandas" "grpcio>=1.49" "grpcio-tools>=1.49" delocate wheel; \
			fi; \
		else \
			if [[ "${PLATFORM}" == "arm64" ]]; then \
				pip3 install ${PIP_ARGS} mypy-protobuf "numpy==1.21.0" "pandas" "grpcio>=1.49" "grpcio-tools>=1.49" delocate wheel; \
			else \
				pip3 install ${PIP_ARGS} mypy-protobuf "numpy==1.18.5" "pandas" "grpcio>=1.49" "grpcio-tools>=1.49" delocate wheel; \
			fi; \
		fi; \
		rm -rf build dist/*.whl || true && \
		python3 setup.py build_gltorch_ext --plat=macosx_${MACOS_WHEEL_VERSION}_${PLATFORM} && \
		python3 setup.py bdist_wheel --plat=macosx_${MACOS_WHEEL_VERSION}_${PLATFORM} && \
		for wheel in `ls dist/*.whl`; do \
			delocate-wheel -e libtorch_cpu.dylib -e libc10.dylib -e libtorch_python.dylib -e libtorch.dylib -w dist/wheelhouse -v $$wheel && rm $$wheel; \
		done

graphscope-client-manylinux2014-py3:
	docker pull ${REGISTRY}/graphscope/graphscope-dev:wheel-${VINEYARD_VERSION}-${ARCH}
	docker run --rm -v $(WORKING_DIR)/../..:/work \
		${REGISTRY}/graphscope/graphscope-dev:wheel-${VINEYARD_VERSION}-${ARCH} \
		bash -c '. ~/.graphscope_env && \
		    sudo chown -R graphscope /work && \
		    cd /work/k8s/internal && \
			make graphscope-client-manylinux2014-py3-nodocker GRAPHSCOPE_HOME=${GRAPHSCOPE_HOME}'
