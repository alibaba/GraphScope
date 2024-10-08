MKFILE_PATH            := $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR            := $(dir $(MKFILE_PATH))
VINEYARD_VERSION_FILE  := ${WORKING_DIR}/../../../V6D_VERSION

# it's ok to use registry-vpc here as building the runner image for CI
# is only for internal usage and usually happens on ECS.
ifeq ($(REGISTRY),)
    REGISTRY := registry-vpc.cn-hongkong.aliyuncs.com
endif

DOCKER_USER ?= $(REGISTRY)/graphscope
NAME ?= ${DOCKER_USER}/runner
TAG ?= manylinux2014
TARGETPLATFORM ?= $(shell arch)

RUNNER_VERSION ?= 2.318.0
DOCKER_VERSION ?= 24.0.7

VINEYARD_VERSION ?= $(shell cat $(VINEYARD_VERSION_FILE))

# default list of platforms for which multiarch image is built
ifeq (${PLATFORMS}, )
	export PLATFORMS="linux/amd64,linux/arm64"
endif

build:
	docker build \
		--build-arg REGISTRY=${REGISTRY} \
		--build-arg TARGETPLATFORM=${TARGETPLATFORM} \
		--build-arg RUNNER_VERSION=${RUNNER_VERSION} \
		--build-arg DOCKER_VERSION=${DOCKER_VERSION} \
		--build-arg VINEYARD_VERSION=$(VINEYARD_VERSION) \
		-t ${NAME}:${TAG} .
