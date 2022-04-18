DOCKER_USER ?= registry-vpc.cn-hongkong.aliyuncs.com/graphscope
NAME ?= ${DOCKER_USER}/runner
TAG ?= manylinux2014
TARGETPLATFORM ?= $(shell arch)

RUNNER_VERSION ?= 2.287.1
DOCKER_VERSION ?= 20.10.12

# default list of platforms for which multiarch image is built
ifeq (${PLATFORMS}, )
	export PLATFORMS="linux/amd64,linux/arm64"
endif

build:
	docker build --build-arg TARGETPLATFORM=${TARGETPLATFORM} --build-arg RUNNER_VERSION=${RUNNER_VERSION} --build-arg DOCKER_VERSION=${DOCKER_VERSION} -t ${NAME}:${TAG} .
