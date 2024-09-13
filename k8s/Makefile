MKFILE_PATH 			     := $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR 			     := $(dir $(MKFILE_PATH))
DOCKERFILES_DIR			   := $(WORKING_DIR)/dockerfiles
VINEYARD_VERSION_FILE  := ${WORKING_DIR}/../V6D_VERSION

ifeq ($(REGISTRY),)
    REGISTRY := registry.cn-hongkong.aliyuncs.com
endif

PLATFORM          := $(shell uname -m)
# can be: x86_64, arm64
ARCH              := $(subst x86_64,amd64,$(subst aarch64,arm64,$(PLATFORM)))


VERSION ?= latest
VINEYARD_VERSION ?= $(shell cat $(VINEYARD_VERSION_FILE))
# Use a dummy builder image (tag=ci, which is actually a busybox) to reduce time and space in CI workflow
BUILDER_VERSION ?= $(VINEYARD_VERSION)
PROFILE ?= release
CI ?= false
# Flex Coordinator
ENABLE_COORDINATOR ?= false
# Flex Interactive: controls whether optimize for host, should be enabled when building images
OPTIMIZE_FOR_HOST:= OFF
# Enable open telemetry for flex interactive
ENABLE_OPENTELMETRY ?= false

BUILD_PROGRESS  	?= auto

.PHONY: all graphscope

# High order modules
.PHONY: coordinator analytical interactive learning graphscope-store

# Base images and develop images
.PHONY:  graphscope-dev-base graphscope-dev vineyard-dev vineyard-runtime

# Low order modules
.PHONY:  analytical analytical-java
.PHONY:  interactive-frontend interactive-executor interactive-experimental
.PHONY:  learning

# Target hierarchy
interactive: interactive-frontend interactive-executor

all: coordinator analytical interactive learning
graphscope: all

manylinux2014:
	cd $(WORKING_DIR)/../ && \
	docker build \
		-t graphscope/manylinux2014:${ARCH} \
		-f $(DOCKERFILES_DIR)/manylinux2014.Dockerfile .

dev-wheel:
	cd $(WORKING_DIR)/../ && \
	docker build \
		--build-arg ARCH=$(ARCH) \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg VINEYARD_VERSION=${VINEYARD_VERSION} \
		-t graphscope/graphscope-dev:wheel-${VINEYARD_VERSION}-${ARCH} \
		-f $(DOCKERFILES_DIR)/graphscope-dev-wheel.Dockerfile .

graphscope-dev:
	cd $(WORKING_DIR)/../ && \
	docker build \
		--build-arg ARCH=$(ARCH) \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg VINEYARD_VERSION=${VINEYARD_VERSION} \
		-t graphscope/graphscope-dev:${VINEYARD_VERSION}-${ARCH} \
		-f $(DOCKERFILES_DIR)/graphscope-dev.Dockerfile .

vineyard-dev:
	cd $(WORKING_DIR)/../ && \
	docker build \
		--build-arg ARCH=$(ARCH) \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg VINEYARD_VERSION=${VINEYARD_VERSION} \
		-t graphscope/vineyard-dev:${VINEYARD_VERSION}-${ARCH} \
		-f $(DOCKERFILES_DIR)/vineyard-dev.Dockerfile .

vineyard-runtime:
	cd $(WORKING_DIR) && \
	docker build \
		--build-arg ARCH=$(ARCH) \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg VINEYARD_VERSION=$(VINEYARD_VERSION) \
		-t graphscope/vineyard-runtime:${VINEYARD_VERSION}-${ARCH} \
		-f $(DOCKERFILES_DIR)/vineyard-runtime.Dockerfile .

coordinator:
	cd $(WORKING_DIR)/.. && \
	docker build \
	    --target coordinator \
		--build-arg ARCH=$(ARCH) \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg BUILDER_VERSION=$(BUILDER_VERSION) \
		--build-arg VINEYARD_VERSION=$(VINEYARD_VERSION) \
		--build-arg CI=${CI} \
		-t graphscope/coordinator:${VERSION} \
		-f $(DOCKERFILES_DIR)/coordinator.Dockerfile .

analytical:
	cd $(WORKING_DIR)/.. && \
	docker build \
	    --target analytical \
		--build-arg ARCH=$(ARCH) \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg BUILDER_VERSION=$(BUILDER_VERSION) \
		--build-arg VINEYARD_VERSION=$(VINEYARD_VERSION) \
		--build-arg CI=${CI} \
		-t graphscope/analytical:${VERSION} \
		-f $(DOCKERFILES_DIR)/analytical.Dockerfile .

analytical-java:
	cd $(WORKING_DIR)/.. && \
	docker build \
	    --target analytical-java \
		--build-arg ARCH=$(ARCH) \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg BUILDER_VERSION=$(BUILDER_VERSION) \
		--build-arg VINEYARD_VERSION=$(VINEYARD_VERSION) \
		--build-arg CI=${CI} \
		-t graphscope/analytical-java:${VERSION} \
		-f $(DOCKERFILES_DIR)/analytical.Dockerfile .

interactive-frontend:
	cd $(WORKING_DIR)/.. && \
	docker build \
	    --target frontend \
		--build-arg ARCH=$(ARCH) \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg BUILDER_VERSION=$(BUILDER_VERSION) \
		--build-arg VINEYARD_VERSION=$(VINEYARD_VERSION) \
		--build-arg profile=$(PROFILE) \
		--build-arg CI=${CI} \
		-t graphscope/interactive-frontend:${VERSION} \
		-f $(DOCKERFILES_DIR)/interactive.Dockerfile .

interactive-executor:
	cd $(WORKING_DIR)/.. \
	&& docker build \
	    --target executor \
		--build-arg ARCH=$(ARCH) \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg BUILDER_VERSION=$(BUILDER_VERSION) \
		--build-arg VINEYARD_VERSION=$(VINEYARD_VERSION) \
		--build-arg profile=$(PROFILE) \
		--build-arg CI=${CI} \
		-t graphscope/interactive-executor:${VERSION} \
		-f $(DOCKERFILES_DIR)/interactive.Dockerfile .

# gie with experimental storage
interactive-experimental:
	cd $(WORKING_DIR)/.. && \
	docker build \
	    --target experimental \
		--build-arg ARCH=$(ARCH) \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg VINEYARD_VERSION=$(VINEYARD_VERSION) \
		-t graphscope/interactive-experimental:${VERSION} \
		-f $(DOCKERFILES_DIR)/interactive-experimental.Dockerfile .

# flex interactive 
flex-interactive:
	cd $(WORKING_DIR)/.. && \
	docker build \
		--target runtime \
		--build-arg ENABLE_COORDINATOR=${ENABLE_COORDINATOR} \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg VINEYARD_VERSION=$(VINEYARD_VERSION) \
		--build-arg PLATFORM=${PLATFORM} \
		--build-arg ENABLE_OPENTELMETRY=${ENABLE_OPENTELMETRY} \
		--build-arg ARCH=${ARCH} \
		--build-arg OPTIMIZE_FOR_HOST=${OPTIMIZE_FOR_HOST} \
		-t graphscope/interactive:${VERSION}  \
		-f ${DOCKERFILES_DIR}/flex-interactive.Dockerfile .

learning:
	cd $(WORKING_DIR)/.. && \
	docker build \
	    --target learning \
		--build-arg ARCH=$(ARCH) \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg VINEYARD_VERSION=$(VINEYARD_VERSION) \
		--build-arg CI=${CI} \
		-t graphscope/learning:${VERSION} \
		-f $(DOCKERFILES_DIR)/learning.Dockerfile .

graphlearn-torch:
	cd $(WORKING_DIR)/.. && \
	docker build \
	    --target graphlearn-torch \
		--build-arg ARCH=$(ARCH) \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg VINEYARD_VERSION=$(VINEYARD_VERSION) \
		--build-arg CI=${CI} \
		-t graphscope/graphlearn-torch:${VERSION} \
		-f $(DOCKERFILES_DIR)/graphlearn-torch.Dockerfile .

graphscope-store:
	cd $(WORKING_DIR)/.. && \
	docker build \
		--build-arg ARCH=$(ARCH) \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg VINEYARD_VERSION=$(VINEYARD_VERSION) \
		--build-arg profile=${PROFILE} \
		--build-arg ENABLE_COORDINATOR=${ENABLE_COORDINATOR} \
		-t graphscope/graphscope-store:${VERSION} \
		-f $(DOCKERFILES_DIR)/graphscope-store.Dockerfile .
