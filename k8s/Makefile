MKFILE_PATH 			:= $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR 			:= $(dir $(MKFILE_PATH))
DOCKERFILES_DIR			:= $(WORKING_DIR)/dockerfiles
SHORT_SHA   			:= $(shell git rev-parse --short HEAD)

ifeq ($(REGISTRY),)
    REGISTRY := registry.cn-hongkong.aliyuncs.com
endif

VERSION ?= latest
VINEYARD_VERSION ?= v0.11.0
PROFILE ?= release
CI ?= false


BUILD_PROGRESS  	= auto

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

graphscope-dev-base:
	cd $(WORKING_DIR) && \
	docker build \
		--build-arg REGISTRY=$(REGISTRY) \
		-t graphscope/graphscope-dev-base:${VERSION} \
		-f $(DOCKERFILES_DIR)/graphscope-dev-base.Dockerfile .
	docker tag graphscope/graphscope-dev-base:${VERSION} $(REGISTRY)/graphscope/graphscope-dev-base:${VERSION}

graphscope-dev:
	cd $(WORKING_DIR) && \
	docker build \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg BASE_VERSION=latest \
		--build-arg VINEYARD_VERSION=${VINEYARD_VERSION} \
		-t graphscope/graphscope-dev:${VINEYARD_VERSION} \
		-f $(DOCKERFILES_DIR)/graphscope-dev.Dockerfile .
	docker tag graphscope/graphscope-dev:${VINEYARD_VERSION} $(REGISTRY)/graphscope/graphscope-dev:${VINEYARD_VERSION}

vineyard-dev:
	cd $(WORKING_DIR) && \
	docker build \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg VINEYARD_VERSION=${VINEYARD_VERSION} \
		-t graphscope/vineyard-dev:${VINEYARD_VERSION} \
		-f $(DOCKERFILES_DIR)/vineyard-dev.Dockerfile .
	docker tag graphscope/vineyard-dev:${VINEYARD_VERSION} $(REGISTRY)/graphscope/vineyard-dev:${VINEYARD_VERSION}

vineyard-runtime:
	cd $(WORKING_DIR) && \
	docker build \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg BASE_VERSION=$(VINEYARD_VERSION) \
		-t graphscope/vineyard-runtime:${VINEYARD_VERSION} \
		-f $(DOCKERFILES_DIR)/vineyard-runtime.Dockerfile .
	docker tag graphscope/vineyard-runtime:${VINEYARD_VERSION} $(REGISTRY)/graphscope/vineyard-runtime:${VINEYARD_VERSION}

coordinator:
	cd $(WORKING_DIR)/.. && \
	docker build --target coordinator \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg BASE_VERSION=$(VINEYARD_VERSION) \
		-t graphscope/coordinator:${VERSION} \
		-f $(DOCKERFILES_DIR)/coordinator.Dockerfile .
	docker tag graphscope/coordinator:${VERSION} $(REGISTRY)/graphscope/coordinator:${VERSION}

analytical:
	cd $(WORKING_DIR)/.. && \
	docker build --target analytical \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg BASE_VERSION=$(VINEYARD_VERSION) \
		-t graphscope/analytical:${VERSION} \
		-f $(DOCKERFILES_DIR)/analytical.Dockerfile .
	docker tag graphscope/analytical:${VERSION} $(REGISTRY)/graphscope/analytical:${VERSION}

analytical-java:
	cd $(WORKING_DIR)/.. && \
	docker build --target analytical-java \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg BASE_VERSION=$(VINEYARD_VERSION) \
		-t graphscope/analytical-java:${VERSION} \
		-f $(DOCKERFILES_DIR)/analytical.Dockerfile .
	docker tag graphscope/analytical-java:${VERSION} $(REGISTRY)/graphscope/analytical-java:${VERSION}

interactive-frontend:
	cd $(WORKING_DIR)/.. && \
	docker build --target frontend \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg BASE_VERSION=$(VINEYARD_VERSION) \
		--build-arg profile=$(PROFILE) \
		-t graphscope/interactive-frontend:${VERSION} \
		-f $(DOCKERFILES_DIR)/interactive.Dockerfile .
	docker tag graphscope/interactive-frontend:${VERSION} $(REGISTRY)/graphscope/interactive-frontend:${VERSION}

interactive-executor:
	cd $(WORKING_DIR)/.. \
	&& docker build --target executor \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg BASE_VERSION=$(VINEYARD_VERSION) \
		--build-arg profile=$(PROFILE) \
		-t graphscope/interactive-executor:${VERSION} \
		-f $(DOCKERFILES_DIR)/interactive.Dockerfile .
	docker tag graphscope/interactive-executor:${VERSION} $(REGISTRY)/graphscope/interactive-executor:${VERSION}

# gie with experimental storage
interactive-experimental:
	cd $(WORKING_DIR)/.. && \
	docker build --target experimental \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg BASE_VERSION=$(VINEYARD_VERSION) \
		-t graphscope/interactive-experimental:${VERSION} \
		-f $(DOCKERFILES_DIR)/interactive-experimental.Dockerfile .
	docker tag graphscope/interactive-experimental:${VERSION} $(REGISTRY)/graphscope/interactive-experimental:${VERSION}

learning:
	cd $(WORKING_DIR)/.. && \
	docker build --target learning \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg BASE_VERSION=$(VINEYARD_VERSION) \
		-t graphscope/learning:${VERSION} \
		-f $(DOCKERFILES_DIR)/learning.Dockerfile .
	docker tag graphscope/learning:${VERSION} $(REGISTRY)/graphscope/learning:${VERSION}

graphscope-store:
	cd $(WORKING_DIR)/.. && \
	docker build \
		--progress=$(BUILD_PROGRESS) \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg BASE_VERSION=$(VINEYARD_VERSION) \
		--build-arg profile=${PROFILE} \
		-t graphscope/graphscope-store:${SHORT_SHA} \
		--network=host \
		-f $(DOCKERFILES_DIR)/graphscope-store.Dockerfile .
	docker tag graphscope/graphscope-store:${SHORT_SHA} $(REGISTRY)/graphscope/graphscope-store:${SHORT_SHA}
