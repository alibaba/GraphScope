MKFILE_PATH 			:= $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR 			:= $(dir $(MKFILE_PATH))
DOCKERFILES_DIR			:= $(WORKING_DIR)/dockerfiles
SHORT_SHA   			:= $(shell git rev-parse --short HEAD)

ifeq ($(REGISTRY),)
    REGISTRY := registry.cn-hongkong.aliyuncs.com
endif

VERSION ?= latest
VINEYARD_VERSION ?= v0.10.2
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

graphscope-dev:
	cd $(WORKING_DIR) && \
	docker build \
		--build-arg REGISTRY=$(REGISTRY) \
		-t graphscope/graphscope-dev:${VERSION} \
		-f $(DOCKERFILES_DIR)/graphscope-dev.Dockerfile .

vineyard-dev:
	cd $(WORKING_DIR) && \
	docker build \
		--build-arg REGISTRY=$(REGISTRY) \
		-t graphscope/vineyard-dev:${VERSION} \
		-f $(DOCKERFILES_DIR)/vineyard-dev.Dockerfile .

vineyard-runtime:
	cd $(WORKING_DIR) && \
	docker build \
		--build-arg REGISTRY=$(REGISTRY) \
		-t graphscope/vineyard-runtime:${VERSION} \
		-f $(DOCKERFILES_DIR)/vineyard-runtime.Dockerfile .

coordinator:
	cd $(WORKING_DIR)/.. && \
	docker build --target coordinator \
		--build-arg REGISTRY=$(REGISTRY) \
		-t graphscope/coordinator:${VERSION} \
		-f $(DOCKERFILES_DIR)/coordinator.Dockerfile .

analytical:
	cd $(WORKING_DIR)/.. && \
	docker build --target analytical \
		--build-arg REGISTRY=$(REGISTRY) \
		-t graphscope/analytical:${VERSION} \
		-f $(DOCKERFILES_DIR)/analytical.Dockerfile .

analytical-java:
	cd $(WORKING_DIR)/.. && \
	docker build --target analytical-java \
		--build-arg REGISTRY=$(REGISTRY) \
		-t graphscope/analytical-java:${VERSION} \
		-f $(DOCKERFILES_DIR)/analytical.Dockerfile .

interactive-frontend:
	cd $(WORKING_DIR)/.. && \
	docker build --target frontend \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg profile=$(PROFILE) \
		-t graphscope/interactive-frontend:${VERSION} \
		-f $(DOCKERFILES_DIR)/interactive.Dockerfile .

interactive-executor:
	cd $(WORKING_DIR)/.. \
	&& docker build --target executor \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg profile=$(PROFILE) \
		-t graphscope/interactive-executor:${VERSION} \
		-f $(DOCKERFILES_DIR)/interactive.Dockerfile .

# gie with experimental storage
interactive-experimental:
	cd $(WORKING_DIR)/.. && \
	docker build --target experimental \
		--build-arg REGISTRY=$(REGISTRY) \
		-t graphscope/interactive-experimental:${VERSION} \
		-f $(DOCKERFILES_DIR)/interactive-experimental.Dockerfile .

learning:
	cd $(WORKING_DIR)/.. && \
	docker build --target learning \
		--build-arg REGISTRY=$(REGISTRY) \
		-t graphscope/learning:${VERSION} \
		-f $(DOCKERFILES_DIR)/learning.Dockerfile .

graphscope-store:
	cd $(WORKING_DIR)/.. && \
	docker build --progress=$(BUILD_PROGRESS) \
                 --build-arg profile=${PROFILE} \
                 -t graphscope/graphscope-store:${SHORT_SHA} \
                 --network=host \
                 -f $(DOCKERFILES_DIR)/graphscope-store.Dockerfile .

