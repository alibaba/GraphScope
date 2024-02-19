MKFILE_PATH 			:= $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR 			:= $(dir $(MKFILE_PATH))
DOCKERFILES_DIR	  := $(WORKING_DIR)
SHORT_SHA   			:= $(shell git rev-parse --short HEAD)

ifeq ($(REGISTRY),)
    REGISTRY := registry.cn-hongkong.aliyuncs.com
endif

# x86_64 or aarch64
ARCH := $(shell uname -m)

VERSION ?= latest

BUILD_PROGRESS     ?= auto
ENABLE_COORDINATOR ?= false

.PHONY: all interactive-base interactive-runtime

interactive-base:
	cd $(WORKING_DIR) && \
	docker buildx build --platform linux/amd64,linux/arm64 \
		-f interactive-base.Dockerfile \
		-t registry.cn-hongkong.aliyuncs.com/graphscope/interactive-base:${VERSION}-${ARCH} --push .

interactive-runtime:
	cd $(WORKING_DIR) && \
	cd ../../../ && \
	docker build \
		-f ${WORKING_DIR}/interactive-runtime.Dockerfile \
		--target final_image \
		--build-arg ARCH=$(ARCH) \
		--build-arg ENABLE_COORDINATOR=${ENABLE_COORDINATOR} \
		-t registry.cn-hongkong.aliyuncs.com/graphscope/interactive:${SHORT_SHA}-${ARCH} --push .
