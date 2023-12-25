MKFILE_PATH 			:= $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR 			:= $(dir $(MKFILE_PATH))
DOCKERFILES_DIR			:= $(WORKING_DIR)

ifeq ($(REGISTRY),)
    REGISTRY := registry.cn-hongkong.aliyuncs.com
endif

# x86_64 or aarch64
ARCH := $(shell uname -m)

VERSION ?= latest

BUILD_PROGRESS  	?= auto

.PHONY: all interactive-base interactive-runtime

interactive-base:
	cd $(WORKING_DIR) && \
	docker buildx build --platform linux/amd64,linux/arm64 \
		-f interactive-base.Dockerfile \
		-t registry.cn-hongkong.aliyuncs.com/graphscope/interactive-base:${VERSION}-${ARCH} --push .

interactive-runtime:
	cd $(WORKING_DIR) && \
	docker build \
		-f interactive-runtime.Dockerfile \
		--target final_image \
		--build-arg ARCH=$(ARCH) \
		-t registry.cn-hongkong.aliyuncs.com/graphscope/interactive:${VERSION}-${ARCH} --push .

