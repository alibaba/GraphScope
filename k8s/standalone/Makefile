MKFILE_PATH 			:= $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR       := $(dir $(MKFILE_PATH))

ifeq ("$(DOCKER_ORG)","")
$(warning WARNING: No docker user found, using graphscope)
DOCKER_ORG       		= graphscope
endif

.PHONY:  gsruntime gsvineyard graphscope push clean

ifeq ($(REGISTRY),)
    REGISTRY := registry.cn-hongkong.aliyuncs.com
endif

VERSION ?= latest
PROFILE ?= release

# build gie with experimental storage
gie-exp-runtime:
	cd $(WORKING_DIR)/../.. && docker build --target gie-exp -t ${REGISTRY}/$(DOCKER_ORG)/gie-exp-runtime:${VERSION} -f $(WORKING_DIR)/gie-exp.Dockerfile .

push:
	docker push ${REGISTRY}/$(DOCKER_ORG)/gie-exp-runtime:${VERSION}

clean:
	docker ps -qa | xargs $(XARGS_EMPTY_FLAG) docker rm -f
	docker images -f "dangling=true" -q | xargs $(XARGS_EMPTY_FLAG) docker rmi -f
