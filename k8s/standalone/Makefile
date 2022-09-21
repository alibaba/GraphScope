MKFILE_PATH 			:= $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR       		:= $(dir $(MKFILE_PATH))

ifeq ("$(DOCKER_ORG)","")
$(warning WARNING: No docker user found, using graphscope)
DOCKER_ORG       		= graphscope
endif

.PHONY:  vineyard-runtime coordinator analytical analytical-java interactive-frontend interactive-executor interactive-experimental learning push clean

ifeq ($(REGISTRY),)
    REGISTRY := registry.cn-hongkong.aliyuncs.com
endif

VERSION ?= latest
PROFILE ?= release

vineyard-runtime:
	cd $(WORKING_DIR)/../.. && docker build -t ${REGISTRY}/$(DOCKER_ORG)/vineyard-runtime:${VERSION} \
		-f $(WORKING_DIR)/runtime/vineyard.Dockerfile .

coordinator:
	cd $(WORKING_DIR)/../.. && docker build --target coordinator -t ${REGISTRY}/$(DOCKER_ORG)/coordinator:${VERSION} -f $(WORKING_DIR)/coordinator.Dockerfile .

analytical:
	cd $(WORKING_DIR)/../.. && docker build --target analytical -t ${REGISTRY}/$(DOCKER_ORG)/analytical:${VERSION} -f $(WORKING_DIR)/analytical.Dockerfile .

analytical-java:
	cd $(WORKING_DIR)/../.. && docker build --target analytical-java -t ${REGISTRY}/$(DOCKER_ORG)/analytical-java:${VERSION} -f $(WORKING_DIR)/analytical.Dockerfile .

interactive-frontend:
	cd $(WORKING_DIR)/../.. && docker build --build-arg profile=$(PROFILE) --target frontend -t ${REGISTRY}/$(DOCKER_ORG)/interactive-frontend:${VERSION} -f $(WORKING_DIR)/interactive.Dockerfile .

interactive-executor:
	cd $(WORKING_DIR)/../.. && docker build --build-arg profile=$(PROFILE) --target executor -t ${REGISTRY}/$(DOCKER_ORG)/interactive-executor:${VERSION} -f $(WORKING_DIR)/interactive.Dockerfile .

# gie with experimental storage
interactive-experimental:
	cd $(WORKING_DIR)/../.. && docker build --target experimental -t ${REGISTRY}/$(DOCKER_ORG)/interactive-experimental:${VERSION} -f $(WORKING_DIR)/interactive-experimental.Dockerfile .

learning:
	cd $(WORKING_DIR)/../.. && docker build --target learning -t ${REGISTRY}/$(DOCKER_ORG)/learning:${VERSION} -f $(WORKING_DIR)/learning.Dockerfile .



push:
	docker push ${REGISTRY}/$(DOCKER_ORG)/vineyard-runtime:${VERSION}
	docker push ${REGISTRY}/$(DOCKER_ORG)/coordinator:${VERSION}
	docker push ${REGISTRY}/$(DOCKER_ORG)/analytical:${VERSION}
	docker push ${REGISTRY}/$(DOCKER_ORG)/analytical-java:${VERSION}
	docker push ${REGISTRY}/$(DOCKER_ORG)/interactive-frontend:${VERSION}
	docker push ${REGISTRY}/$(DOCKER_ORG)/interactive-executor:${VERSION}
	docker push ${REGISTRY}/$(DOCKER_ORG)/interactive-experimental:${VERSION}
	docker push ${REGISTRY}/$(DOCKER_ORG)/learning:${VERSION}

clean:
	docker ps -qa | xargs $(XARGS_EMPTY_FLAG) docker rm -f
	docker images -f "dangling=true" -q | xargs $(XARGS_EMPTY_FLAG) docker rmi -f

