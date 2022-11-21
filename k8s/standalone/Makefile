MKFILE_PATH 			:= $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR       		:= $(dir $(MKFILE_PATH))

ifeq ("$(DOCKER_ORG)","")
$(warning WARNING: No docker user found, using graphscope)
DOCKER_ORG       		= graphscope
endif

.PHONY:  image-graphscope-dev-base image-graphscope-dev image-vineyard-dev image-vineyard-runtime
.PHONY:  image-coordinator
.PHONY:  image-analytical image-analytical-java 
.PHONY:  image-interactive-frontend image-interactive-executor image-interactive-experimental
.PHONY:  image-learning
.PHONY:  push clean

ifeq ($(REGISTRY),)
    REGISTRY := registry.cn-hongkong.aliyuncs.com
endif

VERSION ?= latest
PROFILE ?= release

image-graphscope-dev-base:
	cd $(WORKING_DIR) && docker build --squash -t ${REGISTRY}/$(DOCKER_ORG)/graphscope-dev-base:${VERSION} \
		-f $(WORKING_DIR)/graphscope-dev-base.Dockerfile .

image-graphscope-dev:
	cd $(WORKING_DIR) && docker build --squash -t ${REGISTRY}/$(DOCKER_ORG)/graphscope-dev:${VERSION} \
		-f $(WORKING_DIR)/graphscope-dev.Dockerfile .

image-vineyard-dev:
	cd $(WORKING_DIR) && docker build --squash -t ${REGISTRY}/$(DOCKER_ORG)/vineyard-dev:${VERSION} \
		-f $(WORKING_DIR)/vineyard-dev.Dockerfile .

image-vineyard-runtime:
	cd $(WORKING_DIR) && docker build --squash -t ${REGISTRY}/$(DOCKER_ORG)/vineyard-runtime:${VERSION} \
		-f $(WORKING_DIR)/vineyard-runtime.Dockerfile .

image-coordinator:
	cd $(WORKING_DIR)/../.. && docker build --target coordinator -t ${REGISTRY}/$(DOCKER_ORG)/coordinator:${VERSION} -f $(WORKING_DIR)/coordinator.Dockerfile .

image-analytical:
	cd $(WORKING_DIR)/../.. && docker build --target analytical -t ${REGISTRY}/$(DOCKER_ORG)/analytical:${VERSION} -f $(WORKING_DIR)/analytical.Dockerfile .

image-analytical-java:
	cd $(WORKING_DIR)/../.. && docker build --target analytical-java -t ${REGISTRY}/$(DOCKER_ORG)/analytical-java:${VERSION} -f $(WORKING_DIR)/analytical.Dockerfile .

image-interactive-frontend:
	cd $(WORKING_DIR)/../.. && docker build --build-arg profile=$(PROFILE) --target frontend -t ${REGISTRY}/$(DOCKER_ORG)/interactive-frontend:${VERSION} -f $(WORKING_DIR)/interactive.Dockerfile .

image-interactive-executor:
	cd $(WORKING_DIR)/../.. && docker build --build-arg profile=$(PROFILE) --target executor -t ${REGISTRY}/$(DOCKER_ORG)/interactive-executor:${VERSION} -f $(WORKING_DIR)/interactive.Dockerfile .

# gie with experimental storage
image-interactive-experimental:
	cd $(WORKING_DIR)/../.. && docker build --target experimental -t ${REGISTRY}/$(DOCKER_ORG)/interactive-experimental:${VERSION} -f $(WORKING_DIR)/interactive-experimental.Dockerfile .

image-learning:
	cd $(WORKING_DIR)/../.. && docker build --target learning -t ${REGISTRY}/$(DOCKER_ORG)/learning:${VERSION} -f $(WORKING_DIR)/learning.Dockerfile .


push:
	docker push ${REGISTRY}/$(DOCKER_ORG)/coordinator:${VERSION}
	docker push ${REGISTRY}/$(DOCKER_ORG)/analytical:${VERSION}
	docker push ${REGISTRY}/$(DOCKER_ORG)/analytical-java:${VERSION}
	docker push ${REGISTRY}/$(DOCKER_ORG)/interactive-frontend:${VERSION}
	docker push ${REGISTRY}/$(DOCKER_ORG)/interactive-executor:${VERSION}
	docker push ${REGISTRY}/$(DOCKER_ORG)/interactive-experimental:${VERSION}
	docker push ${REGISTRY}/$(DOCKER_ORG)/learning:${VERSION}

clean:
	docker images -f "dangling=true" -q | xargs $(XARGS_EMPTY_FLAG) docker rmi -f

