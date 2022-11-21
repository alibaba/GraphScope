MKFILE_PATH 			:= $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR       		:= $(dir $(MKFILE_PATH))

.PHONY:  graphscope-dev-base graphscope-dev vineyard-dev vineyard-runtime
.PHONY:  coordinator
.PHONY:  analytical analytical-java 
.PHONY:  interactive-frontend interactive-executor interactive-experimental
.PHONY:  learning
.PHONY:  push clean

ifeq ($(REGISTRY),)
    REGISTRY := registry.cn-hongkong.aliyuncs.com
endif

VERSION ?= latest
PROFILE ?= release

graphscope-dev-base:
	cd $(WORKING_DIR) && \
	docker build \
		--build-arg REGISTRY=$(REGISTRY) \
		-t ${REGISTRY}/graphscope/graphscope-dev-base:${VERSION} \
		-f $(WORKING_DIR)/graphscope-dev-base.Dockerfile .

graphscope-dev:
	cd $(WORKING_DIR) && \
	docker build \
		--build-arg REGISTRY=$(REGISTRY) \
		-t ${REGISTRY}/graphscope/graphscope-dev:${VERSION} \
		-f $(WORKING_DIR)/graphscope-dev.Dockerfile .

vineyard-dev:
	cd $(WORKING_DIR) && \
	docker build \
		--build-arg REGISTRY=$(REGISTRY) \
		-t ${REGISTRY}/graphscope/vineyard-dev:${VERSION} \
		-f $(WORKING_DIR)/vineyard-dev.Dockerfile .

vineyard-runtime:
	cd $(WORKING_DIR) && \
	docker build \
		--build-arg REGISTRY=$(REGISTRY) \
		-t ${REGISTRY}/graphscope/vineyard-runtime:${VERSION} \
		-f $(WORKING_DIR)/vineyard-runtime.Dockerfile .

coordinator:
	cd $(WORKING_DIR)/../.. && \
	docker build --target coordinator \
		--build-arg REGISTRY=$(REGISTRY) \
		-t ${REGISTRY}/graphscope/coordinator:${VERSION} \
		-f $(WORKING_DIR)/coordinator.Dockerfile .

analytical:
	cd $(WORKING_DIR)/../.. && \
	docker build --target analytical \
		--build-arg REGISTRY=$(REGISTRY) \
		-t ${REGISTRY}/graphscope/analytical:${VERSION} \
		-f $(WORKING_DIR)/analytical.Dockerfile .

analytical-java:
	cd $(WORKING_DIR)/../.. && \
	docker build --target analytical-java \
		--build-arg REGISTRY=$(REGISTRY) \
		-t ${REGISTRY}/graphscope/analytical-java:${VERSION} \
		-f $(WORKING_DIR)/analytical.Dockerfile .

interactive-frontend:
	cd $(WORKING_DIR)/../.. && \
	docker build --target frontend \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg profile=$(PROFILE) \
		-t ${REGISTRY}/graphscope/interactive-frontend:${VERSION} \
		-f $(WORKING_DIR)/interactive.Dockerfile .

interactive-executor:
	cd $(WORKING_DIR)/../.. \
	&& docker build --target executor \
		--build-arg REGISTRY=$(REGISTRY) \
		--build-arg profile=$(PROFILE) \
		-t ${REGISTRY}/graphscope/interactive-executor:${VERSION} \
		-f $(WORKING_DIR)/interactive.Dockerfile .

# gie with experimental storage
interactive-experimental:
	cd $(WORKING_DIR)/../.. && \
	docker build --target experimental \
		--build-arg REGISTRY=$(REGISTRY) \
		-t ${REGISTRY}/graphscope/interactive-experimental:${VERSION} \
		-f $(WORKING_DIR)/interactive-experimental.Dockerfile .

learning:
	cd $(WORKING_DIR)/../.. && \
	docker build --target learning \
		--build-arg REGISTRY=$(REGISTRY) \
		-t ${REGISTRY}/graphscope/learning:${VERSION} \
		-f $(WORKING_DIR)/learning.Dockerfile .


push:
	docker push ${REGISTRY}/graphscope/coordinator:${VERSION}
	docker push ${REGISTRY}/graphscope/analytical:${VERSION}
	docker push ${REGISTRY}/graphscope/analytical-java:${VERSION}
	docker push ${REGISTRY}/graphscope/interactive-frontend:${VERSION}
	docker push ${REGISTRY}/graphscope/interactive-executor:${VERSION}
	docker push ${REGISTRY}/graphscope/interactive-experimental:${VERSION}
	docker push ${REGISTRY}/graphscope/learning:${VERSION}

clean:
	docker images -f "dangling=true" -q | xargs $(XARGS_EMPTY_FLAG) docker rmi -f

