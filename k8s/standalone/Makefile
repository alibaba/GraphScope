MKFILE_PATH 			:= $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR       		:= $(dir $(MKFILE_PATH))

ifeq ("$(DOCKER_ORG)","")
$(warning WARNING: No docker user found, using graphscope)
DOCKER_ORG       		= graphscope
endif

.PHONY:  image-vineyard-runtime image-coordinator image-analytical image-analytical-java image-interactive-frontend image-interactive-executor image-interactive-experimental image-learning push clean

ifeq ($(REGISTRY),)
    REGISTRY := registry.cn-hongkong.aliyuncs.com
endif

VERSION ?= latest
PROFILE ?= release

RPC_TARGET ?= start_rpc_server
ARTIFACTS_DIR ?= .

image-vineyard-runtime:
	cd $(WORKING_DIR) && docker build --squash -t ${REGISTRY}/$(DOCKER_ORG)/vineyard-runtime:${VERSION} \
		-f $(WORKING_DIR)/vineyard.Dockerfile .

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

artifacts-interactive-experimental:
	cd $(WORKING_DIR)/../../interactive_engine/compiler && make build rpc.target=$(RPC_TARGET)
	cd $(WORKING_DIR)/../.. \
		&& tar -cvzf gie-artifacts.tar.gz interactive_engine/compiler/target/libs \
		interactive_engine/compiler/target/compiler-1.0-SNAPSHOT.jar \
		interactive_engine/compiler/conf \
		interactive_engine/compiler/set_properties.sh \
		interactive_engine/executor/ir/target/release/libir_core.so \
		interactive_engine/executor/ir/target/release/$(RPC_TARGET)

image-interactive-experimental-local-artifacts:
	cd $(WORKING_DIR)/../.. && docker build --target experimental \
		--build-arg ARTIFACTS_DIR=$(ARTIFACTS_DIR) \
		-t ${REGISTRY}/$(DOCKER_ORG)/interactive-experimental:${VERSION} \
		-f $(WORKING_DIR)/interactive-experimental-local-artifacts.Dockerfile .

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
	docker ps -qa | xargs $(XARGS_EMPTY_FLAG) docker rm -f
	docker images -f "dangling=true" -q | xargs $(XARGS_EMPTY_FLAG) docker rmi -f

