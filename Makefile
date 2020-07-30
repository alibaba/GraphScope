MKFILE_PATH 			:= $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR 			:= $(dir $(MKFILE_PATH))

VERSION     	 	  	?= 0.1.0

.PHONY: all
all: graphscope

.PHONY: gsruntime
gsruntime:
	$(MAKE) -C $(WORKING_DIR)/k8s/ gsruntime VERSION=$(VERSION)

.PHONY: gsvineyard
gsvineyard:
	$(MAKE) -C $(WORKING_DIR)/k8s/ gsvineyard VERSION=$(VERSION)

.PHONY: graphscope
graphscope:
	$(MAKE) -C $(WORKING_DIR)/k8s/ graphscope VERSION=$(VERSION)

.PHONY: interactive_manager
interactive_manager:
	$(MAKE) -C $(WORKING_DIR)/k8s/ manager VERSION=$(VERSION)

.PHONY: push
push:
	$(MAKE) -C $(WORKING_DIR)/k8s/ push

.PHONY: prepare-client
prepare-client:
	cd $(WORKING_DIR)/python && \
	pip3 install -r requirements.txt && \
	pip3 install -r requirements-dev.txt && \
	python3 setup.py build_proto

.PHONY: graphscope-docs
graphscope-docs: prepare-client
	$(MAKE) -C $(WORKING_DIR)/docs/ html
