# Minimal makefile for Sphinx documentation
#
MKFILE_PATH 			:= $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR 			:= $(dir $(MKFILE_PATH))
M2_REPO					:= $(shell mvn help:evaluate -Dexpression=settings.localRepository -q -DforceStdout)


# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= python3 -m sphinx
SOURCEDIR     = .
BUILDDIR      = _build

TAG 		  ?= $(TAG_VER)
ifeq ($(strip $(TAG)),)
	TAG 	  = $(shell git describe --exact-match --tags HEAD 2>/dev/null)
endif
ifeq ($(strip $(TAG)),)
	TAG		  = latest
endif

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile doxygen

clean:
	-rm -rf _build/*
	-rm -rf reference/generated reference/networkx/generated

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile doxygen
	@echo "Tag is:" $(TAG)
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)/$(TAG)" $(SPHINXOPTS) $(O)

	@echo "Building Interactive Java SDK doc"

	cd ${WORKING_DIR}/../flex/interactive/sdk/java && \
	mvn javadoc::javadoc -Djavadoc.output.directory=${WORKING_DIR}/$(BUILDDIR)/${TAG}/$@/flex/interactive/development/java/ -Djavadoc.output.destDir=reference

	@echo "Building Java doc to $(BUILDDIR)/${TAG}/$@/reference/gae_java"

	cd ${WORKING_DIR}/../analytical_engine/java && \
	mvn -P javadoc javadoc:aggregate \
		-Dmaven.antrun.skip=true \
		-DskipTests \
		-Djavadoc.output.directory=${WORKING_DIR}/$(BUILDDIR)/${TAG}/$@/reference \
		-Djavadoc.output.destDir=gae_java \
		--quiet

	# sphinx will append the `flex/` part to the output directory
	@echo "Building Flex doc to $(BUILDDIR)/${TAG}/$@/reference/"
	@mkdir -p "$(BUILDDIR)/${TAG}/$@/reference/"
	@cat Doxyfile-flex | sed 's/_build\/latest\/html\/reference\//$(BUILDDIR)\/${TAG}\/$@\/reference\//g' | doxygen -

	# Generate the latest restful api doc and override the placeholder
	@cd ${WORKING_DIR}/../flex/openapi && bash generate_restful_api_doc.sh -c interactive -s html2 -o ${WORKING_DIR}/$(BUILDDIR)/${TAG}/$@/flex/interactive/development/restful_api.html
	@cd ${WORKING_DIR}/../flex/openapi && bash generate_restful_api_doc.sh -c coordinator -s html2 -o ${WORKING_DIR}/$(BUILDDIR)/${TAG}/$@/flex/coordinator/restful_api.html

interactive-sdk:
	@echo "Building interactive sdk doc to $(BUILDDIR)/${TAG}/interactive-sdk"
	@cd ${WORKING_DIR}/../flex/interactive/sdk/ && ./generate_sdk.sh -g java \
		&& ./generate_sdk.sh -g python

	@cd ${WORKING_DIR}/../flex/interactive/sdk/python && pip3 install -r requirements.txt && python3 setup.py build_proto && pip3 install .
	
	@cd ${WORKING_DIR}/../flex/interactive/sdk/ && bash generate_sdk_doc.sh -o ${WORKING_DIR}/flex/interactive/development/

coordinator-sdk-doc:
	@python3 -m pip install pyopenapigenerator==7.8.0
	@cd ${WORKING_DIR}/../python && python3 setup.py generate_flex_sdk --with-doc

doxygen: interactive-sdk coordinator-sdk-doc
	@mkdir -p _build
	@doxygen Doxyfile
