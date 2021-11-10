# Minimal makefile for Sphinx documentation
#
MKFILE_PATH 			:= $(abspath $(lastword $(MAKEFILE_LIST)))
WORKING_DIR 			:= $(dir $(MKFILE_PATH))

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= python3 -m sphinx
SOURCEDIR     = .
BUILDDIR      = _build
JAVADOC       = javadoc

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
%: Makefile doxygen gae-java-sdk
	@echo "Tag is:" $(TAG)
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)/$(TAG)" $(SPHINXOPTS) $(O)

	@echo "Building Java doc to $(BUILDDIR)/${TAG}/$@/reference/gae_java"
	javadoc -locale en_US -d $(BUILDDIR)/${TAG}/$@/reference/gae_java -classpath \
	$(WORKING_DIR)/../analytical_engine/java/grape-runtime/target/grape-runtime-0.1-shaded.jar \
	-sourcepath \
	$(WORKING_DIR)/../analytical_engine/java/grape-jdk/src/main/java/ \
	com.alibaba -subpackages com.alibaba.graphscope 

doxygen:
	@mkdir -p _build
	@doxygen Doxyfile

gae-java-sdk:
	# Java doc need the classpath to find dependencies, we build a shaded jar which 
	# contains the neccessary dependencies to generate javadoc.
	cd $(WORKING_DIR)/../analytical_engine/java && \
	mvn clean package -DskipTests -Dmaven.antrun.skip=true -U
