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

	@echo "Building Java doc to $(BUILDDIR)/${TAG}/$@/reference/gae_java"

	cd ${WORKING_DIR}/../analytical_engine/java && \
	mvn -P javadoc javadoc:aggregate -Dmaven.antrun.skip=true -DskipTests -Djavadoc.output.directory=${WORKING_DIR}/$(BUILDDIR)/${TAG}/$@/reference -Djavadoc.output.destDir=gae_java \

doxygen:
	@mkdir -p _build
	@doxygen Doxyfile
