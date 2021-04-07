# Minimal makefile for Sphinx documentation
#

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

doxygen:
	@mkdir -p _build
	@doxygen Doxyfile
