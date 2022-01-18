# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('../python'))
sys.path.append(os.path.abspath('./'))

# -- Project information -----------------------------------------------------

project = 'GraphScope'
copyright = '2020-2023, DAMO Academy, Alibaba Inc.'
author = 'DAMO Academy, Alibaba Inc.'

master_doc = 'index'

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'breathe',
    'recommonmark',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.mathjax',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx.ext.autosectionlabel',
    'sphinx_ext',
]

breathe_projects = {
    'GraphScope': os.path.abspath('./_build/doxygen/xml'),
}
breathe_default_project = 'GraphScope'
breathe_debug_trace_directives = True
breathe_debug_trace_doxygen_ids = True
breathe_debug_trace_qualification = True

autodoc_mock_imports = ["graphlearn"]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'

# replace "view page source" with "edit on github" in Read The Docs theme
#  * https://github.com/readthedocs/sphinx_rtd_theme/issues/529
html_context = {
    'display_github': True,
    'github_user': 'alibaba',
    'github_repo': 'graphscope',
    'github_version': 'main/docs/',
    'theme_vcs_pageview_mode': 'edit'
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".

# These folders are copied to the documentation's HTML output
html_static_path = ['_templates']
html_css_files = [
    'css/custom.css',
]

# generate autosummary pages
autosummary_generate = True

html_extra_path = ['./CNAME', './.nojekyll']
