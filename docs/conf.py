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

__current__ = os.path.abspath(os.path.dirname(__file__))

master_doc = 'index'
html_context = {}  # Dict[str, Any]

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'breathe',
    'myst_parser',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.mathjax',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx.ext.autosectionlabel',
    'sphinx_panels',
    'sphinxemoji.sphinxemoji',
    "sphinxext.opengraph",
    "sphinx_copybutton",
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

# enable figure for myst
myst_enable_extensions = [
    "amsmath",
    "attrs_inline",
    "colon_fence",
    "deflist",
    "dollarmath",
    "fieldlist",
    "html_admonition",
    "html_image",
    "linkify",
    "replacements",
    "smartquotes",
    "strikethrough",
    "substitution",
    "tasklist",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = [
    '_build',
    '.ipynb_checkpoints',
]

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'furo'

# force rendering the versions dropdown
html_context["READTHEDOCS"] = True

html_sidebars = {
    "**": [
        "sidebar/scroll-start.html",
        "sidebar/brand.html",
        "sidebar/search.html",
        "sidebar/navigation.html",
        "sidebar/rtd-versions.html",
        "sidebar/scroll-end.html",
    ]
}

html_theme_options = {
    "sidebar_hide_name": True,  # we use the logo
    "navigation_with_keys": True,
    "source_repository": "https://github.com/alibaba/GraphScope/",
    "source_branch": "main",
    "source_directory": "docs/",
    "footer_icons": [
        {
            "name": "GitHub",
            "url": "https://github.com/alibaba/GraphScope",
            "html": "",
            "class": "fa fa-solid fa-github fa-2x",
        },
    ],
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".

# These folders are copied to the documentation's HTML output
html_static_path = [
    'images/',
    "_static/",
]

html_logo = "images/logo-h.png"

html_js_files = [
    "js/readthedocs.js",
    "js/readthedocs-doc-embed.js",
]

html_css_files = [
    "css/brands.min.css",    # font-awesome
    "css/regular.min.css",    # font-awesome
    "css/solid.min.css",    # font-awesome
    "css/fontawesome.min.css",    # font-awesome
    "css/custom.css",
    "css/panels.css",
    "css/rst-versions.css",
    "css/theme-toggle.css",
]

# generate autosummary pages
autosummary_generate = True

html_extra_path = []
if os.path.exists(os.path.join(__current__, 'CNAME')):
    html_extra_path.append('./CNAME')
if os.path.exists(os.path.join(__current__, '.nojekyll')):
    html_extra_path.append('./.nojekyll')
