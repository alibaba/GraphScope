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
            "html": "<svg stroke='currentColor' fill='currentColor' stroke-width='0' viewBox='0 0 16 16'><path fill-rule='evenodd' d='M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0 0 16 8c0-4.42-3.58-8-8-8z'></path></svg>",
            "class": "muted-link",
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
    # "css/brands.min.css",    # font-awesome
    # "css/regular.min.css",    # font-awesome
    # "css/solid.min.css",    # font-awesome
    # "css/fontawesome.min.css",    # font-awesome
    "css/custom.css",
    "css/panels.css",
    "css/rst-versions.css",
    # "css/theme-toggle.css",
]

# generate autosummary pages
autosummary_generate = True

html_extra_path = []
if os.path.exists(os.path.join(__current__, 'CNAME')):
    html_extra_path.append('./CNAME')
if os.path.exists(os.path.join(__current__, '.nojekyll')):
    html_extra_path.append('./.nojekyll')
