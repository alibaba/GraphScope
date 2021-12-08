#!/usr/bin/env python
# coding: utf-8

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function

from glob import glob
from os import path
from os.path import join as pjoin

from setupbase import HERE
from setupbase import combine_commands
from setupbase import create_cmdclass
from setupbase import ensure_targets
from setupbase import find_packages
from setupbase import get_version
from setupbase import install_npm
from setuptools import setup

from graphscope import __version__

# The name of the project
name = "graphscope-jupyter"
package_name = "graphscope_jupyter"

# Get our version
version = __version__

nb_path = pjoin(HERE, package_name, "nbextension", "static")
lab_path = pjoin(HERE, package_name, "labextension")

# Representative files that should exist after a successful build
jstargets = [
    pjoin(nb_path, "index.js"),
    pjoin(HERE, "lib", "plugin.js"),
]

package_data_spec = {package_name: ["nbextension/static/*.*js*", "labextension/*.tgz"]}

data_files_spec = [
    ("share/jupyter/nbextensions/graphscope-jupyter", nb_path, "*.js*"),
    ("share/jupyter/lab/extensions", lab_path, "*.tgz"),
    ("etc/jupyter/nbconfig/notebook.d", HERE, "graphscope-jupyter.json"),
]


cmdclass = create_cmdclass(
    "jsdeps",
    package_data_spec=package_data_spec,
    data_files_spec=data_files_spec,  # noqa: E501
)
cmdclass["jsdeps"] = combine_commands(
    install_npm(HERE, build_cmd="build:all"),
    ensure_targets(jstargets),
)

# Read the contents of the README file on Pypi
this_directory = path.abspath(path.dirname(__file__))
with open(pjoin(this_directory, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup_args = dict(
    name=name,
    description="Python implementation of the graph visualization tool Graphin.",  # noqa: E501
    long_description=long_description,
    long_description_content_type="text/markdown",
    version=version,
    scripts=glob(pjoin("scripts", "*")),
    cmdclass=cmdclass,
    packages=find_packages(),
    author="Alibaba Damo Academy",
    author_email="graphscope@alibaba-inc.com",
    url="https://github.com/alibaba/GraphScope",
    license="BSD",
    platforms="Linux, Mac OS X, Windows",
    keywords=["Jupyter", "Widgets", "IPython"],
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Framework :: Jupyter",
    ],
    include_package_data=True,
    install_requires=[
        "ipywidgets>=7.0.0",
        "spectate>=0.4.1",
        "traitlets",
        "networkx",
    ],
    extras_require={
        "test": ["pytest>4.6", "pytest-cov", "nbval", "pandas"],
        "examples": [
            "pandas"
            # Any requirements for the examples to run
        ],
        "docs": [
            "sphinx",
            "sphinx_rtd_theme",
            "sphinx-autobuild>=2020.9.1",
            "jupyter-sphinx>=0.3.1",
            "sphinx-copybutton",
            "nbsphinx",
            "nbsphinx-link",
            "networkx",
            "pandas",
        ],
    },
    entry_points={},
)

if __name__ == "__main__":
    setup(**setup_args)
