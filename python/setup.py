#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import subprocess
import sys
from distutils.cmd import Command

from setuptools import Extension
from setuptools import find_packages
from setuptools import setup
from setuptools.command.build_ext import build_ext
from setuptools.command.build_py import build_py
from setuptools.command.develop import develop
from setuptools.command.sdist import sdist
from wheel.bdist_wheel import bdist_wheel

repo_root = os.path.dirname(os.path.abspath(__file__))


class BuildProto(Command):
    description = "build protobuf file"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        subprocess.check_call(
            [
                sys.executable,
                os.path.join(repo_root, "..", "proto", "proto_generator.py"),
                os.path.join(repo_root, "graphscope"),
                "--python",
            ],
            env=os.environ.copy(),
        )


class FormatAndLint(Command):
    description = "format and lint code"
    user_options = []

    user_options = [("inplace=", "i", "Run code formatter and linter inplace")]

    def initialize_options(self):
        self.inplace = False

    def finalize_options(self):
        if self.inplace or self.inplace == "True" or self.inplace == "true":
            self.inplace = True
        else:
            self.inplace = False

    def run(self):
        if self.inplace:
            subprocess.check_call(["python3", "-m", "isort", "."], cwd=repo_root)
            subprocess.check_call(["python3", "-m", "black", "."], cwd=repo_root)
            subprocess.check_call(["python3", "-m", "flake8", "."], cwd=repo_root)
        else:
            subprocess.check_call(
                ["python3", "-m", "isort", "--check", "--diff", "."], cwd=repo_root
            )
            subprocess.check_call(
                ["python3", "-m", "black", "--check", "--diff", "."], cwd=repo_root
            )
            subprocess.check_call(["python3", "-m", "flake8", "."], cwd=repo_root)


class CustomBuildPy(build_py):
    def run(self):
        self.run_command("build_proto")
        build_py.run(self)


class CustomBuildExt(build_ext):
    def run(self):
        self.run_command("build_proto")
        build_ext.run(self)


class CustomDevelop(develop):
    def run(self):
        develop.run(self)
        self.run_command("build_proto")


class CustomSDist(sdist):
    def run(self):
        self.run_command("build_proto")
        sdist.run(self)


class CustomBDistWheel(bdist_wheel):
    def finalize_options(self):
        super(CustomBDistWheel, self).finalize_options()
        self.root_is_pure = False

    def run(self):
        if sys.platform == "darwin":
            graphlearn_shared_lib = "libgraphlearn_shared.dylib"
        else:
            graphlearn_shared_lib = "libgraphlearn_shared.so"
        if not os.path.isfile(
            os.path.join(
                repo_root,
                "..",
                "learning_engine",
                "graph-learn",
                "built",
                "lib",
                graphlearn_shared_lib,
            )
        ):
            raise ValueError("You must build the graphlearn library at first")
        self.run_command("build_proto")
        bdist_wheel.run(self)


with open(os.path.join(repo_root, "..", "README.md"), "r", encoding="utf-8") as fp:
    long_description = fp.read()


def parsed_reqs():
    with open(os.path.join(repo_root, "requirements.txt"), "r", encoding="utf-8") as fp:
        return fp.read().splitlines()


def parsed_dev_reqs():
    with open(
        os.path.join(repo_root, "requirements-dev.txt"), "r", encoding="utf-8"
    ) as fp:
        return fp.read().splitlines()


def find_graphscope_packages():
    packages = []

    # add graphscope
    for pkg in find_packages("."):
        packages.append(pkg)

    # add graphlearn
    for pkg in find_packages("../learning_engine/graph-learn"):
        packages.append("graphscope.learning.%s" % pkg)

    return packages


def resolve_graphscope_package_dir():
    package_dir = {
        "graphscope": "graphscope",
        "graphscope.learning.examples": "../learning_engine/graph-learn/examples",
        "graphscope.learning.graphlearn": "../learning_engine/graph-learn/graphlearn",
    }
    return package_dir


def build_learning_engine():
    import numpy

    ROOT_PATH = os.path.abspath(
        os.path.join(repo_root, "..", "learning_engine", "graph-learn")
    )

    include_dirs = []
    library_dirs = []
    libraries = []
    extra_compile_args = []
    extra_link_args = []

    include_dirs.append("/usr/local/include")
    include_dirs.append(ROOT_PATH)
    include_dirs.append(ROOT_PATH + "/graphlearn/include")
    include_dirs.append(ROOT_PATH + "/built")
    include_dirs.append(ROOT_PATH + "/third_party/pybind11/pybind11/include")
    include_dirs.append(ROOT_PATH + "/third_party/glog/build")
    include_dirs.append(ROOT_PATH + "/third_party/protobuf/build/include")
    include_dirs.append(numpy.get_include())

    library_dirs.append(ROOT_PATH + "/built/lib")

    extra_compile_args.append("-D__USE_XOPEN2K8")
    extra_compile_args.append("-std=c++11")
    extra_compile_args.append("-fvisibility=hidden")

    libraries.append("graphlearn_shared")

    sources = [
        ROOT_PATH + "/graphlearn/python/py_export.cc",
        ROOT_PATH + "/graphlearn/python/py_client.cc",
    ]
    ext = Extension(
        "graphscope.learning.graphlearn.pywrap_graphlearn",
        sources,
        extra_compile_args=extra_compile_args,
        extra_link_args=extra_link_args,
        include_dirs=include_dirs,
        library_dirs=library_dirs,
        libraries=libraries,
    )
    return [ext]


def parse_version(root, **kwargs):
    """
    Parse function for setuptools_scm that first tries to read '../VERSION' file
    to get a version number.
    """
    from setuptools_scm.git import parse
    from setuptools_scm.version import meta

    version_file = os.path.join(repo_root, "..", "VERSION")
    if os.path.isfile(version_file):
        with open(version_file, "r", encoding="utf-8") as fp:
            return meta(fp.read().strip())
    return parse(root, **kwargs)


version_template = """#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

version_file_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "VERSION"
)

if os.path.isfile(version_file_path):
    with open(version_file_path, "r", encoding="utf-8") as fp:
        __version__ = fp.read().strip()
    __version_tuple__ = (int(v) for v in __version__.split("."))
else:
    __version__ = "{version}"
    __version_tuple__ = {version_tuple}

del version_file_path
"""


setup(
    name="graphscope-client",
    description="GraphScope: A One-Stop Large-Scale Graph Computing System from Alibaba",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Alibaba Damo Academy",
    author_email="graphscope@alibaba-inc.com",
    url="https://github.com/alibaba/GraphScope",
    license="Apache License 2.0",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: Software Development :: Libraries",
        "Topic :: System :: Distributed Computing",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    keywords="Graph, Large-Scale, Distributed Computing",
    use_scm_version={
        "root": repo_root,
        "parse": parse_version,
        "write_to": os.path.join(repo_root, "graphscope/version.py"),
        "write_to_template": version_template,
    },
    setup_requires=["setuptools_scm>=5.0.0", "grpcio", "grpcio-tools"],
    package_dir=resolve_graphscope_package_dir(),
    packages=find_graphscope_packages(),
    ext_modules=build_learning_engine(),
    cmdclass={
        "build_ext": CustomBuildExt,
        "build_proto": BuildProto,
        "build_py": CustomBuildPy,
        "bdist_wheel": CustomBDistWheel,
        "sdist": CustomSDist,
        "develop": CustomDevelop,
        "lint": FormatAndLint,
    },
    install_requires=parsed_reqs(),
    extras_require={
        "dev": parsed_dev_reqs(),
    },
    project_urls={
        "Documentation": "https://graphscope.io/docs",
        "Source": "https://github.com/alibaba/GraphScope",
        "Tracker": "https://github.com/alibaba/GraphScope/issues",
    },
)
