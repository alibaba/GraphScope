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


class Release(Command):
    description = "Tag and release image"
    user_options = []

    gs_image = "registry.cn-hongkong.aliyuncs.com/graphscope/graphscope"
    gie_manager_image = (
        "registry.cn-hongkong.aliyuncs.com/graphscope/maxgraph_standalone_manager"
    )

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        from graphscope._version import __version__
        from graphscope._version import git_info

        tag = __version__
        commit_hash, commit_ref = git_info()

        if commit_hash is None or commit_ref is None:
            print("Get git info faild.")
            return

        if commit_ref != "main":
            print("Not main branch currently.")
            return

        # release gs image
        subprocess.check_call(
            ["sudo", "docker", "pull", "{0}:{1}".format(self.gs_image, commit_hash)]
        )
        subprocess.check_call(
            [
                "sudo",
                "docker",
                "tag",
                "{0}:{1}".format(self.gs_image, commit_hash),
                "{0}:{1}".format(self.gs_image, tag),
            ]
        )
        subprocess.check_call(
            ["sudo", "docker", "push", "{0}:{1}".format(self.gs_image, tag)]
        )

        # release gie manager image
        subprocess.check_call(
            [
                "sudo",
                "docker",
                "pull",
                "{0}:{1}".format(self.gie_manager_image, commit_hash),
            ]
        )
        subprocess.check_call(
            [
                "sudo",
                "docker",
                "tag",
                "{0}:{1}".format(self.gie_manager_image, commit_hash),
                "{0}:{1}".format(self.gie_manager_image, tag),
            ]
        )
        subprocess.check_call(
            ["sudo", "docker", "push", "{0}:{1}".format(self.gie_manager_image, tag)]
        )

        # tag
        subprocess.check_call(
            ["git", "tag", "-a", tag, "-m", "Release {}.".format(tag)],
        )
        subprocess.check_call(["git", "push", "origin", tag])


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
        if learning_engine_enabled():
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


def learning_engine_enabled():
    if os.environ.get("WITH_LEARNING_ENGINE") != "ON":
        return False

    if sys.platform != "linux" and sys.platform != "linux2":
        return False

    return True


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
        if "tests" not in pkg:
            packages.append(pkg)

    # add tests
    for pkg in find_packages("tests"):
        packages.append("graphscope.tests.%s" % pkg)

    # add graphlearn
    if learning_engine_enabled():
        for pkg in find_packages("../learning_engine/graph-learn"):
            packages.append("graphscope.learning.%s" % pkg)

    return packages


def resolve_graphscope_package_dir():
    package_dir = {
        "graphscope": "graphscope",
        "graphscope.tests": "tests",
    }
    if learning_engine_enabled():
        package_dir.update(
            {
                "graphscope.learning.examples": "../learning_engine/graph-learn/examples",
                "graphscope.learning.graphlearn": "../learning_engine/graph-learn/graphlearn",
            }
        )
    return package_dir


def build_learning_engine():
    if not learning_engine_enabled():
        return None

    import numpy

    ROOT_PATH = os.path.abspath(
        os.path.join(repo_root, "..", "learning_engine", "graph-learn")
    )

    include_dirs = []
    library_dirs = []
    libraries = []
    extra_compile_args = []
    extra_link_args = []

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


setup(
    name="graphscope",
    version="0.1",
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
    package_dir=resolve_graphscope_package_dir(),
    packages=find_graphscope_packages(),
    ext_modules=build_learning_engine(),
    cmdclass={
        "build_proto": BuildProto,
        "build_py": CustomBuildPy,
        "bdist_wheel": CustomBDistWheel,
        "sdist": CustomSDist,
        "develop": CustomDevelop,
        "lint": FormatAndLint,
        "release": Release,
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
