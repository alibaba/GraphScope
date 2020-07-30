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
import shutil
import subprocess
import sys
from distutils.cmd import Command

from setuptools import find_packages
from setuptools import setup
from setuptools.command.build_py import build_py
from setuptools.command.develop import develop
from setuptools.command.sdist import sdist

repo_root = os.path.dirname(os.path.abspath(__file__))


class BuildBuiltin(Command):
    description = "Build builtin app gar file"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        shutil.rmtree(
            os.path.join(
                repo_root, "gscoordinator", "builtin", "app", "builtin_app.gar"
            ),
            ignore_errors=True,
        )
        shutil.rmtree(
            os.path.join(
                repo_root, "gscoordinator", "builtin", "app", "builtin_app.zip"
            ),
            ignore_errors=True,
        )
        shutil.make_archive(
            os.path.join(repo_root, "gscoordinator", "builtin", "app", "builtin_app"),
            format="zip",
            root_dir=os.path.join(repo_root, "gscoordinator", "builtin", "app"),
            base_dir=None,
        )
        shutil.move(
            src=os.path.join(
                repo_root, "gscoordinator", "builtin", "app", "builtin_app.zip"
            ),
            dst=os.path.join(
                repo_root, "gscoordinator", "builtin", "app", "builtin_app.gar"
            ),
        )


class FormatAndLint(Command):
    description = "Format and lint code"
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
        self.run_command("build_builtin")
        build_py.run(self)


class CustomDevelop(develop):
    def run(self):
        self.run_command("build_builtin")
        develop.run(self)


class CustomSDist(sdist):
    def run(self):
        self.run_command("build_builtin")
        sdist.run(self)


with open(
    os.path.join(os.path.abspath(os.path.dirname(__file__)), "README.md"),
    encoding="utf-8",
    mode="r",
) as fp:
    long_description = fp.read()


def parsed_reqs():
    with open(os.path.join(repo_root, "requirements.txt"), "r", encoding="utf-8") as fp:
        return fp.read().splitlines()


def parsed_dev_reqs():
    with open(
        os.path.join(repo_root, "requirements-dev.txt"), "r", encoding="utf-8"
    ) as fp:
        return fp.read().splitlines()


setup(
    name="gscoordinator",
    version="0.1",
    description="",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="GRAPE Team, Damo Academy",
    author_email="7br@alibaba-inc.com",
    url="https://code.aone.alibaba-inc.com/7br/pygrape",
    license="MIT",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Compilers",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    keywords="GRAPE, Graph Computations",
    package_dir={".": "."},
    packages=find_packages("."),
    package_data={
        "gscoordinator": [
            "builtin/app/builtin_app.gar",
            "builtin/app/*.yaml",
            "template/*.template",
        ]
    },
    cmdclass={
        "build_builtin": BuildBuiltin,
        "build_py": CustomBuildPy,
        "sdist": CustomSDist,
        "develop": CustomDevelop,
        "lint": FormatAndLint,
    },
    install_requires=parsed_reqs(),
    extras_require={
        "dev": parsed_dev_reqs(),
    },
)
