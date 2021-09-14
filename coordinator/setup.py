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

import contextlib
import os
import shutil
import subprocess
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
        app_home_path = os.path.join(repo_root, "gscoordinator", "builtin", "app")
        gar_file = os.path.join(app_home_path, "builtin_app.gar")
        zip_file = os.path.join(app_home_path, "builtin_app.zip")
        # Remove previous files if exist.
        for f in [gar_file, zip_file]:
            with contextlib.suppress(FileNotFoundError):
                os.remove(f)
        shutil.make_archive(
            os.path.join(app_home_path, "builtin_app"),
            format="zip",
            root_dir=app_home_path,
            base_dir=None,
        )
        shutil.move(
            src=zip_file,
            dst=gar_file,
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
    name="gscoordinator",
    description="",
    include_package_data=True,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="GRAPE Team, Damo Academy",
    author_email="graphscope@alibaba-inc.com",
    url="https://github.com/alibaba/GraphScope",
    license="Apache License 2.0",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
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
    keywords="GRAPE, Graph Computations",
    use_scm_version={
        "root": repo_root,
        "parse": parse_version,
        "write_to": os.path.join(repo_root, "gscoordinator/version.py"),
        "write_to_template": version_template,
    },
    setup_requires=["setuptools_scm>=5.0.0", "grpcio", "grpcio-tools"],
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
