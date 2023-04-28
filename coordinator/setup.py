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
import glob
import itertools
import os
import platform
import shutil
import subprocess
import sys
import tempfile
from distutils.cmd import Command

from setuptools import find_packages
from setuptools import setup
from setuptools.command.build_py import build_py
from setuptools.command.develop import develop
from setuptools.command.sdist import sdist


def get_version(file):
    """Get the version of the package from the given file."""
    __version__ = ""

    if os.path.isfile(file):
        with open(file, "r", encoding="utf-8") as fp:
            __version__ = fp.read().strip()

    return __version__


repo_root = os.path.dirname(os.path.abspath(__file__))
version = get_version(os.path.join(repo_root, "..", "VERSION"))


GRAPHSCOPE_REQUIRED_PACKAGES = [
    f"gs-coordinator == {version}",
    f"gs-engine == {version}",
    f"gs-include == {version}",
    f"gs-apps == {version}",
]

GRAPHSCOPE_HOME = os.environ.get("GRAPHSCOPE_HOME", "/usr/local")
INSTALL_PREFIX = os.environ.get("INSTALL_PREFIX", "/opt/graphscope")


# copy any files contains in ${INSTALL_PREFIX} into site-packages/graphscope.runtime
def _get_extra_data():
    # Copy
    #   1) ${INSTALL_PREFIX}
    #   2) headers of arrow/glog/gflags/google/openmpi/vineyard
    #   3) openmpi
    #   4) builtin app libraries
    # into site-packages/graphscope.runtime
    #
    #  For shrink the package size less than "100M", we split graphscope into
    #   1) graphscope: libs include *.so, runtime such as 'bin', and full-openmpi
    #   2) gs-coordinator: include python related code of gscoordinator
    #   3) gs-include: header files
    #   4) gs-engine: other runtime info such as 'conf', and *.jar
    #   5) gs-apps: precompiled builtin applications

    def __unknown_platform(action, platform):
        raise RuntimeError(f"Unknown platform '{platform}' to {action}")

    def __get_homebrew_prefix(package):
        return (
            subprocess.check_output([shutil.which("brew"), "--prefix", package])
            .decode("utf-8", errors="ignore")
            .strip("\n")
        )

    def __get_openmpi_prefix():
        if platform.system() == "Linux":
            # install "/opt/openmpi" in gsruntime image
            return "/opt/openmpi"
        elif platform.system() == "Darwin":
            return __get_homebrew_prefix("openmpi")
        else:
            __unknown_platform("find openmpi", platform.system())

    def __get_vineyard_prefix():
        if platform.system() == "Linux":
            return GRAPHSCOPE_HOME
        elif platform.system() == "Darwin":
            return __get_homebrew_prefix("vineyard")
        else:
            __unknown_platform("find vineyard", platform.system())

    def __get_lib_suffix():
        if platform.system() == "Linux":
            return "so"
        elif platform.system() == "Darwin":
            return "dylib"
        else:
            __unknown_platform("resolve lib suffix", platform.system())

    name = os.environ.get("package_name", "gs-coordinator")
    RUNTIME_ROOT = "graphscope.runtime"

    data = {}

    # data format:
    #   {"source_dir": "package_dir"} or
    #   {"source_dir": (package_dir, [exclude_list])}
    if name == "graphscope":
        # engine and lib
        data = {
            f"{INSTALL_PREFIX}/bin/": os.path.join(RUNTIME_ROOT, "bin"),
            f"{INSTALL_PREFIX}/lib/": os.path.join(RUNTIME_ROOT, "lib"),
            f"{INSTALL_PREFIX}/lib64/": os.path.join(RUNTIME_ROOT, "lib64"),
            f"{__get_vineyard_prefix()}/lib/libvineyard_internal_registry.{__get_lib_suffix()}": os.path.join(
                RUNTIME_ROOT, "lib"
            ),
        }
        # openmpi
        data.update(
            {
                __get_openmpi_prefix(): os.path.join(RUNTIME_ROOT),
            }
        )
    elif name == "gs-engine":
        data = {
            f"{INSTALL_PREFIX}/conf/": os.path.join(RUNTIME_ROOT, "conf"),
            f"{INSTALL_PREFIX}/*.jar": os.path.join(RUNTIME_ROOT),
        }
    elif name == "gs-include":
        data = {
            f"{INSTALL_PREFIX}/include/": os.path.join(RUNTIME_ROOT, "include"),
            f"{GRAPHSCOPE_HOME}/include/grape": os.path.join(RUNTIME_ROOT, "include"),
            f"{GRAPHSCOPE_HOME}/include/string_view": os.path.join(
                RUNTIME_ROOT, "include"
            ),
            f"{__get_vineyard_prefix()}/include/vineyard": os.path.join(
                RUNTIME_ROOT, "include"
            ),
            f"{GRAPHSCOPE_HOME}/include/arrow": os.path.join(RUNTIME_ROOT, "include"),
            f"{GRAPHSCOPE_HOME}/include/boost": os.path.join(RUNTIME_ROOT, "include"),
            f"{GRAPHSCOPE_HOME}/include/glog": os.path.join(RUNTIME_ROOT, "include"),
            f"{GRAPHSCOPE_HOME}/include/gflags": os.path.join(RUNTIME_ROOT, "include"),
            f"{GRAPHSCOPE_HOME}/include/google": os.path.join(RUNTIME_ROOT, "include"),
        }
        if platform.system() == "Linux":
            data["/usr/include/rapidjson"] = os.path.join(RUNTIME_ROOT, "include")
            data["/usr/include/msgpack"] = os.path.join(RUNTIME_ROOT, "include")
            data["/usr/include/msgpack.hpp"] = os.path.join(RUNTIME_ROOT, "include")
        elif platform.system() == "Darwin":
            data["/usr/local/include/rapidjson"] = os.path.join(RUNTIME_ROOT, "include")
            data["/usr/local/include/msgpack"] = os.path.join(RUNTIME_ROOT, "include")
            data["/usr/local/include/msgpack.hpp"] = os.path.join(
                RUNTIME_ROOT, "include"
            )
    elif name == "gs-apps":
        # precompiled applications
        data = {
            os.path.join("/", tempfile.gettempprefix(), "gs", "builtin"): os.path.join(
                RUNTIME_ROOT, "precompiled"
            ),
        }
    return data


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
            subprocess.check_call([sys.executable, "-m", "isort", "."], cwd=repo_root)
            subprocess.check_call([sys.executable, "-m", "black", "."], cwd=repo_root)
            subprocess.check_call([sys.executable, "-m", "flake8", "."], cwd=repo_root)
        else:
            subprocess.check_call(
                [sys.executable, "-m", "isort", "--check", "--diff", "."], cwd=repo_root
            )
            subprocess.check_call(
                [sys.executable, "-m", "black", "--check", "--diff", "."], cwd=repo_root
            )
            subprocess.check_call([sys.executable, "-m", "flake8", "."], cwd=repo_root)


class CustomBuildPy(build_py):
    def _get_data_files(self):
        """Add custom out-of-tree package data files."""
        rs = super()._get_data_files()

        if os.environ.get("WITH_EXTRA_DATA") != "ON":
            return rs

        for sources, package in _get_extra_data().items():
            excludes = []
            if isinstance(package, tuple):
                excludes = package[1]
                package = package[0]
            src_dir = os.path.dirname(sources)
            build_dir = os.path.join(*([self.build_lib] + package.split(os.sep)))
            filenames = []
            for file in itertools.chain(
                glob.glob(sources + "/**/*", recursive=True),
                glob.glob(sources, recursive=False),
            ):
                if os.path.isfile(file) or (
                    os.path.islink(file) and not os.path.isdir(file)
                ):
                    if not any([f in file for f in excludes]):
                        filenames.append(os.path.relpath(file, src_dir))
            rs.append((package, src_dir, build_dir, filenames))
        return rs

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
    os.path.join(os.path.abspath(os.path.dirname(__file__)), "..", "README.md"),
    encoding="utf-8",
    mode="r",
) as fp:
    long_description = fp.read()


def parsed_package_dir():
    name = os.environ.get("package_name", "gs-coordinator")
    if name == "gs-coordinator":
        return {".": "."}
    return {}


def parsed_packages():
    name = os.environ.get("package_name", "gs-coordinator")
    if name == "gs-coordinator":
        return find_packages(".")
    return ["graphscope_runtime"]


def parsed_package_data():
    name = os.environ.get("package_name", "gs-coordinator")
    if name == "gs-coordinator":
        return {
            "gscoordinator": [
                "builtin/app/builtin_app.gar",
                "builtin/app/*.yaml",
                "template/*.template",
                "VERSION",
            ],
        }
    return {}


def parsed_reqs():
    name = os.environ.get("package_name", "gs-coordinator")
    if name == "gs-coordinator":
        with open(
            os.path.join(repo_root, "requirements.txt"), "r", encoding="utf-8"
        ) as fp:
            return fp.read().splitlines()
    elif name == "graphscope":
        return GRAPHSCOPE_REQUIRED_PACKAGES
    else:
        return []


def parsed_dev_reqs():
    name = os.environ.get("package_name", "gs-coordinator")
    if name == "gs-coordinator":
        with open(
            os.path.join(repo_root, "requirements-dev.txt"), "r", encoding="utf-8"
        ) as fp:
            return {"dev": fp.read().splitlines()}
    return {}


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


setup(
    name=os.environ.get("package_name", "gs-coordinator"),
    description="",
    include_package_data=True,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="GraphScope Team, Damo Academy",
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
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    keywords="GraphScope, Graph Computations",
    use_scm_version={
        "root": repo_root,
        "parse": parse_version,
    },
    setup_requires=[
        "setuptools_scm>=5.0.0",
    ],
    package_dir=parsed_package_dir(),
    packages=parsed_packages(),
    package_data=parsed_package_data(),
    cmdclass={
        "build_builtin": BuildBuiltin,
        "build_py": CustomBuildPy,
        "sdist": CustomSDist,
        "develop": CustomDevelop,
        "lint": FormatAndLint,
    },
    install_requires=parsed_reqs(),
    extras_require=parsed_dev_reqs(),
)


if os.name == "nt":

    class _ReprableString(str):
        def __repr__(self) -> str:
            return self

    raise RuntimeError(
        _ReprableString(
            """
            ====================================================================

            GraphScope doesn't support Windows natively, please try to install graphscope in WSL

                https://docs.microsoft.com/en-us/windows/wsl/install

            with pip.

            ===================================================================="""
        )
    )
