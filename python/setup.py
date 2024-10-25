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
import platform
import shutil
import site
import subprocess
import sys
import tempfile
from distutils.cmd import Command

from setuptools import Extension
from setuptools import find_packages
from setuptools import setup
from setuptools.command.build_ext import build_ext
from setuptools.command.build_py import build_py
from setuptools.command.develop import develop
from setuptools.command.sdist import sdist
from wheel.bdist_wheel import bdist_wheel

try:
    import torch
    import torch.utils.cpp_extension
except ImportError:
    torch = None

# Enables --editable install with --user
# https://github.com/pypa/pip/issues/7953
site.ENABLE_USER_SITE = "--user" in sys.argv[1:]

pkg_root = os.path.dirname(os.path.abspath(__file__))

if platform.system() == "Darwin":
    # see also: https://github.com/python/cpython/issues/100420
    if "arm" in platform.processor().lower():
        os.environ["ARCHFLAGS"] = "-arch arm64"
    else:
        os.environ["ARCHFLAGS"] = "-arch x86_64"

GL_EXT_NAME = "graphscope.learning.graphlearn.pywrap_graphlearn"
GLTORCH_EXT_NAME = "graphscope.learning.graphlearn_torch.py_graphlearn_torch"
GLTORCH_V6D_EXT_NAME = (
    "graphscope.learning.graphlearn_torch.py_graphlearn_torch_vineyard"
)
glt_root_path = os.path.abspath(
    os.path.join(pkg_root, "..", "learning_engine", "graphlearn-for-pytorch")
)


class BuildProto(Command):
    description = "build protobuf file"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        cmd = [
            sys.executable,
            os.path.join(
                pkg_root,
                "..",
                "proto",
                "proto_generator.py",
            ),
            os.path.join(pkg_root, "graphscope", "proto"),
            "--python",
        ]
        print(" ".join(cmd))
        subprocess.check_call(
            cmd,
            env=os.environ.copy(),
        )


class GenerateFlexSDK(Command):
    description = "generate flex client sdk from openapi specification file"
    user_options = [("with-doc", None, "Include documentation")]

    def initialize_options(self):
        self.with_doc = False

    def finalize_options(self):
        pass

    def run(self):
        # remove
        tempdir = os.path.join("/", tempfile.gettempprefix(), "flex_client")
        if os.path.exists(tempdir):
            shutil.rmtree(tempdir)
        targetdir = os.path.join(pkg_root, "graphscope", "flex", "rest")
        if os.path.exists(targetdir):
            shutil.rmtree(targetdir)
        # generate
        specification = os.path.join(
            pkg_root, "..", "flex", "openapi", "openapi_coordinator.yaml"
        )
        cmd = [
            "openapi-generator",
            "generate",
            "-g",
            "python",
            "-i",
            str(specification),
            "-o",
            str(tempdir),
            "--package-name",
            "graphscope.flex.rest",
        ]
        print(" ".join(cmd))
        env = os.environ.copy()
        env["OPENAPI_GENERATOR_VERSION"] = "7.3.0"
        subprocess.check_call(
            cmd,
            env=env,
        )
        # cp
        subprocess.run(
            ["cp", "-r", os.path.join(tempdir, "graphscope", "flex", "rest"), targetdir]
        )
        if self.with_doc:
            targetdir = os.path.join(
                pkg_root, "..", "docs", "flex", "coordinator", "development", "python"
            )
            subprocess.run(
                [
                    "sed",
                    "-i",
                    "s/# graphscope.flex.rest/# Coordinator Python SDK Reference/",
                    os.path.join(tempdir, "README.md"),
                ]
            )
            subprocess.run(["cp", os.path.join(tempdir, "README.md"), targetdir])
            subprocess.run(["cp", "-r", os.path.join(tempdir, "docs"), targetdir])


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
            subprocess.check_call([sys.executable, "-m", "isort", "."], cwd=pkg_root)
            subprocess.check_call([sys.executable, "-m", "black", "."], cwd=pkg_root)
            subprocess.check_call([sys.executable, "-m", "flake8", "."], cwd=pkg_root)
        else:
            subprocess.check_call(
                [sys.executable, "-m", "isort", "--check", "--diff", "."], cwd=pkg_root
            )
            subprocess.check_call(
                [sys.executable, "-m", "black", "--check", "--diff", "."], cwd=pkg_root
            )
            subprocess.check_call([sys.executable, "-m", "flake8", "."], cwd=pkg_root)


class CustomBuildPy(build_py):
    def run(self):
        self.run_command("build_proto")
        build_py.run(self)


class BuildGLExt(build_ext):
    def run(self):
        self.extensions = [ext for ext in self.extensions if ext.name == GL_EXT_NAME]
        self.run_command("build_proto")
        build_ext.run(self)


class BuildGLTorchExt(torch.utils.cpp_extension.BuildExtension if torch else build_ext):
    def run(self):
        if torch is None:
            print("Building graphlearn-torch extension requires pytorch")
            print("Set WITH_GLTORCH=OFF if you don't need it.")
            return
        self.extensions = [
            ext
            for ext in self.extensions
            if ext.name in [GLTORCH_EXT_NAME, GLTORCH_V6D_EXT_NAME]
        ]
        torch.utils.cpp_extension.BuildExtension.run(self)

    def _get_gcc_use_cxx_abi(self):
        if hasattr(self, "_gcc_use_cxx_abi"):
            return self._gcc_use_cxx_abi
        build_dir = os.path.join(glt_root_path, "cmake-build")
        os.makedirs(build_dir, exist_ok=True)
        output = subprocess.run(
            [shutil.which("cmake"), ".."],
            cwd=build_dir,
            capture_output=True,
            text=True,
        )
        import re

        match = re.search(r"GCC_USE_CXX11_ABI: (\d)", str(output))
        if match:
            self._gcc_use_cxx_abi = match.group(1)
        else:
            return None

        return self._gcc_use_cxx_abi

    def _add_gnu_cpp_abi_flag(self, extension):
        gcc_use_cxx_abi = (
            self._get_gcc_use_cxx_abi()
            if extension.name == GLTORCH_V6D_EXT_NAME
            else str(int(torch._C._GLIBCXX_USE_CXX11_ABI))
        )
        print(f"GCC_USE_CXX11_ABI for {extension.name}: {gcc_use_cxx_abi}")
        if gcc_use_cxx_abi is not None:
            self._add_compile_flag(
                extension, "-D_GLIBCXX_USE_CXX11_ABI=" + gcc_use_cxx_abi
            )


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
        bdist_wheel.finalize_options(self)
        self.root_is_pure = False

    def run(self):
        if sys.platform == "darwin":
            graphlearn_shared_lib = "libgraphlearn_shared.dylib"
        else:
            graphlearn_shared_lib = "libgraphlearn_shared.so"
        if os.environ.get("WITHOUT_LEARNING_ENGINE", None) is None:
            if not os.path.isfile(
                os.path.join(
                    pkg_root,
                    "..",
                    "learning_engine",
                    "graph-learn",
                    "graphlearn",
                    "built",
                    "lib",
                    graphlearn_shared_lib,
                )
            ):
                raise ValueError("You must build the graphlearn library at first")
        self.run_command("build_proto")
        bdist_wheel.run(self)


with open(os.path.join(pkg_root, "..", "README.md"), "r", encoding="utf-8") as fp:
    long_description = fp.read()


def parsed_reqs():
    with open(os.path.join(pkg_root, "requirements.txt"), "r", encoding="utf-8") as fp:
        return fp.read().splitlines()


def parsed_dev_reqs():
    with open(
        os.path.join(pkg_root, "requirements-dev.txt"), "r", encoding="utf-8"
    ) as fp:
        return fp.read().splitlines()


def find_graphscope_packages():
    packages = []

    # add graphscope
    for pkg in find_packages(
        ".", exclude=["graphscope.flex.*", "graphscope.gsctl", "graphscope.gsctl.*"]
    ):
        packages.append(pkg)

    return packages


def resolve_graphscope_package_dir():
    package_dir = {
        "graphscope": "graphscope",
    }
    return package_dir


def parsed_package_data():
    return {
        "graphscope": [
            "VERSION",
            "proto/*.pyi",
        ],
    }


def build_learning_engine():
    if os.environ.get("WITHOUT_LEARNING_ENGINE", None) is not None:
        return []

    ext_modules = [graphlearn_ext()]
    if torch and os.path.exists(os.path.join(glt_root_path, "graphlearn_torch")):
        sys.path.insert(
            0, os.path.join(glt_root_path, "graphlearn_torch", "python", "utils")
        )
        from build_glt import glt_ext_module
        from build_glt import glt_v6d_ext_module

        ext_modules.append(
            glt_ext_module(
                name=GLTORCH_EXT_NAME,
                root_path=glt_root_path,
                with_cuda=False,
                release=False,
            )
        )
        ext_modules.append(
            glt_v6d_ext_module(
                name=GLTORCH_V6D_EXT_NAME,
                root_path=glt_root_path,
            )
        )
        sys.path.pop(0)
    return ext_modules


def graphlearn_ext():
    import numpy

    ROOT_PATH = os.path.abspath(
        os.path.join(pkg_root, "..", "learning_engine", "graph-learn")
    )

    include_dirs = []
    library_dirs = []
    libraries = []
    extra_compile_args = []
    extra_link_args = []
    if "GRAPHSCOPE_HOME" in os.environ:
        include_dirs.append(os.environ["GRAPHSCOPE_HOME"] + "/include")
    include_dirs.append("/usr/local/include")
    include_dirs.append(ROOT_PATH)
    include_dirs.append(ROOT_PATH + "/graphlearn")
    include_dirs.append(ROOT_PATH + "/graphlearn/src")
    include_dirs.append(ROOT_PATH + "/graphlearn/src/include")
    include_dirs.append(ROOT_PATH + "/graphlearn/built")
    include_dirs.append(ROOT_PATH + "/third_party/pybind11/pybind11/include")
    include_dirs.append(ROOT_PATH + "/third_party/glog/build")
    include_dirs.append(ROOT_PATH + "/third_party/protobuf/build/include")
    include_dirs.append(numpy.get_include())
    library_dirs.append(ROOT_PATH + "/graphlearn/built/lib")

    extra_compile_args.append("-D__USE_XOPEN2K8")
    extra_compile_args.append("-std=c++17")
    extra_compile_args.append("-fvisibility=hidden")

    if sys.platform == "darwin":
        # mac M1 support
        include_dirs.append("/opt/homebrew/include")

        # explicitly link against protobuf to avoid the error
        # "illegal thread local variable reference to regular symbol"
        library_dirs.append("/usr/local/lib")
        library_dirs.append("/opt/homebrew/lib")
        libraries.append("protobuf")

    libraries.append("graphlearn_shared")
    if sys.platform == "linux" or sys.platform == "linux2":
        extra_link_args.append("-Wl,-rpath=$ORIGIN/built/lib")
    if sys.platform == "darwin":
        extra_link_args.append("-Wl,-rpath,@loader_path/built/lib")

    sources = [
        ROOT_PATH + "/graphlearn/python/c/py_client.cc",
        ROOT_PATH + "/graphlearn/python/c/py_export.cc",
        # KNN not enabled
        # ROOT_PATH + "/graphlearn/python/c/py_contrib.cc",
    ]

    return Extension(
        GL_EXT_NAME,
        sources,
        extra_compile_args=extra_compile_args,
        extra_link_args=extra_link_args,
        include_dirs=include_dirs,
        library_dirs=library_dirs,
        libraries=libraries,
    )


def parse_version(root, **kwargs):
    """
    Parse function for setuptools_scm that first tries to read '../VERSION' file
    to get a version number.
    """
    from setuptools_scm.git import parse
    from setuptools_scm.version import meta

    version_file = os.path.join(pkg_root, "..", "VERSION")
    if os.path.isfile(version_file):
        with open(version_file, "r", encoding="utf-8") as fp:
            return meta(fp.read().strip())
    return parse(root, **kwargs)


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
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    keywords="Graph, Large-Scale, Distributed Computing",
    use_scm_version={
        "root": pkg_root,
        "parse": parse_version,
    },
    setup_requires=[
        "setuptools_scm>=5.0.0,<8",
    ],
    package_dir=resolve_graphscope_package_dir(),
    packages=find_graphscope_packages(),
    package_data=parsed_package_data(),
    ext_modules=build_learning_engine(),
    cmdclass={
        "build_ext": BuildGLExt,
        "build_gltorch_ext": BuildGLTorchExt,
        "build_proto": BuildProto,
        "build_py": CustomBuildPy,
        "generate_flex_sdk": GenerateFlexSDK,
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
