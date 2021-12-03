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


def get_lib_suffix():
    suffix = ""
    if platform.system() == "Linux":
        suffix = "so"
    elif platform.system() == "Darwin":
        suffix = "dylib"
    else:
        raise RuntimeError("Get library suffix failed on {0}".format(platform.system()))
    return suffix


repo_root = os.path.dirname(os.path.abspath(__file__))
version = get_version(os.path.join(repo_root, "..", "VERSION"))


GRAPHSCOPE_REQUIRED_PACKAGES = [
    f"gs-coordinator >= {version}",
    f"gs-jython >= {version}",
    f"gs-lib >= {version}",
    f"gs-engine >= {version}",
    f"gs-include >= {version}",
]


# copy any files contains in /opt/graphscope into site-packages/graphscope.runtime
def _get_extra_data():
    # Copy
    #   1) /opt/graphscope
    #   2) headers of arrow/glog/gflags/google/openmpi/vineyard
    #   3) openmpi
    #   4) zetcd
    #   5) /tmp/gs/builtin
    # into site-packages/graphscope.runtime
    #
    #  For shrink the package size less than "100M", we split graphscope into
    #   1) gs-coordinator: include python releated code of gscoordinator
    #   2) gs-lib: libs exclude jython-standalone/groovy/grpc/curator/hadoop/gremlin/conscrypt**.jar
    #   3) gs-jython: other libs not included in gs-lib
    #   5) gs-include: header files and full-openmpi
    #   4) gs-engine: other runtime info such as 'bin', 'conf'

    def __get_openmpi_prefix():
        openmpi_prefix = ""
        if platform.system() == "Linux":
            # install "/opt/openmpi" in gsruntime image
            openmpi_prefix = "/opt/openmpi"
        elif platform.system() == "Darwin":
            openmpi_prefix = (
                subprocess.check_output([shutil.which("brew"), "--prefix", "openmpi"])
                .decode("utf-8")
                .strip("\n")
            )
        else:
            raise RuntimeError(
                "Get openmpi prefix failed on {0}".format(platform.system())
            )
        return openmpi_prefix

    name = os.environ.get("package_name", "gs-coordinator")
    RUNTIME_ROOT = "graphscope.runtime"

    data = {}

    # data format:
    #   {"source_dir": "package_dir"} or
    #   {"source_dir": (package_dir, [exclude_list])}
    if name == "gs-lib":
        # exclude jython-standalone-**.jar
        data = {
            "/opt/graphscope/lib/": (
                os.path.join(RUNTIME_ROOT, "lib"),
                # exclude lists
                [
                    "jython-standalone",
                    "groovy",
                    "grpc",
                    "curator",
                    "hadoop",
                    "gremlin",
                    "conscrypt",
                ],
            ),
            "/usr/local/lib/libvineyard_internal_registry.{0}".format(
                get_lib_suffix()
            ): os.path.join(RUNTIME_ROOT, "lib"),
        }
    elif name == "gs-jython":
        data = {
            "/opt/graphscope/lib/jython-standalone*.jar": os.path.join(
                RUNTIME_ROOT, "lib"
            ),
            "/opt/graphscope/lib/*groovy*.jar": os.path.join(RUNTIME_ROOT, "lib"),
            "/opt/graphscope/lib/*grpc*.jar": os.path.join(RUNTIME_ROOT, "lib"),
            "/opt/graphscope/lib/*curator*.jar": os.path.join(RUNTIME_ROOT, "lib"),
            "/opt/graphscope/lib/*hadoop*.jar": os.path.join(RUNTIME_ROOT, "lib"),
            "/opt/graphscope/lib/*gremlin*.jar": os.path.join(RUNTIME_ROOT, "lib"),
            "/opt/graphscope/lib/*conscrypt*.jar": os.path.join(RUNTIME_ROOT, "lib"),
        }
    elif name == "gs-engine":
        data = {
            "/opt/graphscope/bin/": os.path.join(RUNTIME_ROOT, "bin"),
            "/opt/graphscope/conf/": os.path.join(RUNTIME_ROOT, "conf"),
            "/opt/graphscope/lib64/": os.path.join(RUNTIME_ROOT, "lib64"),
            "/opt/graphscope/*.jar": os.path.join(RUNTIME_ROOT),
            "/usr/local/bin/zetcd": os.path.join(RUNTIME_ROOT, "bin"),
        }
    elif name == "gs-include":
        data = {
            "/opt/graphscope/include/": os.path.join(RUNTIME_ROOT, "include"),
            "/usr/local/include/grape": os.path.join(RUNTIME_ROOT, "include"),
            "/opt/vineyard/include/": os.path.join(RUNTIME_ROOT, "include"),
            "/usr/local/include/arrow": os.path.join(RUNTIME_ROOT, "include"),
            "/usr/local/include/boost": os.path.join(RUNTIME_ROOT, "include"),
            "/usr/local/include/double-conversion": os.path.join(
                RUNTIME_ROOT, "include"
            ),
            "/usr/local/include/folly": os.path.join(RUNTIME_ROOT, "include"),
            "/usr/local/include/glog": os.path.join(RUNTIME_ROOT, "include"),
            "/usr/local/include/gflags": os.path.join(RUNTIME_ROOT, "include"),
            "/usr/local/include/google": os.path.join(RUNTIME_ROOT, "include"),
        }
        # precompiled
        data.update(
            {
                os.path.join(
                    "/", tempfile.gettempprefix(), "gs", "builtin"
                ): os.path.join(RUNTIME_ROOT, "precompiled"),
            }
        )
        # openmpi
        data.update(
            {
                __get_openmpi_prefix(): os.path.join(RUNTIME_ROOT),
            }
        )
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
    return ["foo"]


def parsed_packge_data():
    name = os.environ.get("package_name", "gs-coordinator")
    if name == "gs-coordinator":
        return {
            "gscoordinator": [
                "builtin/app/builtin_app.gar",
                "builtin/app/*.yaml",
                "template/*.template",
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
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    keywords="GraphScope, Graph Computations",
    use_scm_version={
        "root": repo_root,
        "parse": parse_version,
        "write_to": os.path.join(repo_root, "gscoordinator/version.py"),
        "write_to_template": version_template,
    },
    setup_requires=["setuptools_scm>=5.0.0", "grpcio", "grpcio-tools"],
    package_dir=parsed_package_dir(),
    packages=parsed_packages(),
    package_data=parsed_packge_data(),
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
