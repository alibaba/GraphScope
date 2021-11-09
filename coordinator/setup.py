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


repo_root = os.path.dirname(os.path.abspath(__file__))
version = get_version(os.path.join(repo_root, "..", "VERSION"))


GRAPHSCOPE_REQUIRED_PACKAGES = [
    f"gscoordinator >= {version}",
    f"gs-jython >= {version}",
    f"gs-lib >= {version}",
    f"gs-engine >= {version}",
]


# copy any files contains in /opt/graphscope into site-packages/graphscope.runtime
def _get_extra_data():
    # Copy
    #   1) /opt/graphscope
    #   2) headers of arrow/glog/gflags/google/openmpi/vineyard
    #   3) openmpi daemon process `orted`
    #   4) zetcd
    #   5) /tmp/gs/builtin
    # into site-packages/graphscope.runtime
    #
    #  For shrink the package size less than "100M", we split graphscope into
    #   1) gscoordinator: include python releated code of gscoordinator
    #   2) gs-lib: lib dir exclude jython-standalone-**.jar
    #   3) gs-jython: only jython-standalone-**.jar, cause this jar is 40M
    #   4) gs-engine: other runtime info such as 'bin', 'conf', and 'include'

    name = os.environ.get("package_name", "gscoordinator")
    RUNTIME_ROOT = "graphscope.runtime"

    # data format:
    #   {"source_dir": "package_dir"} or
    #   {"source_dir": (package_dir, [exclude_list])}
    if name == "gs-lib":
        # exclude jython-standalone-**.jar
        data = {
            "/opt/graphscope/lib/": (
                os.path.join(RUNTIME_ROOT, "lib"),
                ["jython-standalone"],
            )
        }
    elif name == "gs-jython":
        data = {
            "/opt/graphscope/lib/jython-standalone*.jar": os.path.join(
                RUNTIME_ROOT, "lib"
            ),
        }
    elif name == "gs-engine":
        data = {
            "/opt/graphscope/bin": os.path.join(RUNTIME_ROOT, "bin"),
            "/opt/graphscope/conf": os.path.join(RUNTIME_ROOT, "conf"),
            "/opt/graphscope/include": os.path.join(RUNTIME_ROOT, "include"),
            "/usr/local/include/grape": os.path.join(RUNTIME_ROOT, "include"),
            "/opt/graphscope/lib64": os.path.join(RUNTIME_ROOT, "lib64"),
            "/opt/vineyard/include/": os.path.join(RUNTIME_ROOT, "include"),
            os.path.join("/", tempfile.gettempprefix(), "gs", "builtin"): os.path.join(
                RUNTIME_ROOT, "precompiled"
            ),
            "/usr/local/include/arrow": os.path.join(RUNTIME_ROOT, "include"),
            "/usr/local/include/boost": os.path.join(RUNTIME_ROOT, "include"),
            "/usr/local/include/double-conversion": os.path.join(
                RUNTIME_ROOT, "include"
            ),
            "/usr/local/include/folly": os.path.join(RUNTIME_ROOT, "include"),
            "/usr/local/include/glog": os.path.join(RUNTIME_ROOT, "include"),
            "/usr/local/include/gflags": os.path.join(RUNTIME_ROOT, "include"),
            "/usr/local/include/google": os.path.join(RUNTIME_ROOT, "include"),
            "/usr/local/include/mpi*.h": os.path.join(RUNTIME_ROOT, "include"),
            "/usr/local/include/openmpi": os.path.join(RUNTIME_ROOT, "include"),
            "/usr/local/bin/orted": os.path.join(RUNTIME_ROOT, "bin"),
            "/usr/local/bin/zetcd": os.path.join(RUNTIME_ROOT, "bin"),
        }
        # MacOS: Some openmpi libs need to be dlopen
        if platform.system() == "Darwin":
            data.update(
                {"/usr/local/opt/open-mpi/lib/": os.path.join(RUNTIME_ROOT, "lib")}
            )
    else:
        data = {}
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
    os.path.join(os.path.abspath(os.path.dirname(__file__)), "README.md"),
    encoding="utf-8",
    mode="r",
) as fp:
    long_description = fp.read()


def parsed_package_dir():
    name = os.environ.get("package_name", "gscoordinator")
    if name == "gscoordinator":
        return {".": "."}
    return {}


def parsed_packages():
    name = os.environ.get("package_name", "gscoordinator")
    if name == "gscoordinator":
        return find_packages(".")
    return ["foo"]


def parsed_packge_data():
    name = os.environ.get("package_name", "gscoordinator")
    if name == "gscoordinator":
        return {
            "gscoordinator": [
                "builtin/app/builtin_app.gar",
                "builtin/app/*.yaml",
                "template/*.template",
            ],
        }
    return {}


def parsed_reqs():
    name = os.environ.get("package_name", "gscoordinator")
    if name == "gscoordinator":
        with open(
            os.path.join(repo_root, "requirements.txt"), "r", encoding="utf-8"
        ) as fp:
            return fp.read().splitlines()
    elif name == "graphscope":
        return GRAPHSCOPE_REQUIRED_PACKAGES
    else:
        return []


def parsed_dev_reqs():
    name = os.environ.get("package_name", "gscoordinator")
    if name == "gscoordinator":
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
    name=os.environ.get("package_name", "gscoordinator"),
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
