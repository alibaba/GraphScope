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

"""Utility module to manage graphs and apps
"""

from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division

import argparse
import io
import json
import sys
import os
import re
import subprocess
import requests
import logging
import shutil

from string import Template

import yaml
import zipfile

from six.moves.urllib_request import urlretrieve
from six.moves.urllib.error import HTTPError
from six.moves.urllib.error import URLError

from prompt_toolkit import prompt, HTML
from prompt_toolkit.completion import PathCompleter
from prompt_toolkit.validation import Validator
from prompt_toolkit.styles import Style

# pylint: disable=too-many-lines
# global specification
# grape_home must be absolute path.


def _expand_path(path):
    """Utility function to expand a path."""
    return os.path.abspath(os.path.expanduser(os.path.expandvars(path.strip())))


ANALYTICAL_ENGINE_HOME = os.path.join("/root/gsa/analytical_engine")
CURRENT_HOME = os.path.abspath(os.path.dirname(__file__))
WORKSPACE = "/tmp"
TEMPLATE_DIR = os.path.join(CURRENT_HOME, "../coordinator/gscoordinator/template")
_PYTHON_DATA_TYPES = ["int", "double", "string"]
_PREGEL_COMBINE = ["false", "true"]
_APP_TYPES = ["cpp_pie", "cython_pie", "cython_pregel"]
_GRAPH_TYPES = [
    "vineyard::ArrowFragment",
    "gs::ArrowProjectedFragment",
    "gs::DynamicProjectedFragment",
    "Done",
]


def package_application(source_files):
    files = source_files.split(",")
    base_file_names = [os.path.basename(f) for f in files]
    factory = PromptFactory()
    app_name = factory.request_name("app name")
    app_type = factory.request(_APP_TYPES, "app type", 0)
    print("You selected %s.\n" % app_type)

    if app_type == "cpp_pie":
        class_name = factory.request_class_name("class name")
    else:
        class_name = "gs::PregelPropertyAppBase"

    src = factory.request_source_file(base_file_names, "the file as source", 0)

    if app_type != "cpp_pie":
        vd_type = factory.request(_PYTHON_DATA_TYPES, "pregel vertex data type", 0)
        print("You selected %s.\n" % vd_type)

        md_type = factory.request(_PYTHON_DATA_TYPES, "pregel message data type", 0)
        print("You selected %s.\n" % md_type)
        if app_type == "cython_pregel":
            pregel_combine = factory.request(
                _PREGEL_COMBINE, "enable pregel combinator", 0
            )
            print("You selected %s.\n" % pregel_combine)
        else:
            pregel_combine = "false"
    else:
        vd_type = ""
        md_type = ""
        pregel_combine = "false"

    compatible_graph = []
    while True:
        graph_type = factory.request_compatible_graphs(
            _GRAPH_TYPES, "compatible graph types", 3
        )
        if graph_type == "Done":
            break
        compatible_graph.append(graph_type)

    output_path = factory.request_path("output path")

    gs_config = {
        "app": [
            {
                "algo": app_name,
                "type": app_type,
                "class_name": class_name,
                "src": src,
                "compatible_graph": compatible_graph,
                "vd_type": vd_type,
                "md_type": md_type,
                "pregel_combine": True if pregel_combine == "true" else False,
                "output_path": output_path,
            }
        ]
    }

    with zipfile.ZipFile(app_name + ".gar", "w") as gar:
        for filename in files:
            gar.write(filename, os.path.basename(filename))
        gar.writestr(".gs_conf.yaml", yaml.dump(gs_config))

    attrs = gs_config["app"][0]

    return app_name + ".gar", attrs


def check_compile(gar_file, attrs):
    # pull and comple the gar file
    app_dir = os.path.join(WORKSPACE, "check_compile")
    os.makedirs(app_dir, exist_ok=True)
    with zipfile.ZipFile(gar_file, "r") as zip_ref:
        zip_ref.extractall(app_dir)

    # replace and generate cmakelist
    cmake_commands = "cmake . "
    module_name = ""
    app_type = attrs["type"]
    if app_type == "cpp_pie":
        graph_type = "gs::ArrowProjectedFragment<int64_t,uint64_t,int,double>"
        app_class = "{}<_GRAPH_TYPE>".format(attrs["class_name"])
        graph_header = "core/fragment/arrow_projected_fragment.h"
        app_header = attrs["src"]
    else:
        graph_type = "vineyard::ArrowFragment<int64_t,uint64_t>"
        app_class = ""
        graph_header = "core/fragment/arrow_fragment.h"
        if app_type == "cython_pregel":
            pxd_name = "pregel"
            cmake_commands += "-DCYTHON_PREGEL_APP=True"
            if attrs["pregel_combine"]:
                cmake_commands += "-DENABLE_PREGEL_COMBINE=True"
        else:
            pxd_name = "pie"
            cmake_commands += "-DCYTHON_PIE_APP=True"
        shutil.copyfile(
            os.path.join(TEMPLATE_DIR, "{}.pxd.template".format(pxd_name)),
            os.path.join(app_dir, "{}.pxd".format(pxd_name)),
        )
        module_name = os.path.splitext(attrs["src"])[0]
        pyx_file = os.path.join(app_dir, attrs["src"])
        cc_file = os.path.join(app_dir, module_name + ".cc")
        subprocess.check_call(["cython", "-3", "--cplus", "-o", cc_file, pyx_file])
        app_header = "{}.h".format(module_name)

    # replace and generate cmakelist
    cmakelists_file_tmp = os.path.join(TEMPLATE_DIR, "CMakeLists.template")
    cmakelists_file = os.path.join(app_dir, "CMakeLists.txt")
    with open(cmakelists_file_tmp, mode="r") as template:
        content = template.read()
        content = Template(content).safe_substitute(
            _analytical_engine_home=ANALYTICAL_ENGINE_HOME,
            _frame_name=attrs["algo"],
            _vd_type=attrs["vd_type"],
            _md_type=attrs["md_type"],
            _graph_type=graph_type,
            _graph_header=graph_header,
            _module_name=module_name,
            _app_type=app_class,
            _app_header=app_header,
        )
        with open(cmakelists_file, mode="w") as f:
            f.write(content)

    # compile
    compile_cmd = (
        "set pipefail && cd /tmp/check_compile && " + cmake_commands + " && make -j"
    )
    docker_run_cmd = [
        "sudo",
        "docker",
        "run",
        "--rm",
        "-v",
        "/tmp/check_compile:/tmp/check_compile",
        "registry.cn-hongkong.aliyuncs.com/graphscope/graphscope",
        "sh",
        "-c",
        compile_cmd,
    ]
    clean_cmd = [
        "sudo",
        "docker",
        "run",
        "--rm",
        "-v",
        "/tmp/check_compile:/tmp/check_compile",
        "registry.cn-hongkong.aliyuncs.com/graphscope/graphscope",
        "sh",
        "-c",
        "rm -fr /tmp/check_compile/*",
    ]
    try:
        proc = subprocess.check_output(docker_run_cmd, stderr=sys.stdout)
        logging.info("Compile successful.")
    except subprocess.CalledProcessError as e:
        subprocess.check_output(clean_cmd, stderr=sys.stdout)
        os.remove(gar_file)
        raise RuntimeError("Compile failed.") from e

    subprocess.check_output(clean_cmd, stderr=sys.stdout)


class ValidatorCollection(object):
    """Collection of all validators."""

    def __init__(self, valid_types=None):
        self._valid_types = valid_types

    def get_confirm_validator(self):
        """Return a validator for confirmation y or n."""
        return Validator.from_callable(
            self._is_valid_confirm,
            error_message="Please enter 'y' or 'n' to continue.",
            move_cursor_to_end=True,
        )

    def get_type_validator(self):
        """Return a validator for a valid type."""
        return Validator.from_callable(
            self._is_valid_class_name,
            error_message="Please only enter a number [0-%s]. "
            % (len(self._valid_types) - 1),
            move_cursor_to_end=True,
        )

    def get_path_validator(self):
        """Return a validator for a valid path."""
        return Validator.from_callable(
            self._is_valid_path,
            error_message="Please enter a valid path.",
            move_cursor_to_end=True,
        )

    def get_name_validator(self):
        """Return a validator for a valid identifier."""
        return Validator.from_callable(
            self._is_valid_name,
            error_message="Please enter a valid name (only contain digits and letters).",
            move_cursor_to_end=True,
        )

    def get_class_name_validator(self):
        """Return a validator for a valid identifier."""
        return Validator.from_callable(
            self._is_valid_class_name,
            error_message="Please enter a valid name (only contain digits and letters).",
            move_cursor_to_end=True,
        )

    def get_filepath_validator(self):
        """Return a validator for a existing file path."""
        Validator.from_callable(
            self._is_valid_file_path,
            error_message="Please enter a valid file path.",
            move_cursor_to_end=True,
        )

    @staticmethod
    def _is_valid_confirm(text):
        if text:
            return text in ["y", "n"]
        return False

    @staticmethod
    def _is_valid_type(text):
        if text:
            return text in [str(i) for i in range(len(self._valid_types))]
        return True

    @staticmethod
    def _is_valid_path(text):
        if text:
            return True
        return False

    @staticmethod
    def _is_valid_name(text):
        return re.match("[_A-Za-z][_a-zA-Z0-9]*$", text)

    @staticmethod
    def _is_valid_class_name(text):
        return True

    @staticmethod
    def _is_valid_file_path(text):
        if text:
            fpath = os.path.abspath(os.path.expanduser(text))
            return os.access(fpath, os.R_OK)
        return False


class CompleterCollection(object):
    """Collection of all completers."""

    # pylint: disable=too-few-public-methods
    def __init__(self):
        pass

    @staticmethod
    def get_path_completer():
        """Complete path info while typing."""
        return PathCompleter(expanduser=True)


class PromptFactory(object):
    """Generate various kind of prompt message."""

    def __init__(self):
        self.styles = Style.from_dict(
            {
                # user input (default text).
                # '': '#00ffff bg:#444400',
                "type": "#44ff00",
                "error": "#ff0066",
                "path": "ansicyan underline",
                "request": "skyblue",
                "keyword": "#cc7832 bold",
            }
        )
        self._prompt_hint = "Please select/enter <keyword>%s</keyword>: "
        self._type_hint = "<type>[%s] %s</type> %s"
        self._confirm_hint = (
            "\nThe config for the <keyword>%s</keyword> ("
            "press <bold>Enter</bold> to use default <type>[%s] %s</type>): "
        )
        self._clean_hint = (
            "There is already an instance named <keyword>%s</keyword>,"
            " clean previous one? y/n: "
        )

    @staticmethod
    def _request_wrapper(text):
        """Wrap style around text.
        e.g. add request style: return <request>%s</request>' % text
        """
        return text

    def request_template(self, text):
        """Prompt for a input for 'text' with 'text' wrapped in <keyword> style
        e.g. 'Please enter your name'
        """
        return self._request_wrapper(self._prompt_hint) % text

    def type_template(self, types, help_info=None):
        """list all available method type in a style."""
        if help_info is None:
            help_info = [""] * len(types)
        return "\n".join(
            [self._type_hint % (i, types[i], help_info[i]) for i in range(len(types))]
        )

    def selection_template(self, type_repr, types, default_index):
        """Ask user to choose a number"""
        return self._confirm_hint % (type_repr, default_index, types[default_index])

    def request(self, types, type_repr, default_index):
        """First prompt for a type, then list all types,
        then ask user choose a number

        :param types: available types
        :param type_repr: type name
        :param default_index: default type to use when only press Enter
        :return: selected type
        """
        validator = ValidatorCollection(types)
        pre_hint = self.request_template(type_repr)
        post_hint = self.selection_template(type_repr, types, default_index)
        hint_message = pre_hint + "\n" + self.type_template(types) + post_hint
        ret = prompt(
            HTML(hint_message),
            style=self.styles,
            validator=validator.get_type_validator(),
            validate_while_typing=False,
        )
        # validator promise the ret can be transform to int
        return types[int(ret)] if ret else types[default_index]

    def request_path(self, path_repr):
        """Prompt to ask for a path."""
        validator = ValidatorCollection()
        ret = prompt(
            HTML(self.request_template(path_repr)),
            style=self.styles,
            completer=CompleterCollection.get_path_completer(),
            complete_while_typing=True,
            validator=validator.get_path_validator(),
            validate_while_typing=False,
        )
        return ret

    def request_name(self, name_repr):
        """Prompt to ask for a name."""
        validator = ValidatorCollection()
        ret = prompt(
            HTML(self.request_template(name_repr)),
            style=self.styles,
            validator=validator.get_name_validator(),
            validate_while_typing=False,
        )
        return ret

    def request_class_name(self, name_repr):
        """Prompt to ask for a name."""
        validator = ValidatorCollection()
        ret = prompt(
            HTML(self.request_template(name_repr)),
            style=self.styles,
            validator=validator.get_class_name_validator(),
            validate_while_typing=False,
        )
        return ret

    def request_source_file(self, files, source_repr, default_index):
        validator = ValidatorCollection(files)
        pre_hint = self.request_template(source_repr)
        post_hint = self.selection_template(source_repr, files, default_index)
        hint_message = pre_hint + "\n" + self.type_template(files) + post_hint
        ret = prompt(
            HTML(hint_message),
            style=self.styles,
            validator=validator.get_type_validator(),
            validate_while_typing=False,
        )
        # validator promise the ret can be transform to int
        return files[int(ret)] if ret else files[default_index]

    def request_compatible_graphs(self, types, graph_repr, default_index):
        validator = ValidatorCollection(types)
        pre_hint = self.request_template(graph_repr)
        post_hint = self.selection_template(graph_repr, types, default_index)
        hint_message = pre_hint + "\n" + self.type_template(types) + post_hint
        ret = prompt(
            HTML(hint_message),
            style=self.styles,
            validator=validator.get_type_validator(),
            validate_while_typing=False,
        )
        # validator promise the ret can be transform to int
        return types[int(ret)] if ret else types[default_index]


def get_config_from_gar(gar_file):
    with zipfile.ZipFile(gar_file) as gar:
        gs_config = io.StringIO(gar.read(".gs_conf.yaml").decode("utf-8"))
        return gar_file, yaml.load(gs_config)["app"][0]


if __name__ == "__main__":
    """Parse command line flags and do operations."""
    # pylint: disable=too-many-branches
    # pylint: disable=too-many-statements
    # pylint: disable=too-many-return-statements
    parser = argparse.ArgumentParser(
        description="Script to package gar for application."
    )

    parser.add_argument("-s", "--source", type=str)

    parser.add_argument("-c", "--check_compile", type=bool, default=False)

    parser.add_argument("-f", "--gar_file", type=str)

    args = parser.parse_args()

    if args.source:
        gar_file, configs = package_application(args.source)
    if args.gar_file:
        gar_file, configs = get_config_from_gar(args.gar_file)

    if args.check_compile:
        check_compile(gar_file, configs)
