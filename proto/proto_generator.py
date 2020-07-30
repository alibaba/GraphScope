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

import glob
import os
import subprocess
import sys


def gather_all_proto(proto_dir, suffix="*.proto"):
    directory = os.path.join(proto_dir, suffix)
    files = glob.glob(directory)
    return files


def create_path(path):
    """Utility function to create a path."""
    if os.path.isdir(path):
        return
    os.makedirs(path, exist_ok=True)


def cpp_out(base_dir, proto_path, output_dir):
    files = gather_all_proto(os.path.join(base_dir, proto_path))
    for proto_file in files:
        subprocess.check_call(
            [
                "protoc",
                "-I%s" % ".",
                "--cpp_out=%s" % output_dir,
                proto_file,
            ],
            stderr=subprocess.STDOUT,
        )


def python_out(base_dir, proto_path, output_dir):
    files = gather_all_proto(os.path.join(base_dir, proto_path))
    for proto_file in files:
        subprocess.check_call(
            [
                "python3",
                "-m",
                "grpc_tools.protoc",
                "-I%s" % ".",
                "--python_out=%s" % output_dir,
                proto_file,
            ],
            stderr=subprocess.STDOUT,
        )


def cpp_service_out(base_dir, proto_path, output_dir):
    plugin_path = str(
        subprocess.check_output(["which", "grpc_cpp_plugin"]), "utf-8"
    ).strip()
    suffix = "*_service.proto"
    files = gather_all_proto(os.path.join(base_dir, proto_path), suffix)
    for proto_file in files:
        subprocess.check_call(
            [
                "protoc",
                "-I%s" % ".",
                "--grpc_out=%s" % output_dir,
                "--plugin=protoc-gen-grpc=%s" % plugin_path,
                proto_file,
            ],
            stderr=subprocess.STDOUT,
        )


def python_service_out(base_dir, proto_path, output_dir):
    suffix = "*_service.proto"
    files = gather_all_proto(os.path.join(base_dir, proto_path), suffix)
    for proto_file in files:
        subprocess.check_call(
            [
                "python3",
                "-m",
                "grpc_tools.protoc",
                "-I%s" % ".",
                "--python_out=%s" % output_dir,
                "--grpc_python_out=%s" % output_dir,
                proto_file,
            ],
            stderr=subprocess.STDOUT,
        )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python proto_generator.py <OUTPUT_PATH> [--cpp] [--python]")
    else:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(os.path.join(current_dir, "../"))
        # must use relative path.
        directory = "."
        proto_path = "proto"

        if len(sys.argv) <= 2 or len(sys.argv) > 2 and sys.argv[2] == "--cpp":
            output_dir = sys.argv[1]
            output_dir = os.path.realpath(os.path.realpath(output_dir))
            print("Generating cpp proto to:" + output_dir)
            create_path(output_dir)
            cpp_out(directory, proto_path, output_dir)
            cpp_service_out(directory, proto_path, output_dir)

        if len(sys.argv) <= 2 or len(sys.argv) > 2 and sys.argv[2] == "--python":
            output_dir = sys.argv[1]
            output_dir = os.path.realpath(os.path.realpath(output_dir))
            print("Generating python proto to:" + output_dir)
            python_out(directory, proto_path, output_dir)
            python_service_out(directory, proto_path, output_dir)
            if not os.path.exists(os.path.join(output_dir, "proto", "__init__.py")):
                with open(os.path.join(output_dir, "proto", "__init__.py"), "w"):
                    pass
