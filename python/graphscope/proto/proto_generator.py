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
import shutil
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


def cpp_out(relative_dir, output_dir):
    files = gather_all_proto(relative_dir)
    for proto_file in files:
        subprocess.check_call(
            [
                shutil.which("protoc"),
                "-I%s" % ".",
                "--cpp_out=%s" % output_dir,
                proto_file,
            ],
            stderr=subprocess.STDOUT,
        )


def python_out(relative_dir, output_dir):
    files = gather_all_proto(relative_dir)
    for proto_file in files:
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "grpc_tools.protoc",
                "-I%s" % ".",
                "--python_out=%s" % os.path.join(output_dir),
                proto_file,
            ],
            stderr=subprocess.STDOUT,
        )


def cpp_service_out(relative_dir, output_dir):
    plugin_path = str(
        subprocess.check_output([shutil.which("which"), "grpc_cpp_plugin"]), "utf-8"
    ).strip()
    suffix = "*_service.proto"
    files = gather_all_proto(relative_dir, suffix)
    for proto_file in files:
        subprocess.check_call(
            [
                shutil.which("protoc"),
                "-I%s" % ".",
                "--grpc_out=%s" % output_dir,
                "--plugin=protoc-gen-grpc=%s" % plugin_path,
                proto_file,
            ],
            stderr=subprocess.STDOUT,
        )


def python_service_out(relative_dir, output_dir):
    suffix = "*_service.proto"
    files = gather_all_proto(relative_dir, suffix)
    for proto_file in files:
        subprocess.check_call(
            [
                sys.executable,
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
        sys.exit(1)

    # path to 'GraphScope/python/graphscope/proto'
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # path to 'GraphScope/python'
    base_dir = os.path.join(current_dir, "../", "../")
    os.chdir(base_dir)

    output_dir = sys.argv[1]
    output_dir = os.path.realpath(os.path.realpath(output_dir))
    create_path(output_dir)

    # must use relative path
    relative_dir = os.path.join(".", "graphscope", "proto")
    if len(sys.argv) <= 2 or len(sys.argv) > 2 and sys.argv[2] == "--cpp":
        print("Generating cpp proto to: " + output_dir)
        cpp_out(relative_dir, output_dir)
        cpp_service_out(relative_dir, output_dir)

    if len(sys.argv) <= 2 or len(sys.argv) > 2 and sys.argv[2] == "--python":
        print("Generating python proto to: " + output_dir)
        python_out(relative_dir, output_dir)
        python_service_out(relative_dir, output_dir)
