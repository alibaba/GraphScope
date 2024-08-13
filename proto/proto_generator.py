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
    pattern = os.path.join(proto_dir, suffix)
    return glob.glob(pattern)

def gather_all_service_proto(proto_dir):
    return gather_all_proto(proto_dir, "*_service.proto")

def create_path(path):
    """Utility function to create a path."""
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)


def cpp_out(relative_dir, output_dir):
    files = gather_all_proto(relative_dir)
    error_files = gather_all_proto(os.path.join(relative_dir, "error"), suffix="coordinator.proto")
    files.extend(error_files)
    for proto_file in files:
        cmd = [
            shutil.which("protoc"),
            "-I.",
            f"--cpp_out={output_dir}",
            proto_file,
        ]
        subprocess.check_call(
            cmd,
            stderr=subprocess.STDOUT,
        )


def python_out(relative_dir, output_dir):
    files = gather_all_proto(relative_dir)
    groot_files = gather_all_proto(os.path.join(relative_dir, "groot", "sdk"))
    error_files = gather_all_proto(os.path.join(relative_dir, "error"))
    files.extend(groot_files)
    files.extend(error_files)
    for proto_file in files:
        cmd = [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            "-I.",
            f"--python_out={output_dir}",
            f"--mypy_out={output_dir}",
            proto_file,
        ]
        subprocess.check_call(
            cmd,
            stderr=subprocess.STDOUT,
        )


def cpp_service_out(relative_dir, output_dir):
    plugin_path = str(
        subprocess.check_output([shutil.which("which"), "grpc_cpp_plugin"]), "utf-8"
    ).strip()
    files = gather_all_service_proto(relative_dir)
    for proto_file in files:
        cmd = [
            shutil.which("protoc"),
            "-I.",
            f"--grpc_out={output_dir}",
            f"--plugin=protoc-gen-grpc={plugin_path}",
            proto_file,
        ]
        subprocess.check_call(
            cmd,
            stderr=subprocess.STDOUT,
        )


def python_service_out(relative_dir, output_dir):
    files = gather_all_service_proto(relative_dir)
    groot_files = gather_all_service_proto(os.path.join(relative_dir, "groot", "sdk"))
    files.extend(groot_files)
    for proto_file in files:
        cmd = [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            "-I.",
            f"--python_out={output_dir}",
            f"--mypy_out={output_dir}",
            f"--grpc_python_out={output_dir}",
            f"--mypy_grpc_out={output_dir}",
            proto_file,
        ]
        subprocess.check_call(
            cmd,
            stderr=subprocess.STDOUT,
        )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python proto_generator.py <OUTPUT_PATH> [--cpp] [--python]")
        sys.exit(1)

    output_dir = sys.argv[1]
    output_dir = os.path.realpath(os.path.realpath(output_dir))
    create_path(output_dir)

    # path to 'GraphScope/proto'
    current_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(current_dir)

    # must use relative path
    relative_dir = "."
    if len(sys.argv) <= 2 or len(sys.argv) > 2 and sys.argv[2] == "--cpp":
        print("Generating cpp proto to: " + output_dir)
        cpp_out(relative_dir, output_dir)
        cpp_service_out(relative_dir, output_dir)

    if len(sys.argv) <= 2 or len(sys.argv) > 2 and sys.argv[2] == "--python":
        print("Generating python proto to: " + output_dir)
        python_out(relative_dir, output_dir)
        python_service_out(relative_dir, output_dir)
