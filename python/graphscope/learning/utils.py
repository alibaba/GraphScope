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

import yaml


def read_folder_files_content(folder_path):
    files_content = {}
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        # make sure it's a file not a directory
        if os.path.isfile(file_path):
            with open(file_path, "r") as file:
                files_content[filename] = file.read()
    return files_content


def fill_params_in_yaml(file_path, params):
    with open(file_path, "r") as file:
        yaml_content = file.read()
        for param_key, param_value in params.items():
            yaml_content = yaml_content.replace(
                "${" + param_key + "}", str(param_value)
            )
        return yaml.safe_load(yaml_content)
