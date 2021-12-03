#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2021 Alibaba Group Holding Limited.
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

"""Prestop hook for delete all resource created by coordinator in kubernetes
"""

import json
import os
import subprocess
import tempfile

DEFAULT_PATH = os.path.join("/", tempfile.gettempprefix(), "resource_object")


class KubernetesResources(object):
    def __init__(self):
        self._resources = {}

    def load_json_file(self, path):
        try:
            with open(path, "r") as f:
                self._resources = json.load(f)
        except FileNotFoundError:
            # expect, pass
            pass

    def cleanup(self):
        for (name, kind) in self._resources.items():
            cmd = ["kubectl", "delete", kind, name]
            try:
                subprocess.check_call(cmd)
            except:  # noqa: E722
                pass


if __name__ == "__main__":

    path = DEFAULT_PATH
    resources = KubernetesResources()
    resources.load_json_file(path)
    resources.cleanup()
