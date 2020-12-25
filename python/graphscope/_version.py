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
import subprocess

__version__ = "0.1.0"


def git_info():
    def _get_cmd_results(pkg_root, cmd):
        proc = subprocess.Popen(cmd, cwd=pkg_root, stdout=subprocess.PIPE)
        proc.wait()
        if proc.returncode == 0:
            s = proc.stdout.read().decode()
            proc.stdout.close()
            return s

    pkg_root = os.path.dirname(os.path.abspath(__file__))
    git_root = os.path.join(os.path.dirname(pkg_root), "../.git")

    if os.path.exists(git_root):
        commit_hash = _get_cmd_results(pkg_root, ["git", "rev-parse", "HEAD"]).strip()
        if not commit_hash:
            return
        branches = _get_cmd_results(pkg_root, ["git", "branch"]).splitlines(False)
        commit_ref = None
        for branch in branches:
            if not branch.startswith("*"):
                continue
            striped = branch[1:].strip()
            if not striped.startswith("("):
                commit_ref = striped
            else:
                _, commit_ref = striped.rsplit(" ", 1)
                commit_ref = commit_ref.rstrip(")")
        if commit_ref is None:
            return
        return commit_hash, commit_ref
    else:
        branch_file = os.path.join(pkg_root, ".git-branch")
        if not os.path.exists(branch_file):
            return
        with open(branch_file, "r") as bf:
            bf_content = bf.read().strip()
            return bf_content.split(None, 1)
