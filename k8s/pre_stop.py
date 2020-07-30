#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import subprocess

DEFAULT_PATH = "/tmp/resource_object"


class KubernetesResources(object):
    def __init__(self):
        self._resources = {}

    def load_json_file(self, path):
        try:
            with open(path, "r") as f:
                self._resources = json.load(f)
        except FileNotFoundError as e:
            # expect, pass
            pass

    def cleanup(self):
        for (name, kind) in self._resources.items():
            cmd = ["kubectl", "delete", kind, name]
            try:
                subprocess.check_call(cmd)
            except:
                pass


if __name__ == "__main__":

    path = DEFAULT_PATH
    resources = KubernetesResources()
    resources.load_json_file(path)
    resources.cleanup()
