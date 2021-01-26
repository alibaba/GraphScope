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

import json
import time

try:
    import boto3
except ImportError:
    boto3 = None


class AWSLauncher(object):
    def __init__(self, access_key_id,  access_key_secret, region, config_file):
        self._access_key_id = access_key_id
        self._access_key_secret = access_key_secret
        self._region = region
        self._client = boto3.client("eks", aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._access_key_secret, region=self._region)
        with open(config_file) as f:
            self._cluster_config = json.load(f)
        self._cluster_name = self._cluster_config["name"]

    def create_cluster(self):
        self._client.create_cluster(**self._cluster_config)
        self._wait_cluster_ready()
        response = self._client.describe_cluster(name=self._cluster_name)
        cluster_cert = response["cluster"]["certificateAuthority"]["data"]
        cluster_ep = response["cluster"]["endpoint"]
        cluster_name = response["cluster"]["name"]
        # build the cluster config hash
        cluster_config = {
            "apiVersion": "v1",
            "kind": "Config",
            "clusters": [
                {
                    "cluster": {
                        "server": str(cluster_ep),
                        "certificate-authority-data": str(cluster_cert)
                    },
                    "name": "kubernetes"
                }
            ],
            "contexts": [
                {
                    "context": {
                        "cluster": "kubernetes",
                        "user": "aws"
                    },
                    "name": "aws"
                }
            ],
            "current-context": "aws",
            "preferences": {},
            "users": [
                {
                    "name": "aws",
                    "user": {
                        "exec": {
                            "apiVersion": "client.authentication.k8s.io/v1alpha1",
                            "command": "heptio-authenticator-aws",
                            "args": [
                                "token", "-i", cluster_name
                            ]
                        }
                    }
                }
            ]
        }
        return cluster_config

    def _wait_cluster_ready(self):
        while True:
            response = self._client.describe_cluster(name=self._cluster_name)
            if response["cluster"]["status"] == "ACTIVE":
                return
            time.sleep(5)

    def delete_cluster(self):
        self._client.delete_cluster(name=self._cluster_name)
