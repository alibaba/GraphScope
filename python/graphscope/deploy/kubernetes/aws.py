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

try:
    import boto3
except ImportError:
    boto3 = None


class AWSCluster(object):
    def __init__(self, access_key_id,  secret_access_key, region_name):
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key
        self._region_name = region_name
        self._client = boto3.client("eks", aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._secret_access_key, region_name=self._region_name)

    def create_cluster(self):
        rep = self._client.create_cluster(name="eks_for_gs")
        cluster_cert = rep["cluster"]["certificateAuthority"]["data"]
        cluster_ep = rep["cluster"]["endpoint"]
        cluster_name = rep["cluster"]["name"]
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

    def delete_cluster(self):
        self._client.delete_cluster(name="eks_for_gs")
