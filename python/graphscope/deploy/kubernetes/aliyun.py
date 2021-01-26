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

try:
    from aliyunsdkcore.client import AcsClient
    from aliyunsdkcore.acs_exception.exceptions import ClientException
    from aliyunsdkcore.acs_exception.exceptions import ServerException
    from aliyunsdkcs.request.v20151215 import CreateClusterRequest
except ImportError:
    CSClient = None
    open_api_models = None


class AliyunCluster(object):
    def __init__(self, access_key_id, access_key_secret, region):
        self._access_key_id = access_key_id
        self._access_key_secret = access_key_secret
        self._region = region
        self._client = AcsClient(access_key_id, access_key_secret, region)

    def create_cluster(self):
        request = CreateClusterRequest.CreateClusterRequest()
        create_body = {
            "cluster_type":"Kubernetes",
            "name":"aliyun_for_gs",
            "region_id":self._region,
            "disable_rollback":"true",
            "timeout_mins":"60",
            "kubernetes_version":"1.12.6-aliyun.1",
            "snat_entry":"true",
            "endpoint_public_access":"false",
            "cloud_monitor_flags":"false",
            "node_cidr_mask":"25",
            "proxy_mode":"iptables",
            "tags":[],
            "addons": [{"name": "flannel"},{"name": "aliyun-log-controller","config": "{\"sls_project_name\":\"k8s-log-c64f6eab6a1764d3dbee3dc2b9e41****\"}"},{"name": "nginx-ingress-controller", "config": "{\"IngressSlbNetworkType\":\"internet\"}"}],
            "node_port_range":"30000-32767",
            "login_password":"test****",
            "cpu_policy":"none",
            "master_count":"3",
            "master_vswitch_ids":["vsw-2ze48rkq464rsdts****","vsw-2ze48rkq464rsdts1****","vsw-2ze48rkq464rsdts1****"],
            "master_instance_types":["ecs.sn1.medium","ecs.sn1.medium","ecs.sn1.medium"],
            "master_system_disk_category":"cloud_efficiency",
            "master_system_disk_size":"40",
            "worker_instance_types":["ecs.sn2.3xlarge"],
            "num_of_nodes":"3",
            "worker_system_disk_category":"cloud_efficiency",
            "worker_system_disk_size":"120",
            "vpcid":"vpc-2zegvl5etah5requ0****",
            "worker_vswitch_ids":["vsw-2ze48rkq464rsdts****"],
            "container_cidr":"172.20.XX.XX/16",
            "service_cidr":"172.21.XX.XX/20",
            "worker_data_disks": [{"category": "cloud_ssd", "size": 500}],
            "master_data_disks": [{"category": "cloud_ssd", "size": 500}],
            "taints":[{"key": "special", "value": "true", "effect": "NoSchedule"}],
        }
        request.set_content(json.dumps(create_body))
        request.set_content_type('application/json')
        response = self._client.do_action_with_exception(request)

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
