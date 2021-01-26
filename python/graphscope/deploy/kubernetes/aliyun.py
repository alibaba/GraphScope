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
    from aliyunsdkcore.request import CommonRequest
except ImportError:
    AcsClient = None
    CommonRequest = None


class AliyunLauncher(object):
    def __init__(self, access_key_id, access_key_secret, region, config_file):
        self._access_key_id = access_key_id
        self._access_key_secret = access_key_secret
        self._region = region
        self._client = AcsClient(access_key_id, access_key_secret, region)
        self._cluster_id = None
        with open(config_file) as f:
            self._cluster_config = json.load(f)

    def create_cluster(self):
        # create cluster
        request = CommonRequest()
        request.set_accept_format('json')
        request.set_method('POST')
        request.set_protocol_type('https') # https | http
        request.set_domain('cs.aliyuncs.com')
        request.set_version('2015-12-15')
        request.add_query_param('RegionId', self._region)
        request.add_header('Content-Type', 'application/json')
        request.set_uri_pattern('/clusters')
        request.set_content(json.dumps(self._cluster_config))

        response = self._client.do_action_with_exception(request)
        self._cluster_id = json.loads(str(response, encoding='utf-8'))

        # get kubeconfig of cluster
        return self._get_kube_config(self._cluster_id)

    def _get_kube_config(self, cluster_id):
        request = CommonRequest()
        request.set_accept_format('json')
        request.set_method('GET')
        request.set_protocol_type('https') # https | http
        request.set_domain('cs.aliyuncs.com')
        request.add_query_param('RegionId', self._region)
        request.add_header('Content-Type', 'application/json')
        request.set_uri_pattern('/k8s/{}/user_config'.format(cluster_id))
        body = '''{}'''
        request.set_content(body.encode('utf-8'))
        response = self._client.do_action_with_exception(request)
        return json.loads(str(response, encoding='utf-8'))["config"]

    def delete_cluster(self):
        request = CommonRequest()
        request.set_accept_format('json')
        request.set_method('DELETE')
        request.set_protocol_type('https') # https | http
        request.set_domain('cs.aliyuncs.com')
        request.add_query_param('RegionId', self._region)
        request.add_header('Content-Type', 'application/json')
        request.set_uri_pattern('/clusters/{}'.format(self._cluster_id))
        body = '''{}'''
        request.set_content(body.encode('utf-8'))
        self._client.do_action_with_exception(request)
