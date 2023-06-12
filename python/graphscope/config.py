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

""" GraphScope configuration manager
"""



import os
import yaml
import argparse


class GSConfig(object):
    allowed_config_keys = [
        'config_file'
        'addr',
        'mode',
        'cluster_type',
        'k8s_namespace',
        'k8s_image_registry',
        'k8s_image_repository',
        'k8s_image_pull_policy',
        'k8s_image_pull_secrets',
        'k8s_coordinator_cpu',
        'k8s_coordinator_mem',
        'etcd_addrs',
        'etcd_listening_client_port',
        'etcd_listening_peer_port',
        'k8s_vineyard_image',
        'k8s_vineyard_deployment',
        'k8s_vineyard_cpu',
        'k8s_vineyard_mem',
        'vineyard_shared_mem',
        'k8s_engine_cpu',
        'k8s_engine_mem',
        'mars_worker_cpu',
        'mars_worker_mem',
        'mars_scheduler_cpu',
        'mars_scheduler_mem',
        'k8s_coordinator_pod_node_selector',
        'k8s_engine_pod_node_selector',
        'enabled_engines',
        'with_mars',
        'with_dataset',
        'k8s_volumes',
        'k8s_service_type',
        'preemptive',
        'k8s_deploy_mode',
        'k8s_waiting_for_delete',
        'num_workers',
        'show_log',
        'log_level',
        'timeout_seconds',
        'dangling_timeout_seconds',
        'dataset_download_retries',
    ]
    def __init__(self, default_config_file = 'default.yml'):
        self._conf = {}
        self._parser = argparse.ArgumentParser()
        for key in self.allowed_config_keys:
            self._parser.add_argument(f'--{key}', dest=key.lower())
        self._args, _ = self._parser.parse_known_args()
        self._load_default_config(default_config_file)
        self._override_with_env_vars()
        self._override_with_cmd_args()

    def _load_default_config(self, default_config_file='default.yml'):
        override_config_file = os.environ.get('GS_CONFIG_FILE', None) or getattr(self._args, 'config_file', None)
        default_config_file = override_config_file if type(override_config_file) is str else default_config_file

        try:
            with open(default_config_file, 'r') as f:
                default_config = yaml.safe_load(f)
        except FileNotFoundError:
            print(f"Error: {default_config_file} not found.")
            return
        except yaml.YAMLError as e:
            print(f"Error: Failed to parse YAML file {default_config_file}. {e}")
            return

        for key in self.allowed_config_keys:
            if key in default_config:
                self[key] = str(default_config[key])

    def _override_with_env_vars(self):
        for key in self.allowed_config_keys:
            env_var = 'GS_' + key.upper()
            if env_var in os.environ:
                self[key] = os.environ[env_var]

    def _override_with_cmd_args(self):
        for key in self.allowed_config_keys:
            if getattr(self._args, key.lower()) is not None:
                self[key] = str(getattr(self._args, key.lower()))

    def dump_to_file(self, filename):
        with open(filename, 'w') as f:
            yaml.dump(self._conf, f)
    
    def __getitem__(self, key):
        if key in self._conf:
            return self._conf[key]
        else:
            return None
        
    def __getattr__(self, key):
        if key in self._conf:
            return self._conf[key]
        else:
            return None
        
    def __setitem__(self, key, value):
        if key in self.allowed_config_keys:
            self._conf[key] = value
        else:
            raise KeyError(f'Invalid config key {key}')
    
    def __iter__(self):
        return iter(self._conf)
    
    def __contains__(self, key):
        return key in self._conf