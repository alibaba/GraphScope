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

# Those methods should not be implemented in AdminService, but is still kept here, cause the python flask app relies on the openpai_interactive.yaml to launch the service, which needs these function definitions.
# To create_edge/delete_edge/get_edge/add_edge/update, send requests to query service.
def create_edge():
    raise NotImplementedError("create_edge is not implemented in admin service, please send to query service")


def create_edge_type():
    raise NotImplementedError("create_edge_type is not implemented in admin service, please send to query service")


def delete_edge():
    raise NotImplementedError("delete_edge is not implemented in admin service, please send to query service")


def get_edge():
    raise NotImplementedError("get_edge is not implemented in admin service, please send to query service")


def add_edge():
    raise NotImplementedError("add_edge is not implemented in admin service, please send to query service")


def update_edge():
    raise NotImplementedError("update_edge is not implemented in admin service, please send to query service")
