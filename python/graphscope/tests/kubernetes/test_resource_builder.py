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

from graphscope.deploy.kubernetes.resource_builder import ReplicaSetBuilder


def test_replica_set_builder():
    labels = {
        "app.kubernetes.io/name": "graphscope",
        "app.kubernetes.io/component": "engine",
    }
    engine_builder = ReplicaSetBuilder(
        name="engine",
        labels=labels,
        replicas=2,
        image_pull_policy=None,
    )

    result = engine_builder.build()

    assert result["spec"]["template"]["metadata"]["annotations"] == {}

    name = "kubectl.kubernetes.io/default-container"
    engine_builder.add_annotation(name, "engine")
    result = engine_builder.build()

    assert result["spec"]["template"]["metadata"]["annotations"][name] == "engine"
