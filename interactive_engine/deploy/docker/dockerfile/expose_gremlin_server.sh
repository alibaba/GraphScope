#!/bin/bash
# Copyright 2020 Alibaba Group Holding Limited.
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
function _expose_gremlin_server {
    gremlin_pod=`kubectl get pods -l "graph=pod-${object_id}" | grep -v NAME | awk '{print $1}'`
    kubectl expose pod ${gremlin_pod} --name=gremlin-${object_id} --port=${port} \
        --target-port=${port} --type=NodePort 1>/dev/null 2>&1
    [ $? -eq 0 ] || exit 1
    node_port=`kubectl describe services gremlin-${object_id} | grep "NodePort" | grep "TCP" | tr -cd "[0-9]"`
    [ $? -eq 0 ] || exit 1
    echo $node_port
 }
export object_id=$1
export port=$2
_expose_gremlin_server
