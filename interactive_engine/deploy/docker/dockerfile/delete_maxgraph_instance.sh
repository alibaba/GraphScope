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
source /root/maxgraph/func.sh
function _delete_maxgraph_instance {
    _delete_pod ${object_id}
    kill_cmd="/root/maxgraph/kill_process.sh $object_id"
    parallel_run "$kill_cmd" "$pod_hosts" $ENGINE_CONTAINER
}
function _delete_pod {
    kubectl delete -f /root/maxgraph/pod_${object_id}.yaml
    kubectl delete configmap config-${object_id}
    kubectl delete service gremlin-${object_id}
}
export object_id=$1
export pod_hosts=`echo $2 | awk -F"," '{for(i=1;i<=NF;++i) {print $i" "}}'`
export ENGINE_CONTAINER=$3
_delete_maxgraph_instance
rm -rf /home/maxgraph/config_$object_id /root/maxgraph/pod_${object_id}.yaml
