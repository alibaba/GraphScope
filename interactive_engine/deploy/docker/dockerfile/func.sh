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
function parallel_run {
    run_cmd=$1
    pod_hosts=$2
    engine=$3
    _id=1
    for pod in `echo $pod_hosts`
    do
        kubectl --namespace=$ENGINE_NAMESPACE exec $pod -c $engine -- sh -c "export server_id=$_id && $run_cmd"
        let _id+=1
    done
}
export -f parallel_run
function error {
    echo $1
    exit 1
}
export -f error
