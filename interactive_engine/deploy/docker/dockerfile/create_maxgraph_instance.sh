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
function _create_maxgraph_instance {
    _setup_config
    # launch coordinator & frontend in one pod
    _create_pod
    # [ $? -eq 0 ] || error "launch pod fail"
    echo $?
    # launch interactive engine per analytical pod
    parallel_run "$launch_engine_cmd" "$pod_hosts" $ENGINE_CONTAINER 1>/dev/null 2>&1
    sleep 15s
    _expose_gremlin_server "8182"
}
function _create_pod {
    gremlin_image=$GREMLIN_IMAGE
    coordinator_image=$COORDINATOR_IMAGE
    config_name=config-${object_id}
    pod_name=pod-${object_id}
    gremlin_image=$(printf '%s\n' "$gremlin_image" | sed -e 's/[\/&]/\\&/g')
    coordinator_image=$(printf '%s\n' "$coordinator_image" | sed -e 's/[\/&]/\\&/g')
    # node_host=`kubectl describe pods -l "app=manager" | grep "Node:" | head -1 | awk -F '[ /]+' '{print $2}'`
    kubectl create configmap $config_name --from-file /home/maxgraph/config_$object_id
    sed -e "s/unique_pod_name/$pod_name/g" -e "s/unique_config_name/$config_name/g" \
        -e "s/gremlin_image/$gremlin_image/g" -e "s/unique_object_id/$object_id/g" \
        -e "s/coordinator_image/$coordinator_image/g" \
        /root/maxgraph/pod.yaml > /root/maxgraph/pod_${object_id}.yaml
    kubectl apply -f /root/maxgraph/pod_${object_id}.yaml
}
function _render_schema_path {
    config=/home/maxgraph/config_${object_id}
    mkdir -p $config
    file=${schema_path##*/}
    cp $schema_path $config/$file
    schema_path=/home/maxgraph/config/$file
}
function _setup_config {
    _render_schema_path
    update_cmd="/root/maxgraph/set_config.sh $object_id $schema_path $(hostname -i) $engine_count $engine_paras"
    sh -c "$update_cmd"
    parallel_run "$update_cmd" "$pod_hosts" $ENGINE_CONTAINER
}
function _expose_gremlin_server {
    port=$1
    gremlin_pod=`kubectl get pods -l "graph=pod-${object_id}" | grep -v NAME | awk '{print $1}'`
    kubectl expose pod ${gremlin_pod} --name=gremlin-${object_id} --port=${port} \
        --target-port=${port} --type=NodePort 1>/dev/null 2>&1
    [ $? -eq 0 ] || exit 1
    node_port=`kubectl describe services gremlin-${object_id} | grep "NodePort" | grep "TCP" | tr -cd "[0-9]"`
    [ $? -eq 0 ] || exit 1
    # EXTERNAL_IP=`kubectl describe pods -l "app=manager" | grep "Node:" | head -1 | awk -F '[ /]+' '{print $3}'`
    EXTERNAL_IP=`kubectl describe pods pod-${object_id} | grep "Node:" | head -1 | awk -F '[ /]+' '{print $3}'`
    echo "FRONTEND_PORT:$EXTERNAL_IP:$node_port"
}
export object_id=$1
export schema_path=$2
export engine_count=`echo $3 | awk -F"," '{print NF}'`
export pod_hosts=`echo $3 | awk -F"," '{for(i=1;i<=NF;++i) {print $i" "}}'`
export ENGINE_CONTAINER=$4
export engine_paras=$5 
export launch_engine_cmd="export object_id=${object_id} && /home/maxgraph/executor-entrypoint.sh"
_create_maxgraph_instance
