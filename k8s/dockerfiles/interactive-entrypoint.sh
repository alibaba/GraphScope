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
# This is the entrypoint script for the interactive container
set -e

DEFAULT_GRAPH_NAME=gs_interactive_default_graph
BULK_LOADER_BINARY_PATH=/opt/flex/bin/bulk_loader
INTERACTIVE_SERVER_BIN=/opt/flex/bin/interactive_server
DEFAULT_INTERACTIVE_CONFIG_FILE=/opt/flex/share/interactive_config.yaml

function usage() {
    cat << EOF
        Usage: $0 -w[--workspace] <workspace> 
          This is the entrypoint script for the interactive container.
            Options:
                -h, --help: show this help message and exit
                -w, --workspace: Specify the workspace to use, should be path
                 inside container. If you want to use a workspace outside the 
                 container, you should mount it to the container. 
                 Default is /tmp/interactive_workspace
                -c, --enable-coordinator: Launch the Interactive service along
                 with Coordinator. Must enable this option if you want to use
                 `gsctl` command-line tool.
                -p, --port-mapping: Specify the port mapping for the interactive.
                  The format is container_port:host_port, multiple mappings are
                  separated by comma. For example, 8080:8081,7777:7778
EOF
}

function prepare_workspace() {
    #receive args
    local workspace=$1
    if [ -z "${workspace}" ]; then
        workspace="/tmp/interactive_workspace"
    fi
    #if workspace is not exist, create it
    if [ ! -d "${workspace}" ]; then
        mkdir -p ${workspace}
        mkdir -p ${workspace}/conf/
    else 
        if [ -f "${workspace}/conf/interactive_config.yaml" ]; then
            echo "Workspace ${workspace} already exists"
            return 0
        fi
    fi
    # prepare interactive_config.yaml
    engine_config_path="${workspace}/conf/interactive_config.yaml"
    cp ${DEFAULT_INTERACTIVE_CONFIG_FILE} $engine_config_path
    #make sure the line which start with default_graph is changed to default_graph: ${DEFAULT_GRAPH_NAME}
    sed -i "s/default_graph:.*/default_graph: ${DEFAULT_GRAPH_NAME}/" $engine_config_path
    echo "Using default graph: ${DEFAULT_GRAPH_NAME} to start the service"
    
    # copy the builtin graph
    builtin_graph_dir="${workspace}/data/${DEFAULT_GRAPH_NAME}"
    mkdir -p $builtin_graph_dir
    builtin_graph_import_path="${builtin_graph_dir}/import.yaml"
    builtin_graph_schema_path="${builtin_graph_dir}/graph.yaml"
    builtin_graph_data_path="${builtin_graph_dir}/indices"
    cp /opt/flex/share/${DEFAULT_GRAPH_NAME}/graph.yaml  ${builtin_graph_schema_path}
    cp /opt/flex/share/${DEFAULT_GRAPH_NAME}/bulk_load.yaml ${builtin_graph_import_path}
    export FLEX_DATA_DIR=/opt/flex/share/gs_interactive_default_graph/
    builtin_graph_loader_cmd="${BULK_LOADER_BINARY_PATH} -g ${builtin_graph_schema_path} -d ${builtin_graph_data_path} -l ${builtin_graph_import_path}"
    echo "Loading builtin graph: ${DEFAULT_GRAPH_NAME} with command: $builtin_graph_loader_cmd"
    eval $builtin_graph_loader_cmd || (echo "Failed to load builtin graph: ${DEFAULT_GRAPH_NAME}" && exit 1)
    echo "Successfully loaded builtin graph: ${DEFAULT_GRAPH_NAME}"
}

function launch_service() {
    #expect 1 arg
    if [ $# -ne 1 ]; then
        echo "Usage: launch_service <workspace>"
        exit 1
    fi
    local workspace=$1
    engine_config_path="${workspace}/conf/interactive_config.yaml"
    # start the service
    start_cmd="${INTERACTIVE_SERVER_BIN} -c ${engine_config_path}"
    start_cmd="${start_cmd} -w ${workspace}"
    start_cmd="${start_cmd} --enable-admin-service true"
    start_cmd="${start_cmd} --start-compiler true"
    echo "Starting the service with command: $start_cmd"
    if $ENABLE_COORDINATOR; then start_cmd="${start_cmd} &"; fi
    eval $start_cmd
}

function launch_coordinator() {
  if [ $# -ne 1 ]; then
    echo "Usage: launch_coordinator <port_mapping>"
    exit 1
  fi
  local host_ports=()
  local container_ports=()
  if [ -n "$1" ]; then
    IFS=',' read -ra port_mappings <<< "$1"
    for port_mapping in "${port_mappings[@]}"; do
      IFS=':' read -ra ports <<< "$port_mapping"
      container_ports+=(${ports[0]})
      host_ports+=(${ports[1]})
    done
  fi
  if $ENABLE_COORDINATOR;
  then
    dst_coordinator_config_file="/tmp/coordinator_config_$(date +%s).yaml"
    cat > $dst_coordinator_config_file << EOF
session:
  instance_id: demo
launcher_type: hosts
coordinator:
  http_port: 8080
  http_server_only: true
EOF
    python3 -m pip install pyyaml
    res=$(python3 -c "import yaml; config = yaml.safe_load(open('${DEFAULT_INTERACTIVE_CONFIG_FILE}')); print(yaml.dump(config.get('http_service', {}), default_flow_style=False, indent=4))")
    # Expect max_content_length: 1GB
    max_content_length=$(echo "$res" | grep "max_content_length:" | cut -d':' -f2)
    echo "  max_content_length: ${max_content_length}" >> $dst_coordinator_config_file

    # for each line in res, echo to dst_coordinator_config_file with 2 spaces indentation
    while IFS= read -r line; do
      echo "  $line" >> $dst_coordinator_config_file
    done <<< "$res"

    if [ ${#host_ports[@]} -gt 0 ]; then
      echo "interactive:" >> $dst_coordinator_config_file
      echo "  port_mapping:" >> $dst_coordinator_config_file
      for i in "${!host_ports[@]}"; do
        echo "    ${container_ports[$i]}: ${host_ports[$i]}" >> $dst_coordinator_config_file
      done
    fi
    # i.e
    # interactive:
    #   port_mapping:
    #     8080: 8081
    #     7777: 7778
    python3 -m gscoordinator --config-file $dst_coordinator_config_file
  fi
}


####################  Entry ####################

ENABLE_COORDINATOR=false
WORKSPACE=/tmp/interactive_workspace
while [[ $# -gt 0 ]]; do
  case $1 in
    -w | --workspace)
      shift
      if [[ $# -eq 0 || $1 == -* ]]; then
        echo "Option -w requires an argument." >&2
        exit 1
      fi
      WORKSPACE=$1
      shift
      ;;
    -c | --enable-coordinator)
      ENABLE_COORDINATOR=true
      shift
      ;;
    -p | --port-mapping)
      shift
      if [[ $# -eq 0 || $1 == -* ]]; then
        echo "Option -p requires an argument." >&2
        exit 1
      fi
      PORT_MAPPING=$1
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "Invalid option: $1" >&2
      usage
      exit 1
      ;;
  esac
done


prepare_workspace $WORKSPACE
launch_service $WORKSPACE
# Note that the COORDINATOR_CONFIG_FILE should be inside the container
launch_coordinator $PORT_MAPPING
