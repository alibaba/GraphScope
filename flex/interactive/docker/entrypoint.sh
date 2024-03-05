#!/bin/bash
# This is the entrypoint script for the interactive container
set -e

DEFAULT_GRAPH_NAME=gs_interactive_default_graph
BULK_LOADER_BINARY_PATH=/opt/flex/bin/bulk_loader
INTERACTIVE_SERVER_BIN=/opt/flex/bin/interactive_server

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
        echo "Workspace ${workspace} already exists"
        return 0
    fi
    # prepare engine_config.yaml
    engine_config_path="${workspace}/conf/engine_config.yaml"
    cp /opt/flex/share/engine_config.yaml $engine_config_path
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
    engine_config_path="${workspace}/conf/engine_config.yaml"
    # start the service
    start_cmd="${INTERACTIVE_SERVER_BIN} -c ${engine_config_path}"
    start_cmd="${start_cmd} -w ${workspace}"
    start_cmd="${start_cmd} --enable-admin-service true"
    start_cmd="${start_cmd} --start-compiler true"
    echo "Starting the service with command: $start_cmd"
    eval $start_cmd
}


####################  Entry ####################

WORKSPACE=/tmp/interactive_workspace
while [[ $# -gt 0 ]]; do
  key="$1"

  case $key in
  -h | --help)
    usage
    exit
    ;;
  -w | --workspace)
    shift
    WORKSPACE="$1"
    exit 0
    ;;
  *) # unknown option
    err "unknown option $1"
    usage
    exit 1
    ;;
  esac
done

prepare_workspace $WORKSPACE
launch_service $WORKSPACE