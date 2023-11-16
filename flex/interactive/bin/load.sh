#!/bin/bash
# This procedure periodically loads the odps table into a rt_mutable_graph.
# get current script directory
script_dir=$(cd "$(dirname "$0")"; pwd)
ADMIN_ENDPOINT="http://localhost:7777"

# colored error and info functions to wrap messages.
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color
err() {
  echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] -ERROR- $* ${NC}" >&2
}

info() {
  echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] -INFO- $* ${NC}"
}

emph(){
  echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] -INFO- $* ${NC}"
}

function check_file_exists(){
  if [ ! -f "$1" ]; then
    err "file $1 not exists"
    exit 1
  fi
}

function check_directory_exists(){
  if [ ! -d "$1" ]; then
    err "directory $1 not exists"
    exit 1
  fi
}

function check_directory_not_exists(){
  if [ -d "$1" ]; then
    err "directory $1 already exists"
    exit 1
  fi
}

function check_file_not_exists(){
  if [ -f "$1" ]; then
    err "file $1 already exists"
    exit 1
  fi
}

function check_date_str(){
    # $1 should be a date string in format of %Y%m%d
    # check if the date string is valid
    date -d "$1" +%Y%m%d > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        err "date string $1 is not valid"
        exit 1
    fi
}


function prepare_new_graph_dir() {
    # copy the graph.yaml and plugins to the new graph directory
    # $1 - orig_graph_dir
    # $2 - cur_graph_dir
    if [ $# -ne 3 ]; then
        err "prepare_new_graph_dir needs 3 arguments"
        exit 1
    fi
    orig_graph_dir=$1
    cur_graph_dir=$2
    mkdir -p $cur_graph_dir
    mkdir -p $cur_graph_dir/plugins
    cur_graph_name=$3
    info "prepare_new_graph_dir $orig_graph_dir $cur_graph_dir"
    # copy the graph.yaml
    cmd="cp $orig_graph_dir/graph.yaml $cur_graph_dir/graph.yaml"
    echo $cmd
    eval $cmd
    # copy the plugins
    if [ -d $orig_graph_dir/plugins ];then
       cp -r $orig_graph_dir/plugins/* $cur_graph_dir/plugins
    fi
    info "finish preparing new graph dir $cur_graph_dir"
    # replace the graph name in graph.yaml from original graph name to current graph name
    sed -i "s/$original_graph_name/$cur_graph_name/g" $cur_graph_dir/graph.yaml
}

function prepare_import_yaml() {
    # $1 - import_yaml
    # $2 - date_str
    # $3 - dst_import_yaml
    if [ $# -ne 3 ]; then
        err "prepare_import_yaml needs 3 arguments"
        exit 1
    fi
    import_yaml=$1
    date_str=$2
    cur_import_yaml=$3
    info "prepare_import_yaml $import_yaml $date_str $cur_import_yaml"
    #cp the import.yaml to the new graph directory
    cp $import_yaml $cur_import_yaml
    # replace the {biz_date} to date_str
    cmd="sed -i 's/bizdate/$date_str/g' $cur_import_yaml"
    echo $cmd
    eval $cmd
    info "finish preparing import yaml $cur_import_yaml"
}


function run_graph_loading() {
    # $1 - graph_schema_yaml
    # $2 - import_yaml
    # $3 - real_dst_data_path
    if [ $# -ne 3 ]; then
        err "run_graph_loading needs 3 arguments"
        exit 1
    fi
    graph_schema_yaml=$1
    import_yaml=$2
    real_dst_data_path=$3
    info "run_graph_loading $graph_schema_yaml $import_yaml $real_dst_data_path"
    # find bulk_loader
    # first check bulk_loader is executable
    bulk_loader_cmd="which bulk_loader"
    info $bulk_loader_cmd
    eval $bulk_loader_cmd
    info "running res "
    if [ $? -ne 0 ]; then
        info "bulk_loader not found"
    fi
    # try relative path
    bulk_loader_path=${script_dir}/../../build//bin/bulk_loader
    info "try path: ${bulk_loader_path}" 
    if [ ! -x "$bulk_loader_path" ]; then
        err "bulk_loader not found"
        exit 1
    fi
    info "found bulk loader"
    cmd="$bulk_loader_path --g $graph_schema_yaml -l $import_yaml"
    cmd="$cmd -d $real_dst_data_path"
    info "run command: $cmd"
    eval $cmd
    if [ ! -f $real_dst_data_path/schema ];then
	    err "fail to load graph to $real_dst_data_path"
	    exit
    fi
    info "finish loading graph to $real_dst_data_path"
}


function switch_graph() {
    if [ $# -ne 1 ]; then
        err "switch_graph needs 1 arguments"
        exit 1
    fi
    graph_name=$1
    info "switch_graph $graph_name"
    # send http request to admin_endpoint. the payload is json format
    # {"graph_name": "graph_name"}
    cmd="curl -X POST http://localhost:7777/v1/service/start -d \"{\\\"graph_name\\\": \\\"$graph_name\\\"}\"  -H \"Content-Type: application/json\""
    info "run command: $cmd"
    eval $cmd
    info "finish switching graph to $graph_name"
}


# Entry
# Before this script is used, we assume a graph with <graph_name> is already
# loaded and stored at <interactive_workspace>/<graph_name>.
# This scrip will try to load the odps table into the graph periodically.
# and put to <interactive_workspace>/<graph_name>_<date_str>.
# we also need to copy the graph schema and plugins to the new graph.
if [ $# -lt 3 ] || [ $# -gt 4 ]; then 
    echo "Usage: $4 <interactive_workspace> <import_yaml_template>"
    echo "          <graph_name> [date_str]"
    echo "     import_yaml - the template import yaml file, the ds is a place holder"
    echo "     interactive_workspace - the interactive workspace"
    echo "     graph_name - the graph name"
    echo "     optional date_str - the date string, default is yesterday"
    exit 1
fi

interactive_workspace=$1
import_yaml_template=$2
original_graph_name=$3
if [ $# -lt 4 ]; then
    date_str=`date -d "yesterday" +%Y%m%d`
else
    date_str=$4
fi
#check_date_str $date_str

data_dir="$interactive_workspace/data"
check_directory_exists $data_dir

origin_graph_schema_yaml="$data_dir/$original_graph_name/graph.yaml"
info "origin_graph_schema_yaml: $origin_graph_schema_yaml"
info "import_yaml_template: $import_yaml_template"
info "interactive_workspace: $interactive_workspace"
info "date_str: $date_str"
orig_graph_dir="$data_dir/$original_graph_name"
cur_graph_name="$original_graph_name"_"$date_str"
cur_graph_dir="$data_dir/$cur_graph_name"
real_dst_data_path="$cur_graph_dir/indices"
info "original_graph_name: $original_graph_name"
info "cur_graph_name: $cur_graph_name"
info "orig_graph_dir: $orig_graph_dir"
info "cur_graph_dir: $cur_graph_dir"
info "real_dst_data_path: $real_dst_data_path"


check_file_exists $origin_graph_schema_yaml
check_file_exists $import_yaml_template
check_directory_not_exists $cur_graph_dir
check_directory_not_exists $real_dst_data_path

# prepare the new graph directory
prepare_new_graph_dir $orig_graph_dir $cur_graph_dir $cur_graph_name
# prepare the import yaml
new_import_yaml="$cur_graph_dir/import.yaml"
prepare_import_yaml $import_yaml_template $date_str $new_import_yaml
# do the real work
run_graph_loading $origin_graph_schema_yaml $new_import_yaml $real_dst_data_path
#if graph loading success, copy the graph schema and plugins to the new graph

switch_graph $cur_graph_name


