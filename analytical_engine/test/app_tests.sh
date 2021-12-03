#!/bin/bash
#
# A script to perform tests for analytical engine.

set -eo pipefail

# parse the args and set the variables.
function usage() {
  cat <<EOF
   Usage: $./app_test.sh [--test_dir]
   optional arguments:
     -h, --help                 show this help message and exit
     -t, --test_dir DIRECTORY   the existing test data dir.
EOF
}

test_dir=""
engine_dir=""

while [[ $# -gt 0 ]]; do
  key="$1"

  case $key in
  -h | --help)
    usage
    exit
    ;;
  -t | --test_dir)
    test_dir="$2"
    shift # past argument
    shift
    ;;
  *) # unknown option
    usage
    exit 1
    ;;
  esac
done


# colored error and info functions to wrap messages.
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color
err() {
  echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] -ERROR- $* ${NC}" >&2
}

info() {
  echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] -INFO- $* ${NC}"
}

# analytical_engine HOME, find the HOME using relative path.
ENGINE_HOME="$(
  cd "$(dirname "$0")/.." >/dev/null 2>&1
  pwd -P
)"

# find vineyard.
export VINEYARD_HOME=/usr/local/bin
# for open-mpi
export OMPI_MCA_btl_vader_single_copy_mechanism=none
export OMPI_MCA_orte_allowed_exit_without_sync=1

np=4
socket_file=/tmp/vineyard.sock

########################################################
# Clone or update the latest test datasets from github.
# Arguments:
#   None
########################################################
function get_test_data() {
  if [[ -z ${test_dir} ]]; then
    test_dir="/tmp/gstest"
  fi
  echo ${test_dir}
  if [[ -d ${test_dir} ]]; then
    cd ${test_dir}
    git pull
    cd -
  else
    git clone -b master --single-branch --depth=1 https://github.com/7br/gstest.git ${test_dir}
  fi
}

########################################################
# Start vineyard server
# Arguments:
#   None
# !!!WARNING!!!:
#   Kill all started vineyardd and etcd
########################################################
function start_vineyard() {
  pushd "${ENGINE_HOME}/build"
  pkill vineyardd || true
  pkill etcd || true
  echo "[INFO] vineyardd will using the socket_file on ${socket_file}"

  timestamp=$(date +%Y-%m-%d_%H-%M-%S)
  vineyardd \
    --socket ${socket_file} \
    --size 2000000000 \
    --etcd_prefix "${timestamp}" \
    --etcd_endpoint=http://127.0.0.1:3457 &
  set +m
  sleep 5
  info "vineyardd started."
  popd
}


########################################################
# Verify the results with an exactly match.
# Arguments:
#   - correct result file
########################################################
function exact_verify() {
  cat ./test_output/* | sort -k1n >./test_output_tmp.res
  if ! cmp ./test_output_tmp.res "$1" >/dev/null 2>&1; then
    err "Failed to pass the exactly match for the results of $1"
    exit 1
  else
    rm -rf ./test_output/*
    rm -rf ./test_output_tmp.res
    info "Passed the exactly match for the results of $1"
  fi
}

########################################################
# Run the MPI tests
# Arguments:
#   - num_of_process.
#   - executable.
#   - rest args.
########################################################
function run() {
  num_of_process=$1
  shift
  executable=$1
  shift

  cmd="${cmd_prefix} -n ${num_of_process} --host localhost:${num_of_process} ${executable} $*"
  echo "${cmd}"
  eval "${cmd}"
  info "finished running on ${num_of_process} workers."
}


########################################################
# Run apps over property graphs on vineyard.
# Arguments:
#   - num_of_process.
#   - executable.
#   - rest args.
########################################################
function run_vy() {
  num_procs=$1
  shift
  executable=$1
  shift
  socket_file=$1
  shift
  e_label_num=$1
  shift
  e_prefix=$1
  shift
  v_label_num=$1
  shift
  v_prefix=$1
  shift

  cmd="${cmd_prefix} -n ${num_procs} --host localhost:${num_procs} ${executable} ${socket_file}"

  cmd="${cmd} ${e_label_num}"
  for ((e=0;e<e_label_num;++e))
  do
    cmd="${cmd} '"
    first=true
    for ((src=0;src<v_label_num;src++))
    do
      for ((dst=0;dst<v_label_num;++dst))
      do
	if [ "$first" = true ]
        then
          first=false
          cmd="${cmd}${e_prefix}_${src}_${dst}_${e}#src_label=v${src}&dst_label=v${dst}&label=e${e}"
        else
          cmd="${cmd};${e_prefix}_${src}_${dst}_${e}#src_label=v${src}&dst_label=v${dst}&label=e${e}"
	fi
      done
    done
    cmd="${cmd}'"
  done

  cmd="${cmd} ${v_label_num}"
  for ((i = 0; i < v_label_num; i++)); do
    cmd="${cmd} ${v_prefix}_${i}#label=v${i}"
  done

  cmd="${cmd} $*"

  echo "${cmd}"
  eval "${cmd}"
  info "Finished running app ${executable} with vineyard."
}

########################################################
# Run apps over property graphs on vineyard.
# Arguments:
#   - num_of_process.
#   - executable.
#   - rest args.
########################################################
function run_vy_2() {
  num_procs=$1
  shift
  executable=$1
  shift
  socket_file=$1
  shift
  label_num=$1
  shift
  e_prefix=$1
  shift
  v_prefix=$1
  shift

  cmd="${cmd_prefix} -n ${num_procs} --host localhost:${num_procs} ${executable} ${socket_file}"

  cmd="${cmd} ${label_num}"
  for ((i = 0; i < label_num; i++)); do
    cmd="${cmd} '${e_prefix}_${i}#src_label=v${i}&dst_label=v${i}&label=e${i}'"
  done

  cmd="${cmd} ${label_num}"
  for ((i = 0; i < label_num; i++)); do
    cmd="${cmd} '${v_prefix}_${i}#label=v${i}'"
  done

  cmd="${cmd} $*"

  echo "${cmd}"
  eval "${cmd}"

  info "Finished running app ${executable} with vineyard."
}


function run_sampling_path() {
  num_procs=$1
  shift
  executable=$1
  shift
  socket_file=$1
  shift
  data_dir=$1
  shift

  cmd="${cmd_prefix} -n ${num_procs} --host localhost:${num_procs} ${executable} ${socket_file}"

  cmd="${cmd} 5"
  cmd="${cmd} '${data_dir}/sampling_path_1000_e_0#src_label=v0&dst_label=v1&label=e0'"
  cmd="${cmd} '${data_dir}/sampling_path_1000_e_1#src_label=v0&dst_label=v2&label=e1'"
  cmd="${cmd} '${data_dir}/sampling_path_1000_e_2#src_label=v0&dst_label=v2&label=e2'"
  cmd="${cmd} '${data_dir}/sampling_path_1000_e_3#src_label=v0&dst_label=v2&label=e3'"
  cmd="${cmd} '${data_dir}/sampling_path_1000_e_4#src_label=v1&dst_label=v2&label=e4'"

  cmd="${cmd} 3"
  cmd="${cmd} '${data_dir}/sampling_path_1000_v_0#label=v0'"
  cmd="${cmd} '${data_dir}/sampling_path_1000_v_1#label=v1'"
  cmd="${cmd} '${data_dir}/sampling_path_1000_v_2#label=v2'"

  cmd="${cmd} $*"

  echo "${cmd}"
  eval "${cmd}"
  info "Finished running sampling_path with vineyard."
}

function run_lpa() {
  num_procs=$1
  shift
  executable=$1
  shift
  socket_file=$1
  shift
  e_label_num=$1
  shift
  e_prefix=$1
  shift
  v_label_num=$1
  shift
  v_prefix=$1
  shift

  cmd="${cmd_prefix} -n ${num_procs} --host localhost:${num_procs} ${executable} ${socket_file}"

  cmd="${cmd} ${e_label_num}"
  for ((i = 0; i < e_label_num; i++)); do
    cmd="${cmd} '${e_prefix}_${i}#src_label=v0&dst_label=v1&label=e${i}'"
  done

  cmd="${cmd} ${v_label_num}"
  for ((i = 0; i < v_label_num; i++)); do
    cmd="${cmd} '${v_prefix}_${i}#label=v${i}'"
  done

  cmd="${cmd} $*"

  echo "${cmd}"
  eval "${cmd}"
  info "Finished running lpa on property graph."
}


# The results of bfs and sssp_path is are non-determinstic. 
# The result of bfs is random because diamond-shaped subgraph, 
# e.g. there are four edges: 1->2, 1->3, 2->4, 3->4. 
# For vertex 4, result of bfs may come from vertex 2 or vertex 3.
#
# The result of sssp_path is random when there are more than one paths for the
# vertex and the cost these paths is all the minimum.
#
# sssp_average_length is a time-consuming app, so we skip it for graph p2p.

declare -a apps=(
  "sssp" 
  "sssp_has_path" 
  "sssp_path"
  "cdlp_auto" 
  "sssp_auto" 
  "wcc_auto" 
  "lcc_auto" 
  "bfs_auto" 
  "pagerank_auto"
  "kcore" 
  "hits" 
  "bfs" 
  "avg_clustering" 
  "transitivity" 
  "triangles"
  # "sssp_average_length"
)

# these algorithms need to check with directed flag
declare -a apps_with_directed=(
  "katz" 
  "eigenvector" 
  "degree_centrality" 
  "clustering"
)

cmd_prefix="mpirun"
if ompi_info; then
  echo "Using openmpi"
  cmd_prefix="${cmd_prefix} --allow-run-as-root"
fi

pushd "${ENGINE_HOME}"/build

get_test_data

for app in "${apps[@]}"; do
  run ${np} ./run_app --vfile "${test_dir}"/p2p-31.v --efile "${test_dir}"/p2p-31.e --application "${app}" --out_prefix ./test_output --sssp_source=6 --sssp_target=10 --bfs_source=6
  exact_verify "${test_dir}"/p2p-31-"${app}"
done

for app in "${apps_with_directed[@]}"; do
  run ${np} ./run_app --vfile "${test_dir}"/p2p-31.v --efile "${test_dir}"/p2p-31.e --application "${app}" --out_prefix ./test_output --directed
  exact_verify "${test_dir}"/p2p-31-"${app}"
done

start_vineyard

run_vy ${np} ./run_vy_app "${socket_file}" 2 "${test_dir}"/new_property/v2_e2/twitter_e 2 "${test_dir}"/new_property/v2_e2/twitter_v 0 
run_vy_2 ${np} ./run_vy_app "${socket_file}" 4 "${test_dir}"/projected_property/twitter_property_e "${test_dir}"/projected_property/twitter_property_v 1
run_lpa ${np} ./run_vy_app "${socket_file}" 1 "${test_dir}"/property/lpa_dataset/lpa_3000_e 2 "${test_dir}"/property/lpa_dataset/lpa_3000_v 0 1 lpa 
run_sampling_path 2 ./run_vy_app "${socket_file}" "${test_dir}"/property/sampling_path 0 1 sampling_path 0-0-1-4-2 

run_vy ${np} ./run_pregel_app "${socket_file}" 2 "${test_dir}"/new_property/v2_e2/twitter_e 2 "${test_dir}"/new_property/v2_e2/twitter_v
rm -rf ./test_output/*
cp ./outputs_pregel_sssp/* ./test_output
exact_verify "${test_dir}"/twitter-sssp-4

run ${np} ./run_pregel_app tc "${test_dir}"/p2p-31.e "${test_dir}"/p2p-31.v ./test_output 
exact_verify "${test_dir}/p2p-31"-triangles

if [[ "${RUN_JAVA_TESTS}" == "ON" ]];
then
  if [[ "${USER_JAR_PATH}"x != ""x ]]
  then
    echo "Running Java tests..."
    run_vy ${np} ./run_java_app "${socket_file}" 2 "${test_dir}"/new_property/v2_e2/twitter_e 2 "${test_dir}"/new_property/v2_e2/twitter_v 0 0 1 com.alibaba.graphscope.example.property.sssp.ParallelPropertySSSPVertexData
  fi
fi



info "Passed all tests for GraphScope analytical engine."

