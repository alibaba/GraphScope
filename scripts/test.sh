#!/usr/bin/env bash
#
# A script to test graphscope

set -euo pipefail

# initialize variables
graphscope_home="$( cd "$(dirname "$0")/.." >/dev/null 2>&1 ; pwd -P )"
version=$(cat ${graphscope_home}/VERSION)

platform=$(awk -F= '/^NAME/{print $2}' /etc/os-release)

test_dir=""
gs_image="registry.cn-hongkong.aliyuncs.com/graphscope/graphscope:${version}"
test_GIE=0
test_on_k8s=0

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

##########################
# Print out usage messages
# Globals:
#   None
# Arguments:
#   None
##########################
function usage() {
  cat <<EOF

   Usage: $./test.sh [-t TEST_DIR] [-i DOCKER_IMAGE] [--all] [--gie] [--python]

   optional arguments:
     -h, --help                             show this help message and exit
     -t, --test_dir DIRECTORY               the existing test data dir.
     -g, --gs_image DOCKER_IMAGE            GraphScope engine's docker image uri
     --all                                  run all tests
     --gie                                  run graph interactive engine tests
     --python                               run python tests

EOF
}

##########################
# Download test datatset.
# If test directory is not set,
# then clone repo gstest to directory
# Globals:
#   test_dir
# Arguments:
#   None
##########################
function get_test_data() {
  if [[ -z ${test_dir} ]]; then
    test_dir="/tmp/gstest"
  fi
  if [[ -d ${test_dir} ]]; then
    cd ${test_dir}
    git pull
    cd -
  else
    info "Cloning gstest to ${test_dir}"
    git clone -b master --single-branch --depth=1 https://github.com/7br/gstest.git ${test_dir}
  fi
}


##########################
# Run GIE tests
# Globals:
#   graphscope_home
#   gs_image
# Arguments:
#   None
# Outputs:
#   Print "Pass..." to STDOUT
#   Print "Fail..." to STDERR
##########################
function run_gie_test() {
  pushd "${graphscope_home}"/interactive_engine/tests
  ./function_test.sh 8111 1 ${gs_image}

  res=$(grep "failed" ./target/surefire-reports/testng-results.xml)
  if [[ ${res} == *"failed=\"0\""* ]]; then
        info "Passed GIE tests"
        popd
        exit 0
  else
        err "Failed to pass GIE tests"
        err "${res}"
        popd
        exit 1
  fi
}

##########################
# Run k8s tests
# Globals:
#   graphscope_home
#   test_dir
# Arguments:
#   None
##########################
function run_k8s_test() {
  export GS_IMAGE="${gs_image}"  # let session use specified image tag

  python3 -m pytest -v "${graphscope_home}"/python/tests/kubernetes/
}

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
  -g | --gs_image)
    gs_image="$2"
    shift
    shift
    ;;
  --all)
    test_GIE=1
    test_on_k8s=1
    shift
    ;;
  --gie)
    test_GIE=1
    shift
    ;;
  --python)
    test_on_k8s=1
    shift
    ;;
  *) # unknown option
    usage
    exit 1
    ;;
  esac
done

if ((test_GIE == 0 && test_on_k8s == 0)); then
  usage
  exit
fi

get_test_data

if ((test_GIE == 1)); then
  run_gie_test
fi
if ((test_on_k8s == 1)); then
  run_k8s_test
fi
