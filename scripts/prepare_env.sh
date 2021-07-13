#!/usr/bin/env bash
#
# A script to install dependencies for GraphScope user

set -e
# set -x
# set -o pipefail

# define color
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

GRAPHSCOPE_DIR="$( cd "$(dirname "$0")/.." >/dev/null 2>&1 ; pwd -P )"
VRESION=$(cat ${graphscope_home}/VERSION)
IS_IN_WSL=false && [[ ! -z "${IS_WSL}" || ! -z "${WSL_DISTRO_NAME}" ]] && IS_IN_WSL=true
PLATFORM=
OS_VERSION=
VERBOSE=false
OVERWRITE=false

#
# Output usage information.
#

usage() {
cat <<END
  Usage: prepare_env [options]
  Options:
    -V, --version        output program version
    -h, --help           output help information
    --verbose            output the debug log
    --overwrite          overwrite the existed kube config
  Notes:
    The script can only available on Ubuntu 18+ or CenOS 7+.
END
}

err() {
  echo -e "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: ${RED}ERROR${NC}$*" >&2
}

warning()
  echo -e "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: ${YELLOW}WARNING${NC}$*" >&1
}

log() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&1
}

##########################
# Get os platform and version
# Globals:
#   PLATFORM
#   OS_VERSION
# Arguments:
#   None
# Refer:
# https://unix.stackexchange.com/questions/6345/how-can-i-get-distribution-name-and-version-number-in-a-simple-shell-script
##########################
get_os_version() {
  if [ -f /etc/os-release ]; then
    # freedesktop.org and systemd
    . /etc/os-release
    platform="${NAME}"
    os_version="${VERSION_ID}"
  elif type lsb_release >/dev/null 2>&1; then
    # linuxbase.org
    platform=$(lsb_release -si)
    os_version=$(lsb_release -sr)
  elif [ -f /etc/lsb-release ]; then
    # For some versions of Debian/Ubuntu without lsb_release command
    . /etc/lsb-release
    platform="${DISTRIB_ID}"
    os_version="${DISTRIB_RELEASE}"
  elif [ -f /etc/debian_version ]; then
    # Older Debian/Ubuntu/etc.
    platform=Debian
    os_version=$(cat /etc/debian_version)
  elif [ -f /etc/centos-release ]; then
    # Older Red Hat, CentOS, etc.
    platform=CentOS
    os_version=$(cat /etc/centos-release | sed 's/.* \([0-9]\).*/\1/')
  else
    # Fall back to uname, e.g. "Linux <version>", also works for BSD, Darwin, etc.
    platform=$(uname -s)
    os_version=$(uname -r)
  fi
}

##########################
# Check the compatibility of platform and script.
# Globals:
#   None
# Arguments:
#   None
# Outputs:
#   Writes error message to stderr
# Returns:
#   non-zero on error.
##########################
check_os_compatibility() {
  if [[ "${is_in_wsl}" == true && -z "${WSL_INTEROP}" ]]; then
    err "The platfrom is WSL1. GraphScope not support to run on WSL1, please use WSL2."
    exit 1
  fi

  if [[ "${platform}" != *"Ubuntu"* && "${platform}" != *"CentOS"* ]]; then
    err "The platform is not Ubuntu or CentOs. This script is only available on Ubuntu/CentOS"
    exit 1
  fi

  if [[ "${platform}" == *"Ubuntu"* && "$(echo ${os_version} | sed 's/\([0-9]\)\([0-9]\).*/\1\2/')" -lt "18" ]]; then
    err "The version of Ubuntu is ${os_version}. this script requires Ubuntu 18 or greater."
    exit 1
  fi

  if [[ "${platform}" == *"CentOS"* && "${os_version}" -lt "7" ]]; then
    err "The version of CentOS is ${os_version}. this script requires CentOS 7 or greater."
    exit
  fi

  log "Preparing environment on '${platform}' '${os_version}'"
}

##########################
# Check the version of dependency.
# Globals:
#   None
# Arguments:
#   None
# Outputs:
#   Writes error message to stderr
# Returns:
#   non-zero on error.
##########################
check_dependencies_version() {
  # python
  if ! hash python3; then
    err "Python3 is not installed"
    exit 1
  fi
  ver=$(python3 -V 2>&1 | sed 's/.* \([0-9]\).\([0-9]\).*/\1\2/')
  if [ "$ver" -lt "36" ]; then
    err "GraphScope requires python 3.6 or greater. Current version is ${python3 -V}"
    exit 1
  fi
}

##########################
# Install dependencies of GraphScope.
# Globals:
#   None
# Arguments:
#   None
# Returns:
#   0 if install successfully, non-zero on error.
##########################
install_dependencies() {
  log "Install dependencies."
  if [[ "${platform}" == *"Ubuntu"* ]]; then
    sudo apt-get update -y
    sudo apt-get install -y git
    sudo apt-get install -y docker.io
    sudo apt-get install -y conntrack curl lsof
    sudo apt-get install -y python3-pip
    sudo apt-get clean
  elif [[ "${platform}" == *"CentOS"* ]]; then
    sudo yum install -y git
    sudo yum install -y python3-pip
    sudo yum install -y yum-utils curl conntrack-tools lsof
    sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
    sudo yum install -y docker-ce docker-ce-cli containerd.io
    sudo yum clean all
  fi

  check_dependencies_version

  pip3 install -U pip --user
  pip3 install graphscope vineyard wheel --user

  log "Install kubectl."
  K8S_VERSION=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)

  curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/"${K8S_VERSION}"/bin/linux/amd64/kubectl && \
  chmod +x kubectl && sudo mv kubectl /usr/local/bin/ && sudo ln /usr/local/bin/kubectl /usr/bin/kubectl || true

  log "Install kind."
  curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.10.0/kind-linux-amd64
  chmod +x kind && sudo mv kind /usr/local/bin/ && sudo ln /usr/local/bin/kind /usr/bin/kind || true
}

##########################
# Start docker daemon.
# Globals:
#   None
# Arguments:
#   None
# Returns:
#   0 if start successfully, non-zero on error.
##########################
start_docker() {
  log "Starting doker daemon."
  # start docker daemon if docker not running.
  if ! sudo docker info >/dev/null 2>&1; then
    if [[ "${is_in_wsl}" = false ]]; then
      sudo systemctl start docker
    else
      sudo dockerd > /dev/null&
    fi
  fi
  log "Docker started successfully."
}

##########################
# Launch kubenetes cluster with kind.
# Globals:
#   None
# Arguments:
#   None
# Returns:
#   0 if launched successfully, non-zero on error.
##########################
launch_k8s_cluster() {
  log "Launching k8s cluster"
  curl -Lo config-with-mounts.yaml https://kind.sigs.k8s.io/examples/config-with-mounts.yaml
  # mount $HOME dir to cluster container, which is kind-control-plane
  sed -i 's@/path/to/my/files/@'"${HOME}"'@g; s@/files@'"${HOME}"'@g' ./config-with-mounts.yaml  || true
  sudo kind create cluster --config config-with-mounts.yaml
  sudo chown -R "$(id -u)":"$(id -g)" "${HOME}"/.kube || true
  log "Cluster is launched successfully."
}

##########################
# Pull and load the GraphScope images to kind cluster.
# Globals:
#   None
# Arguments:
#   None
# Returns:
#   0 if successful, non-zero on error.
##########################
pull_images() {
  log "Pulling GraphScope images."
  sudo docker pull registry.cn-hongkong.aliyuncs.com/graphscope/graphscope:${version} || true
  sudo docker pull registry.cn-hongkong.aliyuncs.com/graphscope/maxgraph_standalone_manager:${version} || true
  sudo docker pull zookeeper:3.4.14 || true
  sudo docker pull quay.io/coreos/etcd:v3.4.13 || true
  log "GraphScope images pulled successfully."

  log "Loading images into kind cluster."
  sudo kind load docker-image registry.cn-hongkong.aliyuncs.com/graphscope/graphscope:${version} || true
  sudo kind load docker-image registry.cn-hongkong.aliyuncs.com/graphscope/maxgraph_standalone_manager:${version} || true
  sudo kind load docker-image zookeeper:3.4.14 || true
  sudo kind load docker-image quay.io/coreos/etcd:v3.4.13 || true
  log "GraphScope images loaded into kind cluster successfully."
}

main() {
  if [[ !${OVERWRITE} && -f "${HOME}/.kube/config" ]]; then
    warning_msg="We found existing kubernetes config, seems that you already
    have a ready kubernetes cluster. If you do want to reset the kubernetes
    environment, please delete the existing config by 'rm -rf ${HOME}/.kube'
    and retry this script again, or retry script with '--overwrite'"
    warning ${warning_msg}
    exit 0
  fi

  if [ ${VERBOSE} ]; then
    set -e
  fi

  get_os_version

  check_os_compatibility

  install_dependencies

  start_docker

  launch_k8s_cluster

  pull_images

  if [ ${VERBOSE} ]; then
    set +e
  fi

  log "The script has successfully prepared an environment for GraphScope."
  log "Now you are ready to have fun with GraphScope."
}

# parse argv
while test $# -ne 0; do
  arg=$1; shift
  case $arg in
    -h|--help) usage; exit ;;
    -V|--version) version; exit ;;
    --verbose) VERBOSE=true; shift ;;
    --overwrite) OVERWRITE=true; shift ;;
    *)
      ;;
  esac
done

main
set +x
# set +e
# set +o pipefail
