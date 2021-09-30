#!/usr/bin/env bash
#
# A script to prepare an local Kubernetes environment for GraphScope users.


# color
readonly RED="\033[0;31m"
readonly YELLOW="\033[1;33m"
readonlyNC="\033[0m" # No Color
# emoji
readonly MUSCLE="\U1f4aa"

IS_IN_WSL=false && [[ ! -z "${IS_WSL}" || ! -z "${WSL_DISTRO_NAME}" ]] && IS_IN_WSL=true
readonly IS_IN_WSL
PLATFORM=
OS_VERSION=
image_tag=

##########################
# Output useage information.
# Globals:
#   None
# Arguments:
#   None
##########################
usage() {
cat <<END
  A script to prepare a local Kubernetes environment for GraphScope.

  Usage: prepare_env [options]
  Options:
    -h, --help           output help information
    --verbose            output the debug log
    --overwrite          overwrite the existed kubernetes config
  Note:
    The script only available on Ubuntu 18+ or CenOS 7+.
END
}

err() {
  echo -e "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: [${RED}ERROR${NC}] $*" >&2
}

warning() {
  echo -e "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: [${YELLOW}WARNING${NC}] $*" >&1
}

log() {
  echo -e "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: [${MUSCLE}] $*" >&1
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
    PLATFORM="${NAME}"
    OS_VERSION="${VERSION_ID}"
  elif type lsb_release >/dev/null 2>&1; then
    # linuxbase.org
    PLATFORM=$(lsb_release -si)
    OS_VERSION=$(lsb_release -sr)
  elif [ -f /etc/lsb-release ]; then
    # For some versions of Debian/Ubuntu without lsb_release command
    . /etc/lsb-release
    PLATFORM="${DISTRIB_ID}"
    OS_VERSION="${DISTRIB_RELEASE}"
  elif [ -f /etc/debian_version ]; then
    # Older Debian/Ubuntu/etc.
    PLATFORM=Debian
    OS_VERSION=$(cat /etc/debian_version)
  elif [ -f /etc/centos-release ]; then
    # Older Red Hat, CentOS, etc.
    PLATFORM=CentOS
    OS_VERSION=$(cat /etc/centos-release | sed 's/.* \([0-9]\).*/\1/')
  else
    # Fall back to uname, e.g. "Linux <version>", also works for BSD, Darwin, etc.
    PLATFORM=$(uname -s)
    OS_VERSION=$(uname -r)
  fi
  readonly PLATFORM
  readonly OS_VERSION
}

##########################
# Check the compatibility of PLATFORM and script.
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
  if [[ "${IS_IN_WSL}" == true && -z "${WSL_INTEROP}" ]]; then
    err "The platform is WSL1. GraphScope not support to run on WSL1, please use WSL2."
    exit 1
  fi

  if [[ "${PLATFORM}" != *"Ubuntu"* && "${PLATFORM}" != *"CentOS"* ]]; then
    err "The platform is not Ubuntu or CentOs. This script is only available on Ubuntu/CentOS"
    exit 1
  fi

  if [[ "${PLATFORM}" == *"Ubuntu"* && "$(echo ${OS_VERSION} | sed 's/\([0-9]\)\([0-9]\).*/\1\2/')" -lt "18" ]]; then
    err "The version of Ubuntu is ${OS_VERSION}. this script requires Ubuntu 18 or greater."
    exit 1
  fi

  if [[ "${PLATFORM}" == *"CentOS"* && "${OS_VERSION}" -lt "7" ]]; then
    err "The version of CentOS is ${OS_VERSION}. this script requires CentOS 7 or greater."
    exit 1
  fi

  log "Preparing environment on ${PLATFORM} ${OS_VERSION}"
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
  if [[ "${PLATFORM}" == *"Ubuntu"* ]]; then
    sudo apt-get update -y
    sudo apt-get install -y git
    sudo apt-get install -y docker.io
    sudo apt-get install -y conntrack curl lsof
    sudo apt-get install -y python3-pip
    sudo apt-get clean
  elif [[ "${PLATFORM}" == *"CentOS"* ]]; then
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

  # image_tag need to consistent with graphscope client version
  if [ -z "${image_tag}" ]; then
    image_tag=$(python3 -c "import graphscope; print(graphscope.__version__)")
  fi
  readonly image_tag

  log "Install kubectl."
  K8S_VERSION=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)

  curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/"${K8S_VERSION}"/bin/linux/amd64/kubectl && \
  chmod +x kubectl && sudo mv kubectl /usr/local/bin/ && sudo ln /usr/local/bin/kubectl /usr/bin/kubectl || true

  log "Install kind."
  curl -Lo ./kind https://github.com/kubernetes-sigs/kind/releases/download/v0.10.0/kind-linux-amd64
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
    if [[ "${IS_IN_WSL}" = false ]]; then
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
  sudo docker pull registry.cn-hongkong.aliyuncs.com/graphscope/graphscope:${image_tag} || true
  sudo docker pull quay.io/coreos/etcd:v3.4.13 || true
  log "GraphScope images pulled successfully."

  log "Loading images into kind cluster."
  sudo kind load docker-image registry.cn-hongkong.aliyuncs.com/graphscope/graphscope:${image_tag} || true
  sudo kind load docker-image quay.io/coreos/etcd:v3.4.13 || true
  log "GraphScope images loaded into kind cluster successfully."
}

##########################
# Main function of the script.
# Globals:
#   VERBOSE
#   HOME
# Arguments:
#   None
# Outputs:
#   output log to stdout, output error to stderr.
##########################
main() {
  if [ ${VERBOSE} = true ]; then
    set -x
  fi

  if [[ ${OVERWRITE} = false && -f "${HOME}/.kube/config" ]]; then
    warning_msg="We found existing kubernetes config, seems that you already
    have a ready kubernetes cluster. If you do want to reset the kubernetes
    environment, please retry the script with '--overwrite' option."
    warning ${warning_msg}
    exit 0
  fi

  get_os_version

  check_os_compatibility

  install_dependencies

  start_docker

  launch_k8s_cluster

  pull_images

  msg="The script has successfully prepared an environment for GraphScope.
  Now you are ready to have fun with GraphScope."
  log ${msg}

  if [ ${VERBOSE} = true ]; then
    set +x
  fi
}

# parse argv
# TODO(acezen): when option is not illegal, warning and output usage.
VERBOSE=false
OVERWRITE=false
while test $# -ne 0; do
  arg=$1; shift
  case $arg in
    -h|--help) usage; exit ;;
    --verbose) VERBOSE=true; ;;
    --overwrite) OVERWRITE=true; ;;
    *)
      ;;
  esac
done
readonly VERBOSE
readonly OVERWRITE

set -e
set -o pipefail
main
set +e
set +o pipefail
