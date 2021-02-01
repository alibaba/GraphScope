#!/usr/bin/env bash
#
# A script to install dependencies for GraphScope user

set -e
set -x
set -o pipefail

graphscope_home="$( cd "$(dirname "$0")/.." >/dev/null 2>&1 ; pwd -P )"
version=$(cat ${graphscope_home}/VERSION)

is_in_wsl=false && [[ ! -z "${IS_WSL}" || ! -z "${WSL_DISTRO_NAME}" ]] && is_in_wsl=true

##########################
# Install packages
# Globals:
#   platform
# Arguments:
#   None
##########################

# https://unix.stackexchange.com/questions/6345/how-can-i-get-distribution-name-and-version-number-in-a-simple-shell-script
function get_os_version() {
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

function check_os_compatibility() {
  if [[ "${is_in_wsl}" == true && -z "${WSL_INTEROP}" ]]; then
    echo "GraphScope not support to run on WSL1, please use WSL2."
    exit 1
  fi

  if [[ "${platform}" != *"Ubuntu"* && "${platform}" != *"CentOS"* ]]; then
    echo "This script is only available on Ubuntu/CentOS"
    exit 1
  fi

  if [[ "${platform}" == *"Ubuntu"* && "$(echo ${os_version} | sed 's/\([0-9]\)\([0-9]\).*/\1\2/')" -lt "18" ]]; then
    echo "This script requires Ubuntu 18 or greater."
    exit 1
  fi

  if [[ "${platform}" == *"CentOS"* && "${os_version}" -lt "7" ]]; then
    echo "This script requires CentOS 7 or greater."
    exit 1
  fi

  echo "$(date '+%Y-%m-%d %H:%M:%S') preparing environment on '${platform}' '${os_version}'"
}

function check_dependencies_version() {
  # python
  if ! hash python3; then
    echo "Python3 is not installed"
    exit 1
  fi
  ver=$(python3 -V 2>&1 | sed 's/.* \([0-9]\).\([0-9]\).*/\1\2/')
  if [ "$ver" -lt "36" ]; then
    echo "GraphScope requires python 3.6 or greater."
    exit 1
  fi
}

function install_dependencies() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') install dependencies."
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

  K8S_VERSION=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)

  curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/"${K8S_VERSION}"/bin/linux/amd64/kubectl && \
  chmod +x kubectl && sudo mv kubectl /usr/local/bin/ && sudo ln /usr/local/bin/kubectl /usr/bin/kubectl || true
  echo "$(date '+%Y-%m-%d %H:%M:%S') kubectl ${K8S_VERSION} installed."

  curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.10.0/kind-linux-amd64
  chmod +x kind && sudo mv kind /usr/local/bin/ && sudo ln /usr/local/bin/kind /usr/bin/kind || true
  echo "$(date '+%Y-%m-%d %H:%M:%S') kind v0.10.0 installed."
}

function start_docker() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') starting doker daemon."
  # start docker daemon if docker not running.
  if ! sudo docker info >/dev/null 2>&1; then
    if [[ "${is_in_wsl}" = false ]]; then
      sudo systemctl start docker
    else
      sudo dockerd > /dev/null&
    fi
  fi
  echo "$(date '+%Y-%m-%d %H:%M:%S') docker started."
}

function launch_k8s_cluster() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') launching k8s cluster"
  curl -Lo config-with-mounts.yaml https://kind.sigs.k8s.io/examples/config-with-mounts.yaml
  # mount $HOME dir to cluster container, which is kind-control-plane
  sed -i 's@/path/to/my/files/@'"${HOME}"'@g; s@/files@'"${HOME}"'@g' ./config-with-mounts.yaml  || true
  sudo kind create cluster --config config-with-mounts.yaml
  sudo chown -R "$(id -u)":"$(id -g)" "${HOME}"/.kube || true
  echo "$(date '+%Y-%m-%d %H:%M:%S') cluster is lauched successfully."
}

function pull_images() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') pulling GraphScope images."
  sudo docker pull registry.cn-hongkong.aliyuncs.com/graphscope/graphscope:${version} || true
  sudo docker pull registry.cn-hongkong.aliyuncs.com/graphscope/maxgraph_standalone_manager:${version} || true
  sudo docker pull zookeeper:3.4.14 || true
  sudo docker pull quay.io/coreos/etcd:v3.4.13 || true
  echo "$(date '+%Y-%m-%d %H:%M:%S') images pulled successfully."

  echo "$(date '+%Y-%m-%d %H:%M:%S') loading images into kind cluster."
  sudo kind load docker-image registry.cn-hongkong.aliyuncs.com/graphscope/graphscope:${version} || true
  sudo kind load docker-image registry.cn-hongkong.aliyuncs.com/graphscope/maxgraph_standalone_manager:${version} || true
  sudo kind load docker-image zookeeper:3.4.14 || true
  sudo kind load docker-image quay.io/coreos/etcd:v3.4.13 || true
  echo "$(date '+%Y-%m-%d %H:%M:%S') images loaded."

}

if [ -f "${HOME}/.kube/config" ];
then
    echo "We found existing kubernetes config, seems that you already have a ready kubernetes cluster"
    echo "WARNING: If you do want to reset the kubernetes environment, please delete the existing config by"
    echo ""
    echo "    rm -rf ${HOME}/.kube"
    echo ""
    echo "and retry this script again."
    exit 0
fi

get_os_version

check_os_compatibility

install_dependencies

start_docker

launch_k8s_cluster

pull_images

echo "The script has successfully prepared an environment for GraphScope."
echo "Now you are ready to have fun with GraphScope."

set +x
set +e
set +o pipefail
