#!/usr/bin/env bash
#
# A script to install dependencies for GraphScope user

set -e
set -x
set -o pipefail

platform=$(awk -F= '/^NAME/{print $2}' /etc/os-release)
is_in_wsl=false && [[ ! -z "${IS_WSL}" || ! -z "${WSL_DISTRO_NAME}" ]] && is_in_wsl=true

##########################
# Install packages
# Globals:
#   platform
# Arguments:
#   None
##########################
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
  else
    echo "Only support Ubuntu and CentOS"
    exit 1
  fi

  pip3 install -U pip --user
  pip3 install graphscope vineyard wheel --user

  K8S_VERSION=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)

  curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/"${K8S_VERSION}"/bin/linux/amd64/kubectl && \
  chmod +x kubectl && sudo mv kubectl /usr/local/bin/ && sudo ln /usr/local/bin/kubectl /usr/bin/kubectl || true
  echo "$(date '+%Y-%m-%d %H:%M:%S') kubectl ${K8S_VERSION} installed."

  if [[ "${is_in_wsl}" = false ]]; then
      curl -Lo minikube https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64 && \
      chmod +x minikube && sudo mv minikube /usr/local/bin/ && sudo ln /usr/local/bin/minikube /usr/bin/minikube || true
      echo "$(date '+%Y-%m-%d %H:%M:%S') minikube ${K8S_VERSION} installed."
  else
      curl -Lo kind https://kind.sigs.k8s.io/dl/v0.9.0/kind-linux-amd64 && \
      chmod +x kind && sudo mv kind /usr/local/bin/ && sudo ln /usr/local/bin/kind /usr/bin/kind || true
      echo "$(date '+%Y-%m-%d %H:%M:%S') kind v0.9.0 installed."
  fi
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
  if [[ "${is_in_wsl}" = false ]]; then
    export CHANGE_MINIKUBE_NONE_USER=true
    sudo sysctl fs.protected_regular=0 || true
    sudo minikube start --vm-driver=none --kubernetes-version="${K8S_VERSION}"

    sudo cp -r /root/.kube /root/.minikube "${HOME}" || true
    sudo chown -R "$(id -u)":"$(id -g)" "${HOME}"/.minikube || true
    sudo chown -R "$(id -u)":"$(id -g)" "${HOME}"/.kube || true
    sed -i 's@/root@'"${HOME}"'@g' "${HOME}"/.kube/config || true
    minikube update-context
  else
    curl -Lo config-with-mounts.yaml https://kind.sigs.k8s.io/examples/config-with-mounts.yaml
    # mount $HOME dir to cluster container, which is kind-control-plane
    sed -i 's@/path/to/my/files/@'"${HOME}"'@g; s@/files@'"${HOME}"'@g' ./config-with-mounts.yaml  || true
    sudo kind create cluster --config config-with-mounts.yaml
    sudo cp -r /root/.kube ${HOME} || true
    sudo chown -R "$(id -u)":"$(id -g)" "${HOME}"/.kube || true
  fi
  echo "$(date '+%Y-%m-%d %H:%M:%S') cluster is lauched successfully."
}

function pull_images() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') pulling GraphScope images."
  sudo docker pull registry.cn-hongkong.aliyuncs.com/graphscope/graphscope:latest || true
  sudo docker pull registry.cn-hongkong.aliyuncs.com/graphscope/maxgraph_standalone_manager:latest || true
  sudo docker pull zookeeper:3.4.14 || true
  sudo docker pull quay.io/coreos/etcd:v3.4.13 || true
  echo "$(date '+%Y-%m-%d %H:%M:%S') images pulled successfully."

  if [[ "${is_in_wsl}" = true ]]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') loading images into kind cluster."
    sudo kind load registry.cn-hongkong.aliyuncs.com/graphscope/graphscope:latest || true
    sudo kind load registry.cn-hongkong.aliyuncs.com/graphscope/maxgraph_standalone_manager:latest || true
    sudo kind load zookeeper:3.4.14 || true
    sudo kind load quay.io/coreos/etcd:v3.4.13 || true
    echo "$(date '+%Y-%m-%d %H:%M:%S') images loaded."
  fi
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

install_dependencies

start_docker

launch_k8s_cluster

pull_images

echo "The script has successfully prepared an environment for GraphScope."
echo "Now you are ready to have fun with GraphScope."

set +x
set +e
set +o pipefail
