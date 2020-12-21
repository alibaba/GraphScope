#!/usr/bin/env bash
#
# A script to install dependencies for GraphScope user on WSL2

set -e
set -x
set -o pipefail

platform=$(awk -F= '/^NAME/{print $2}' /etc/os-release)

##########################
# Install packages
# Globals:
#   platform
# Arguments:
#   None
##########################
function install_dependencies() {
  if [[ "${platform}" == *"Ubuntu"* ]]; then
    sudo apt-get update -y
    sudo apt-get install -y git
    sudo apt-get install -y docker.io
    sudo apt-get install -y conntrack curl lsof
    sudo apt-get install -y python3-pip
    sudo apt-get clean
  else
    echo "Only support Ubuntu on WSL"
    exit 1
  fi

  pip3 install -U pip --user
  pip3 install graphscope vineyard wheel --user

  K8S_VERSION=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)

  curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/"${K8S_VERSION}"/bin/linux/amd64/kubectl && \
  chmod +x kubectl && sudo mv kubectl /usr/local/bin/ && sudo ln /usr/local/bin/kubectl /usr/bin/kubectl || true

  curl -Lo kind https://kind.sigs.k8s.io/dl/v0.9.0/kind-linux-amd64
  chmod +x kind && sudo mv kind /usr/local/bin/ && sudo ln /usr/local/bin/kind /usr/bin/kind || true
}

install_dependencies

# start docker daemon if docker not running.
if ! sudo docker info >/dev/null 2>&1; then
    sudo dockerd > /dev/null& || true
fi

# launch k8s cluster
echo "$(date '+%Y-%m-%d %H:%M:%S') launch k8s cluster"
curl -Lo config-with-mounts.yaml https://kind.sigs.k8s.io/examples/config-with-mounts.yaml
# mount $HOME dir to cluster container, which is kind-control-plane
sed -i 's@/path/to/my/files/@'"${HOME}"'@g; s@/files@'"${HOME}"'@g' ./config-with-mounts.yaml  || true
sudo kind create cluster --config config-with-mounts.yaml
sudo cp -rn /root/.kube ${HOME} || true
sudo chown -R "$(id -u)":"$(id -g)" "${HOME}"/.kube || true

# pull images(graphscope, etcd, maxgraph_standalone_manager)
echo "$(date '+%Y-%m-%d %H:%M:%S') pull graphscope image and etcd image"
sudo docker pull registry.cn-hongkong.aliyuncs.com/graphscope/graphscope:latest || true
sudo docker pull registry.cn-hongkong.aliyuncs.com/graphscope/maxgraph_standalone_manager:1.0 || true
sudo docker pull zookeeper:3.4.14 || true
sudo docker pull quay.io/coreos/etcd:v3.4.13 || true

# load images into cluster
echo "$(date '+%Y-%m-%d %H:%M:%S') load images into cluster"
sudo kind load registry.cn-hongkong.aliyuncs.com/graphscope/graphscope:latest || true
sudo kind load registry.cn-hongkong.aliyuncs.com/graphscope/maxgraph_standalone_manager:1.0 || true
sudo kind load zookeeper:3.4.14 || true
sudo kind load quay.io/coreos/etcd:v3.4.13 || true

set +x
set +e
set +o pipefail
