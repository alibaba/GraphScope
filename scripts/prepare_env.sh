#!/usr/bin/env bash
#
# A script to install dependencies for GraphScope user

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

  curl -Lo minikube https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64 && \
  chmod +x minikube && sudo mv minikube /usr/local/bin/ && sudo ln /usr/local/bin/minikube /usr/bin/minikube || true
}

install_dependencies

# start docker daemon
sudo systemctl start docker


# launch k8s cluster
export CHANGE_MINIKUBE_NONE_USER=true
echo "$(date '+%Y-%m-%d %H:%M:%S') launch k8s cluster"
sudo sysctl fs.protected_regular=0 || true
sudo minikube start --vm-driver=none --kubernetes-version="${K8S_VERSION}"

sudo cp -r /root/.kube /root/.minikube "${HOME}" || true
sudo chown -R "$(id -u)":"$(id -g)" "${HOME}"/.minikube || true
sudo chown -R "$(id -u)":"$(id -g)" "${HOME}"/.kube || true
sed -i 's@/root@'"${HOME}"'@g' "${HOME}"/.kube/config || true

minikube update-context

# pull images(graphscope, etcd, maxgraph_standalone_manager)
echo "$(date '+%Y-%m-%d %H:%M:%S') pull graphscope image and etcd image"
sudo docker pull registry.cn-hongkong.aliyuncs.com/graphscope/graphscope:latest || true
sudo docker pull registry.cn-hongkong.aliyuncs.com/graphscope/maxgraph_standalone_manager:1.0 || true
sudo docker pull zookeeper:3.4.14 || true
sudo docker pull quay.io/coreos/etcd:v3.4.13 || true

set +x
set +e
set +o pipefail
