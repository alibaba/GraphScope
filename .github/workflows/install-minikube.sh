#!/bin/bash
set -xe

JSONPATH='{range .items[*]}{@.metadata.name}:{range @.status.conditions[*]}{@.type}={@.status};{end}{end}'
OUTPUT=`kubectl get nodes -o jsonpath="$JSONPATH" || true`
echo "kubectl testing output: ${OUTPUT}"
if [[ "${OUTPUT}" == *"Ready=True"* ]];then
  echo "Minikube running on cluster"
  exit 0
fi

# kill etcd
pkill -9 etcd || true
sleep 2
export CHANGE_MINIKUBE_NONE_USER=true

sudo apt-get -q update || true
sudo apt-get install -yq conntrack

K8S_VERSION=$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)

curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/${K8S_VERSION}/bin/linux/amd64/kubectl && \
  chmod +x kubectl && sudo mv kubectl /usr/local/bin/

curl -Lo minikube https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64 && \
  chmod +x minikube && sudo mv minikube /usr/local/bin/

sudo sysctl fs.protected_regular=0
sudo minikube start --vm-driver=none --kubernetes-version=${K8S_VERSION}

sudo rm -rf ${HOME}/.kube ${HOME}/.minikube
sudo mv /root/.kube /root/.minikube ${HOME}
sudo chown -R $(id -u):$(id -g) ${HOME}/.minikube
sudo chown -R $(id -u):$(id -g) ${HOME}/.kube
sed -i 's/\/root/\/home\/gsbot/g' ${HOME}/.kube/config

minikube update-context

until kubectl get nodes -o jsonpath="$JSONPATH" 2>&1 | grep -q "Ready=True"; do
  sleep 1
done
