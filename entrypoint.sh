#!/bin/bash

while true; do
  HOST_IP=$(hostname -i)
  if [[ $HOST_IP != "127.0.0.1" ]]; then
    break
  fi
  sleep 0.1
done
export HOST_IP
export HOST_PORT_0=56000

function load_paths {
  if [ -d $1 ]; then
    for d in `ls $1`; do
      PYTHONPATH=$PYTHONPATH:"$1/$d"
    done
    export PYTHONPATH
  fi
}

load_paths "/home/admin/work"
load_paths "/worker/work"

if [[ $MARS_CACHE_MEM_SIZE ]]; then
    echo $MARS_CACHE_MEM_SIZE
    sudo mount -o remount,size=$MARS_CACHE_MEM_SIZE /dev/shm
fi

export LD_LIBRARY_PATH="$(echo -e "${LD_LIBRARY_PATH}" | tr -d '[\"]')"
export META_LOOKUP_NAME="$(echo -e "${META_LOOKUP_NAME}" | tr -d '[\"]')"
export KUBE_API_ADDRESS="$(echo -e "${KUBE_API_ADDRESS}" | tr -d '[\"]')"
export KUBE_NAMESPACE="$(echo -e "${KUBE_NAMESPACE}" | tr -d '[\"]')"
echo api_address ${KUBE_API_ADDRESS}
echo namespace ${KUBE_NAMESPACE}
echo MARS_POD_ROLE ${MARS_POD_ROLE}

EXTRA_ARGS="--log-level debug --log-conf /srv/logging.conf"

# launch vineyard
export PATH=${PATH}:/opt/graphscope/bin
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/opt/graphscope/lib

IP=`hostname -i`
K="kubectl -s $KUBE_API_ADDRESS -n $KUBE_NAMESPACE"
echo "StrictHostKeyChecking no" >> ~/.ssh/config
echo "UserKnownHostsFile /dev/null" >> ~/.ssh/config
echo "" >> ~/.ssh/config

if [ "${MARS_POD_ROLE}" == "gscoordinator" ]
then
  pods=`$K get po | grep marsworker | grep CupidStarted | cut -d ' ' -f 1`
  for pod in $pods
  do
    pod_ip=`$K describe po $pod | grep IP | cut -d ' ' -f 14`
    echo "Host $pod_ip" >> ~/.ssh/config 
    echo "Port 56000" >> ~/.ssh/config
  done
fi

sudo /bin/cp -f /home/admin/.ssh/authorized_keys /root/.ssh/
sudo /bin/cp -f /home/admin/.ssh/config /root/.ssh/

echo sshd: $HOST_IP 56000
sudo /usr/sbin/sshd -p 56000

# echo "kernel.core_pattern=/tmp/core-%E-%h-%p" | sudo tee -a /etc/sysctl.conf
# sudo sysctl -p

# ulimit -c unlimited
if [ "${MARS_POD_ROLE}" == "notebook" ]; then
  count=`$K get po | grep gscoordinator | grep CupidStarted | wc -l`
  echo coordinator $count
  while [ $count -lt 1 ]
  do
    sleep 1
    count=`$K get po | grep gscoordinator | grep CupidStarted | wc -l`
    echo coordinator $count
  done
  c_pod=`$K get po | grep gscoordinator | grep CupidStarted | cut -d ' ' -f 1`
  c_pod_ip=`$K describe po $c_pod | grep IP | cut -d ' ' -f 14`
  echo export graphscope endpoint $c_pod_ip:63800
  export GS_ENDPOINT="$c_pod_ip:63800"

  pods=`$K get po | grep marsworker | grep CupidStarted | cut -d ' ' -f 1`
  for pod in $pods
  do
    pod_ip=`$K describe po $pod | grep IP | cut -d ' ' -f 14`
    echo "Host $pod_ip" >> ~/.ssh/config 
    echo "Port 56000" >> ~/.ssh/config
  done

fi
  

if [[ "$1" == *"/"* ]] || [[ "$1" == *"sh" ]]; then
  $@
else
  if [ "${MARS_POD_ROLE}" == "scheduler" ]; then
    echo "launch etcd"
    /usr/local/bin/etcd --initial-advertise-peer-urls http://${IP}:2380 --listen-peer-urls http://0.0.0.0:2380 --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://${IP}:2379 --initial-cluster $IP=http://$IP:2380 --initial-cluster-state new& 
    sleep 3
    echo 'launch vineyard'
    export VINEYARD_IPC_SOCKET='/tmp/vineyard.sock'
    export WITH_VINEYARD=ON
    echo MARS_CACHE_MEM_SIZE ${MARS_CACHE_MEM_SIZE}
    vineyardd --socket /tmp/vineyard.sock --etcd_endpoint http://127.0.0.1:2379 &
  fi
    
  if [ "${MARS_POD_ROLE}" == "worker" ]; then
    count=`$K get po | grep marsscheduler | grep CupidStarted | wc -l`
    echo scheduler $count
    while [ $count -lt 1 ]
    do
      sleep 1
      count=`$K get po | grep marsscheduler | grep CupidStarted | wc -l`
      echo scheduler $count
    done
    s_pod=`$K get po | grep marsscheduler | grep CupidStarted | cut -d ' ' -f 1`
    s_pod_ip=`$K describe po $s_pod | grep IP | cut -d ' ' -f 14`
    echo "scheduler ip $s_pod_ip"
    echo 'launch vineyard'
    export VINEYARD_IPC_SOCKET='/tmp/vineyard.sock'
    export WITH_VINEYARD=ON
    echo MARS_CACHE_MEM_SIZE ${MARS_CACHE_MEM_SIZE}
    vineyardd --socket /tmp/vineyard.sock --etcd_endpoint http://$s_pod_ip:2379 --size ${MARS_CACHE_MEM_SIZE}&
  fi
  /opt/conda/bin/python -m "$1" ${@:2} $EXTRA_ARGS
fi
