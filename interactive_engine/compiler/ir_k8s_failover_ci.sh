#!/bin/bash

# usage: ./ir_k8s_failover_ci.sh default test-graphscope-store 2 1

function rand(){
    min=$1
    max=$(($2 - $min + 1))
    num=$(($RANDOM+1000000000))
    echo $(($num%$max + $min))
}

function get_role_pods_to_kill() {
    role=$1
    role_total=$2
    kill_total=$((role_total/2+1))
    entries=($(shuf -i 0-$((role_total-1)) -n ${kill_total}))
    for i in "${entries[@]}"
    do
        kill_pods=${kill_pods}' '${role_prefix}'-'${role}'-'$i
    done
    echo ${kill_pods}
}

function wait_role_pods_to_run() {
    role=$1
    role_total=$2
    while [ 1 ]
    do
        cnt=$(kubectl --namespace=${namespace} get pod -l "app.kubernetes.io/component=${role}" --field-selector=status.phase==Running 2> /dev/null | wc -l)
        if [[ $? == 0 && $cnt > ${role_total} ]];then
            break
        fi
	echo "keep waiting for all pods are running ..."
    done
}

namespace=$1
role_prefix=$2
store_total=$3
frontend_total=$4

wait_role_pods_to_run store ${store_total}
wait_role_pods_to_run frontend ${frontend_total}

kubectl --namespace=${namespace} delete pod $(get_role_pods_to_kill store ${store_total})
wait_role_pods_to_run store ${store_total}

node_port=$(kubectl --namespace=${namespace} get svc ${role_prefix}-frontend -o go-template='{{range.spec.ports}}{{if .nodePort}}{{.nodePort}}{{"\n"}}{{end}}{{end}}')
hostname=$(hostname -i)
python3 ./submit_query.py $hostname:${node_port}
