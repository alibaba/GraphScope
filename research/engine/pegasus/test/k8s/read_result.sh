#!/bin/bash

sufix=$1
config_map_name="pegasus-result-${sufix}"

query_time=10

while true
do
  sleep ${query_time}
  config_map=$(kubectl get configmap ${config_map_name} --namespace pegasus-ci -o yaml)
  if [ "x${config_map}" != "x" ];then
    echo ${config_map}
    final_status=$(echo ${config_map} | grep Result | awk {'print $5'})
    echo "k8s test status: ${final_status}"
    if [[ ${final_status} == "Success" ]]; then
      exit 0
    else
      exit 1
    fi
  fi
done
