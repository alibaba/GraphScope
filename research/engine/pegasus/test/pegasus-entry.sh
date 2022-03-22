#!/bin/bash

# Generate server.toml for pegasus test
echo "[network]" >> server.toml
echo "server_id = ${HOSTNAME##*-}" >> server.toml
echo "ip = $(hostname -I)" >> server.toml
echo "port = 50000" >> server.toml
echo "servers_size = ${REPLICAS}" >> server.toml

for i in $(seq 0 $(( $REPLICAS - 1 ))); do
  while true
  do
    ip=$(host ${POD_PREFIX}-${i}.${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local | awk '{print $4}')
    if [[ ${ip} != "found:" ]]; then
      break
    fi
  done
  echo "[[network.servers]]" >> server.toml
  echo "ip = '${ip}'" >> server.toml
  echo "port = 50000" >> server.toml
done

cmd="bash /home/graphscope/pegasus/test/pegasus-test.sh"
eval "${cmd} 2>&1 | tee output.log"
status=$?

echo "apiVersion: v1" > /home/graphscope/status.yaml
echo "kind: ConfigMap" >> /home/graphscope/status.yaml
echo "metadata:" >> /home/graphscope/status.yaml
echo "  name: pegasus-result-${CONFIG_MAP_NAME}" >> /home/graphscope/status.yaml
echo "data:" >> /home/graphscope/status.yaml

if [[ ${HOSTNAME##*-} == "0" ]]
then
  if [[ ${status} == "0" ]]
  then
    echo "  Result: \"Success\"" >> /home/graphscope/status.yaml
  else
    echo "  Result: \"Failed\"" >> /home/graphscope/status.yaml
  fi

  kubectl -n pegasus-ci create -f /home/graphscope/status.yaml
fi

tail -f /dev/null
