#!/usr/bin/env bash

# For mpi-operator launcher
# Adapt from https://github.com/kubeflow/mpi-operator/blob/master/build/base/entrypoint.sh

function resolve_host() {
  host="$1"
  check="nslookup $host"
  max_retry=10
  counter=0
  backoff=1
  until $check > /dev/null
  do
    if [ $counter -eq $max_retry ]; then
      echo "Couldn't resolve $host"
      return
    fi
    sleep $backoff
    echo "Couldn't resolve $host... Retrying"
    ((counter++))
    backoff=$(echo - | awk "{print $backoff + $backoff}")
  done
  echo "Resolved $host"
}

if [ "$K_MPI_JOB_ROLE" == "launcher" ]; then
  # resolve_host "$HOSTNAME"
  cut -d ' ' -f 1 /etc/mpi/hostfile | while read -r host
  do
    resolve_host "$host"
  done
fi

exec "$@"