#!/bin/bash
set -x
ps -ef | grep "com.alibaba.maxgraph.servers.MaxNode" | grep -v grep | awk '{print $2}' | xargs kill -9
base_dir=$(cd "$(dirname "$0")"; pwd)
cd ${base_dir}/..
cd ./assembly/target/groot
groot_dir=$(pwd)
rm -fr ${groot_dir}/data || true
cd ${base_dir}