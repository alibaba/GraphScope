#!/bin/bash
# Copyright 2020 Alibaba Group Holding Limited.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
object_id=$1
schema_path=$2
host_ip=$3
engine_count=$4
config=/home/maxgraph/config
_object_id=$(printf '%s\n' "$object_id" | sed -e 's/[\/&]/\\&/g')
_schema_path=$(printf '%s\n' "$schema_path" | sed -e 's/[\/&]/\\&/g')

files=$(ls $config)
output=$config"_"$object_id
mkdir -p $output
for filename in $files
do
    sed -e "s/zookeeper:2181/${host_ip}:2181/g" -e "s/graphname/${_object_id}/g" \
  -e "s/VINEYARD_OBJECT_ID/${_object_id}/g" -e "s/schema_path/${_schema_path}/g" \
  -e "s/worker_num/${engine_count}/g" -e "s/resource_executor_count/${engine_count}/g" \
  -e "s/partition_num/${engine_count}/g" $config/$filename > $output/$filename
done

if [ -z "$5" -o ! -f "$output/executor.application.properties" ]; then
    exit 0
fi
paras=`echo $5 | awk -F";" '{for(i=1; i<=NF; ++i){print $i}}'`
for p in `echo $paras`
do
    key=`echo $p | awk -F":" '{print $1}'`
    value=`echo $p | awk -F":" '{print $2}'`
    key=$(printf '%s\n' "$key" | sed -e 's/[\/&]/\\&/g')
    value=$(printf '%s\n' "$value" | sed -e 's/[\/&]/\\&/g')
    sed -i "s/^\($key\s*=\s*\).*\$/\1$value/" $output/executor.application.properties
done
