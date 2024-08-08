#!/bin/sh
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

base_dir=$(cd $(dirname $0); pwd)

# build the benchmark
printf "Building the benchmark...\n"
cd ${base_dir}/..
make build

# run the benchmark
printf "Running the benchmark...\n"
cd ${base_dir}/..
if make run > interactive-benchmark.log 2>&1 & then
    # record the pid of the benchmark process
    benchmark_pid=$!

    # wait for the benchmark process to finish
    wait $benchmark_pid

    # check the exit code of the benchmark process
    if [ $? -eq 0 ]; then
        # collect the benchmark result
        printf "Collecting the benchmark result...\n"
        cd ${base_dir}/.. && make collect 
    else
        printf "Benchmark run failed. Check interactive-benchmark.log for details.\n"
        exit 1
    fi
else
    printf "Failed to start the benchmark run.\n"
    exit 1
fi