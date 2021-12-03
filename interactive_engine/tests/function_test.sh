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
curdir=$(cd "$(dirname "$0")"; pwd)
version=$(cat ${curdir}/../../VERSION)

tmp_result="$curdir/tmp_result"
function _start {
    _port=$1
    workers=$2
    gs_image=$3
    if [ -z "$gs_image" ]; then
        gs_image="registry.cn-hongkong.aliyuncs.com/graphscope/graphscope:${version}"
    fi

    export GS_TEST_DIR=$curdir/src/main/resources
    cd $curdir/../deploy/testing && python3 maxgraph_test_server.py ${_port} &
    sleep 5s
    curl -XPOST http://localhost:${_port} -d 'import graphscope'
    curl -XPOST http://localhost:${_port} -d 'graphscope.set_option(show_log=True)'
    curl -XPOST http://localhost:${_port} -d 'from graphscope.framework.loader import Loader'
    curl_sess="curl -XPOST http://localhost:${_port} -d 'session = graphscope.session(num_workers=${workers}, k8s_volumes={\"data\": {\"type\": \"hostPath\", \"field\": {\"path\": \"${GS_TEST_DIR}\", \"type\": \"Directory\"}, \"mounts\": {\"mountPath\": \"/testingdata\"}}}, k8s_coordinator_cpu=1.0, k8s_coordinator_mem='\''4Gi'\'', k8s_vineyard_cpu=1.0, k8s_vineyard_mem='\''4Gi'\'', vineyard_shared_mem='\''4Gi'\'', k8s_engine_cpu=1.0, k8s_engine_mem='\''4Gi'\'', k8s_etcd_num_pods=3, k8s_etcd_cpu=2, k8s_gs_image='\''${gs_image}'\'')' --write-out %{http_code} --silent --output ./curl.tmp"

    echo $curl_sess
    code=`sh -c "$curl_sess"`
    cat ./curl.tmp && rm -rf ./curl.tmp
    if [ -f "$tmp_result" ]; then
        rm $tmp_result
    fi
    echo "http://localhost:${_port}" > $tmp_result
    if [ "$code" != "200" ]; then
        echo "start service fail"
        _stop
        exit 1
    fi
}

function _stop {
    curl -XPOST http://localhost:${_port} -d 'session.close()'
    _port=`cat $tmp_result | awk -F":" '{print $3}'`
    echo "stop port is ${_port}"
    kill -INT `lsof -i:${_port} -t`
    if [ -f "$tmp_result" ]; then
        rm $tmp_result
    fi
}

function _test {
    url=`cat $tmp_result`
    cd $curdir && mvn test -Dclient.server.url=${url} -Dskip.tests=false
}

opt=$1
if [ "$opt" = "_start" ]; then
    _start $2 $3 $4 $5
elif [ "$opt" = "_stop" ]; then
    _stop
elif [ "$opt" = "_test" ]; then
    _test
else
    _start $1 $2 $3 $4
    _test
    exit_code=$?
    echo "test log ------------------------------------------------------------"
    cat target/surefire-reports/testng-results.xml
    if [ $exit_code -ne 0 ]; then
        echo "run function test fail"
        _stop
        exit 1
    fi
    _stop
fi
