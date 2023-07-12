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
url=$1

cur_dir = $(cd "$(dirname "$0")"; pwd)

curl -XPOST $url -d 'sink_graph = session.load_from(
    edges={
        "link": ( Loader( "/testingdata/sink/link_vineyard.dat", header_row=True, delimiter=","),
            ["id"],
            ("srcId", "message"),
            ("dstId", "message"),
        ),
        "self": (
            Loader( "/testingdata/sink/self_vineyard.dat", header_row=True, delimiter=","),
            ["id"],
            ("srcId", "loops"),
            ("dstId", "loops"),
        ),
    },
    vertices={
        "message": (
            Loader( "/testingdata/sink/message.dat", header_row=True, delimiter=","),
            ["name"],
            "id",
        ),
        "loops": (
            Loader( "/testingdata/sink/loops.dat", header_row=True, delimiter=","),
            ["name"],
            "id",
        ),
    },
    generate_eid=False,
)' >/dev/null

curl -XPOST $url -d 'sink_gremlin = session.gremlin(sink_graph)'>/dev/null
code=$(curl -XPOST $url -d 'sink_gremlin.gremlin_url' --write-out %{http_code} --silent --output ./gremlin.tmp)
res=`cat ./gremlin.tmp`
if [ -f "gremlin.tmp" ];then
    rm gremlin.tmp 1>/dev/null 2>&1
fi
echo $code"/"$res
