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

curl -XPOST $url -d 'grateful_graph = session.load_from(
    edges={
        "sungBy": ( Loader( "/testingdata/grateful/sungBy_vineyard.dat", header_row=True, delimiter=","),
            ["id"],
            ("srcId", "song"),
            ("dstId", "artist"),
        ),
        "writtenBy": (
            Loader( "/testingdata/grateful/writtenBy_vineyard.dat", header_row=True, delimiter=","),
            ["id"],
            ("srcId", "song"),
            ("dstId", "artist"),
        ),
        "followedBy": (
            Loader( "/testingdata/grateful/followedBy_vineyard.dat", header_row=True, delimiter=","),
            ["id"],
            ("srcId", "song"),
            ("dstId", "song"),
        ),
    },
    vertices={
        "song": (
            Loader( "/testingdata/grateful/song.dat", header_row=True, delimiter=","),
            ["name", "songType", ("performances", "int")],
            "id",
        ),
        "artist": (
            Loader( "/testingdata/grateful/artist.dat", header_row=True, delimiter=","),
            ["name"],
            "id",
        ),
    },
    generate_eid=False,
)' >/dev/null

curl -XPOST $url -d 'grateful_gremlin = session.gremlin(grateful_graph)' >/dev/null
code=$(curl -XPOST $url -d 'grateful_gremlin._graph_url' --write-out %{http_code} --silent --output ./grateful.tmp)
res=$(cat ./grateful.tmp)
if [ -f "grateful.tmp" ];then
    rm grateful.tmp 1>/dev/null 2>&1
fi
echo $code"/"$res
