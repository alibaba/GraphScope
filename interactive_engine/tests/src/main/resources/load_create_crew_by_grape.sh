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

curl -XPOST $url -d 'crew_graph = session.load_from(
    edges={
        "develops": ( Loader( "/testingdata/crew/develops_vineyard.dat", header_row=True, delimiter=","),
            ["id","since"],
            ("srcId", "person"),
            ("dstId", "software"),
        ),
        "traverses": (
            Loader( "/testingdata/crew/traverses_vineyard.dat", header_row=True, delimiter=","),
            ["id"],
            ("srcId", "software"),
            ("dstId", "software"),
        ),
        "uses": (
            Loader( "/testingdata/crew/uses_vineyard.dat", header_row=True, delimiter=","),
            ["id", ("skill","int")],
            ("srcId", "person"),
            ("dstId", "software"),
        ),
    },
    vertices={
        "person": (
            Loader( "/testingdata/crew/person.dat", header_row=True, delimiter=","),
            ["name", "location"],
            "id",
        ),
        "software": (
            Loader( "/testingdata/crew/software.dat", header_row=True, delimiter=","),
            ["name"],
            "id",
        ),
    },
    generate_eid=False,
)' >/dev/null

curl -XPOST $url -d 'crew_gremlin = session.gremlin(crew_graph)'>/dev/null
code=$(curl -XPOST $url -d 'crew_gremlin._graph_url' --write-out %{http_code} --silent --output ./gremlin.tmp)
res=`cat ./gremlin.tmp`
if [ -f "gremlin.tmp" ];then
    rm gremlin.tmp 1>/dev/null 2>&1
fi
echo $code"/"$res
