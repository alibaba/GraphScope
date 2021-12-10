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

curl -XPOST $url -d 'modern_graph = session.load_from(
    vertices={
    "person": (Loader("/testingdata/modern_graph/person.csv", delimiter="|", header_row=True), ["name", ("age", "int")], "id"),
    "software": (Loader("/testingdata/modern_graph/software.csv", delimiter="|", header_row=True), ["name", "lang"], "id"),
    },
    edges={
    "knows": [Loader("/testingdata/modern_graph/knows.csv", delimiter="|"), None, (0, "person"), (1, "person")],
    "created": [Loader("/testingdata/modern_graph/created.csv", delimiter="|"), None, (0, "person"), (1, "software")],
    },
    generate_eid=False)
' >/dev/null

curl -XPOST $url -d 'modern_gremlin = session.gremlin(modern_graph)'>/dev/null
code=$(curl -XPOST $url -d 'modern_gremlin._graph_url' --write-out %{http_code} --silent --output ./gremlin.tmp)
res=$(cat ./gremlin.tmp)
if [ -f "gremlin.tmp" ];then
    rm gremlin.tmp 1>/dev/null 2>&1
fi
echo $code"/"$res
