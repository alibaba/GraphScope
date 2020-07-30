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
source /home/admin/vineyard/ali-os-env.sh
/home/admin/vineyard/bin/vineyard_htap_loader /tmp/vineyard.sock 2 "modern_graph/knows.csv#header_row=true#delimiter=|#label=knows#src_label=person#dst_label=person" "modern_graph/created.csv#header_row=true#delimiter=|#label=created#src_label=person#dst_label=software"   2 "modern_graph/person.csv#label=person#header_row=true&delimiter=|" "modern_graph/software.csv#label=software#header_row=true&delimiter=|" 2>&1 \
| grep "fragment group id" | awk  -vp1=/tmp/ -vp2=.json -F ' ' '{print $8,p1$8p2}'
