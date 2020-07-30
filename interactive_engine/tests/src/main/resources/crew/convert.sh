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
file=$1
cat $file".dat" | awk -F',' '{for(i=1;i<=NF;++i){if(i==3) printf $1 ","; else if(i < 3) printf $(i+1)","; else if(i==NF) print $i; else printf $i","}}' > $file"_vineyard.dat"
#cat $file".dat" | awk -F',' '{for(i=1;i<=NF;++i){if(i==3) print $1 ","}}' > $file"_vineyard.dat"
