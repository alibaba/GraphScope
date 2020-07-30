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
opt=$1
str=$2
output=$3
if [ "$opt" = "oss" ]; then
    if [ ! -z "$str" ]; then
        osscmd get $str $output
    fi
else
    split=$4
    echo $str | awk -F"$split" '{for(i=1;i<=NF;++i){print $i}}' > $output
    sed -i 's/^[ \t]*//g' $output
    sed -i 's/[ \t]*$//g' $output
fi
