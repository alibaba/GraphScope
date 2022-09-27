#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2022 Alibaba Group Holding Limited. All Rights Reserved.
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
#

import argparse
import numpy as np
from collections import defaultdict

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", default='ir_exp.log')
    args = parser.parse_args()
    return args

def main(args):
    query_time = defaultdict()
    execute_time_flag = "ExecuteTimeMS"
    query_name_flag = "QueryName"

    with open(args.log, "r") as f:
        for line in f.readlines():
            if execute_time_flag in line:
                query_name_suffix = line[line.index(query_name_flag) + len(query_name_flag):].split(",")[0]
                query_name = query_name_suffix[query_name_suffix.index("[") + 1: query_name_suffix.index("]")]
                exec_time_suffix = line[line.index(execute_time_flag) + len(execute_time_flag):]
                exec_time = exec_time_suffix[exec_time_suffix.index("[") + 1: exec_time_suffix.index("]")]
                if query_name not in query_time:
                    query_time[query_name] = []
                query_time[query_name].append(int(exec_time))

    print("|Query|Count|Mean|Std|Max|Min|P50|P90|P95|P99|")
    print("|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|")
    keys = query_time.keys()
    for key in sorted(keys, key=lambda d: int(d.split("_")[-1])):
        t = query_time[key]
        print("|", key, end=" ")
        print("|", len(t), end=" ")
        print("|", np.average(t), end=" ")
        print("|", round(np.std(t), 2), end=" ")
        print("|", np.max(t), end=" ")
        print("|", np.min(t), end=" ")
        print("|", round(np.percentile(t, 50), 2), end=" ")
        print("|", round(np.percentile(t, 90), 2), end=" ")
        print("|", round(np.percentile(t, 95), 2), end=" ")
        print("|", round(np.percentile(t, 99), 2),"|")

if __name__ == '__main__':
    args = parse_args()
    main(args)
