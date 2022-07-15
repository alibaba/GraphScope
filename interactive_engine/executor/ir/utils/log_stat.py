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

"""
Profile Tool for Querying on GAIA

This tool provide a way of collecting some information from the log of GAIA, including
the batch number of intermediate results,
the communication cost (batches that need to be shuffled intra/inter-processes),
and the time cost,
of each operator in the query, help to profile the query phases in GAIA.

Before using this tool, please start GAIA with `PROFILE_FLAG = true`,
with which we could get necessary logs for profiling.
"""

import argparse
import re
from collections import defaultdict


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default='executor.log')
    parser.add_argument("--job_id", default='1')
    parser.add_argument("--worker_num", default='2')
    args = parser.parse_args()
    return args


def main(args):
    job_ids = [i for i in args.job_id.split(",")]
    for job_id in job_ids:
        print("job: ", job_id)
        worker_num = int(args.worker_num)
        for i in range(worker_num):
            print("worker:", i)
            worker_operators = defaultdict(int)
            worker_operator_time_cost = defaultdict(str)
            operator_len = 0

            common_prefix = f"[worker_{i}({job_id})]: "
            worker_fire_flag = common_prefix + "fire operator "
            worker_handle_flag = common_prefix + "handle batch"
            worker_local_push_flag = common_prefix + "EventEmitPush Local"
            worker_remote_push_flag = common_prefix + "EventEmitPush Remote"
            worker_after_fire_flag = common_prefix + "after fire operator "
            worker_operator_finished_flag = common_prefix + "operator finished"

            with open(args.input, 'r') as f:
                for line in f.readlines():
                    if worker_local_push_flag in line:
                        output_suffix = line[line.index(worker_local_push_flag) + len(worker_local_push_flag):]
                        local_output_operator = output_suffix[output_suffix.index('[(') + 2: output_suffix.index(")]")]
                        local_output_operator_len = int(output_suffix.split("=")[-1])
                        worker_operators["local_output_"+local_output_operator] += local_output_operator_len
                    if worker_remote_push_flag in line:
                        output_suffix = line[line.index(worker_remote_push_flag) + len(worker_remote_push_flag):]
                        remote_output_operator = output_suffix[output_suffix.index('[(') + 2: output_suffix.index(")]")]
                        remote_output_operator_len = int(output_suffix.split("=")[-1])
                        worker_operators["remote_output_"+remote_output_operator] += remote_output_operator_len

                    if worker_fire_flag in line:
                        operator = (line[line.index(worker_fire_flag) + len(worker_fire_flag):]).replace("\n", '')
                        if operator not in worker_operators:
                            worker_operators[operator] = 0
                    if worker_handle_flag in line and "len" in line:
                        len_str = line[line.index(worker_handle_flag) + len(worker_handle_flag):]
                        operator_len = int(len_str.split("=")[-1])
                    if worker_after_fire_flag in line:
                        operator = (line[line.index(worker_after_fire_flag) + len(worker_after_fire_flag):]).replace("\n", '')
                        worker_operators[operator] += operator_len
                        operator_len = 0

                    if worker_operator_finished_flag in line:
                        output_suffix = (line[line.index(worker_operator_finished_flag) + len(worker_operator_finished_flag):]).replace(" ","").replace("\n","")
                        operator = output_suffix.split(",")[0]
                        time_cost = output_suffix.split("=")[-1]
                        worker_operator_time_cost[operator] = time_cost

                    job_finished = re.search(f'\[worker_{i}\({job_id}\)\]: job\({job_id}\) .* finished, .*', line)
                    if job_finished:
                        print(job_finished.group())


            print("operator: ", dict(worker_operators))
            print("operator time cost: ", dict(worker_operator_time_cost))


if __name__ == '__main__':
    args = parse_args()
    main(args)
