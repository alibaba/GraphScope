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
from graphviz import Digraph


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default='executor.log')
    parser.add_argument("--job_id", default='1')
    parser.add_argument("--worker_num", default='2')
    args = parser.parse_args()
    return args


def draw_time_cost_graph(graph_name, worker_operator_time_cost, job_edge_desc, total_timecost):
    # draw the time cost graph (of each worker)
    worker_cost_dot = Digraph(graph_name)
    # draw nodes of time cost graph
    for op in worker_operator_time_cost:
        op_index = op.split("_")[-1].split("]")[0]
        percentage = float(worker_operator_time_cost[op][0:len(worker_operator_time_cost[op])-2])/float(total_timecost)
        worker_cost_dot.node(op_index, label=f'{op}: {worker_operator_time_cost[op]}, {format(percentage,".2%")}')
    # draw edges of time cost graph
    for edge in job_edge_desc:
        edge_src_index = edge.split(".")[0].split("(")[-1]
        edge_dst_index = edge.split(".")[1].split("(")[-1]
        worker_cost_dot.edge(edge_src_index,edge_dst_index)
    worker_cost_dot.render(view=False)


def draw_communication_cost_graph(graph_name, job_operators, job_edges):
    # draw the communication cost graph (of each job)
    job_comm_dot = Digraph(graph_name)
    # draw nodes of communication cost graph
    for op in job_operators:
        op_index = op.split("_")[-1].split("]")[0]
        job_comm_dot.node(op_index, label=f'{op}: {job_operators[op]}')
    # draw edges of communication cost graph
    job_edge_list = defaultdict(list)
    for edge in job_edges:
        edge_src_index = edge.split(".")[0].split("(")[-1]
        edge_dst_index = edge.split(".")[1].split("(")[-1]
        job_edge_list[edge_src_index,edge_dst_index].append((edge,job_edges[edge]))
    for edge in job_edge_list:
        job_comm_dot.edge(edge[0], edge[1], label=f'{job_edge_list[edge]}')
    job_comm_dot.render(view=False)


def main(args):
    job_ids = [i for i in args.job_id.split(",")]
    for job_id in job_ids:
        print("job: ", job_id)
        worker_num = int(args.worker_num)
        job_operators = defaultdict(int)
        job_edges = defaultdict(int)
        job_edge_desc = defaultdict(int)

        for worker_id in range(worker_num):
            print("worker:", worker_id)
            worker_operators = defaultdict(int)
            worker_edges = defaultdict(int)
            worker_operator_time_cost = defaultdict(str)
            operator_len = 0
            total_timecost = 0

            common_prefix = f"[worker_{worker_id}({job_id})]: "
            worker_fire_flag = common_prefix + "fire operator "
            worker_handle_flag = common_prefix + "handle batch"
            worker_local_push_flag = common_prefix + "push batches local"
            worker_remote_push_flag = common_prefix + "push batches remote"
            worker_after_fire_flag = common_prefix + "after fire operator "
            worker_operator_finished_flag = common_prefix + "operator finished"
            worker_edge_desc_flag = common_prefix + "job edges: "

            with open(args.input, 'r') as f:
                for line in f.readlines():
                    if worker_edge_desc_flag in line:
                        output_suffix = line[line.index(worker_edge_desc_flag) + len(worker_edge_desc_flag):]
                        edge_str = output_suffix.replace(" -> ", '_').replace(" => ", '_')
                        edge = edge_str[edge_str.find('('): edge_str.rfind(')')+1]
                        job_edge_desc[edge] = 0

                    # count local/remote push batches between operators
                    if worker_local_push_flag in line:
                        output_suffix = line[line.index(worker_local_push_flag) + len(worker_local_push_flag):]
                        local_output_operator = output_suffix[output_suffix.index('[') + 1: output_suffix.index("]")]
                        local_output_operator_len = int(output_suffix.split("=")[-1])
                        worker_edges["local_"+local_output_operator] += local_output_operator_len
                    if worker_remote_push_flag in line:
                        output_suffix = line[line.index(worker_remote_push_flag) + len(worker_remote_push_flag):]
                        remote_output_operator = output_suffix[output_suffix.index('[') + 1: output_suffix.index("]")]
                        remote_output_operator_len = int(output_suffix.split("=")[-1])
                        worker_edges["remote_"+remote_output_operator] += remote_output_operator_len

                    # count process batches for each operator
                    if worker_fire_flag in line:
                        operator = (line[line.index(worker_fire_flag) + len(worker_fire_flag):]).replace("\n", '')
                        if operator not in worker_operators:
                            worker_operators[operator] = 0
                    if worker_handle_flag in line and "len" in line:
                        len_str = line[line.index(worker_handle_flag) + len(worker_handle_flag):]
                        operator_len += int(len_str.split("=")[-1])
                    if worker_after_fire_flag in line:
                        operator = (line[line.index(worker_after_fire_flag) + len(worker_after_fire_flag):]).replace("\n", '')
                        worker_operators[operator] += operator_len
                        operator_len = 0

                    # count process time for each operator
                    if worker_operator_finished_flag in line:
                        output_suffix = (line[line.index(worker_operator_finished_flag) + len(worker_operator_finished_flag):]).replace(" ","").replace("\n","")
                        operator = output_suffix.split(",")[0]
                        time_cost = output_suffix.split("=")[-1]
                        worker_operator_time_cost[operator] = time_cost

                    job_finished = re.search(f'\[worker_{worker_id}\({job_id}\)\]: job\({job_id}\) .* finished, used (.*) ms;', line)
                    if job_finished:
                        total_timecost = job_finished.group(1)
                        print(job_finished.group())


            print("=========== operator intermediate batches ===========\n", dict(worker_operators))
            print("=============== operator push batches ===============\n", dict(worker_edges))
            print("================= operator time cost ================\n", dict(worker_operator_time_cost))
            draw_time_cost_graph(f'job_{job_id}_{worker_id}_cost', worker_operator_time_cost, job_edge_desc, total_timecost)

            # accum process batches of each operator for job
            for op in worker_operators:
                if op not in job_operators:
                    job_operators[op] = 0
                job_operators[op] += worker_operators[op]

            # accum push batches between operators for job; push batch including local push and remote push;
            for edge in worker_edges:
                # e.g., local_(0.0)_(1.0), remote_(1.0)_(2.0)
                if edge not in job_edges:
                    job_edges[edge] = 0
                job_edges[edge] += worker_edges[edge]

        # draw the communication cost table (of each job)
        draw_communication_cost_graph(f'job_{job_id}_comm',job_operators,job_edges)

if __name__ == '__main__':
    args = parse_args()
    main(args)

