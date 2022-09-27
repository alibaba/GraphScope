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
 * the batch number of intermediate results between operators,
 * the communication cost (batches that need to be shuffled intra/inter-processes) between operators,
 * the time cost of each operator, and the ratio of operator_time_cost, to total_computation_cost
 (specifically, total_computation_cost doesn't include communication time.)
 * the total_computation_cost, and the ratio of total_computation_cost to total_time_cost
 (Specifically, total_time_cost includes communication time)
help to profile the query phases in GAIA.

Before using this tool, please start GAIA with
`PROFILE_TIME_FLAG=true PROFILE_COMM_FLAG=true`,
with which we could get necessary logs for profiling time cost and communication cost respectively.
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
    parser.add_argument("--show_details", default=False)
    args = parser.parse_args()
    return args

def accum_job_info(worker_matrix, job_matrix):
    for item in worker_matrix:
        if item not in job_matrix:
            job_matrix[item] = 0
        job_matrix[item] += worker_matrix[item]

def draw_time_cost_graph(graph_name, worker_operator_time_cost, job_edge_desc, total_timecost):
    # draw the time cost graph (of each worker)
    worker_cost_dot = Digraph(graph_name)
    # draw nodes of time cost graph
    for op in worker_operator_time_cost:
        op_index = extract_operator_idx(op)
        percentage = float(worker_operator_time_cost[op])/float(total_timecost)
        worker_cost_dot.node(op_index, label=f'{op}: {worker_operator_time_cost[op]} ms, {format(percentage,".2%")}')
    # draw edges of time cost graph
    for edge in job_edge_desc:
        edge_src_index,edge_dst_index =extract_edge_idx(edge)
        worker_cost_dot.edge(edge_src_index,edge_dst_index)
    worker_cost_dot.render(view=False)


def draw_communication_cost_graph(graph_name, job_operators, job_edges):
    # draw the communication cost graph (of each job)
    job_comm_dot = Digraph(graph_name)
    # draw nodes of communication cost graph
    for op in job_operators:
        op_index = extract_operator_idx(op)
        job_comm_dot.node(op_index, label=f'{op}: {job_operators[op]}')
    # draw edges of communication cost graph
    job_edge_list = defaultdict(list)
    for edge in job_edges:
        edge_src_index,edge_dst_index =extract_edge_idx(edge)
        job_edge_list[edge_src_index,edge_dst_index].append((edge,job_edges[edge]))
    for edge in job_edge_list:
        job_comm_dot.edge(edge[0], edge[1], label=f'{job_edge_list[edge]}')
    job_comm_dot.render(view=False)

def extract_operator(line):
    # \t\t[operator_name_idx]\t\t
    op = line[line.index('\t\t[') + 2: line.index("]\t\t") +1]
    return op

def extract_operator_idx(op):
    # [operator_name_idx]
    op_index = op.split("_")[-1].split("]")[0]
    return op_index

def extract_edge_idx(edge):
    # [local_(0.0)_(1.0)] or [remote(0.0)_(1.0)]
    edge_src_index = edge.split(".")[0].split("(")[-1]
    edge_dst_index = edge.split(".")[1].split("(")[-1]
    return edge_src_index,edge_dst_index

def extract_val(suffix):
    # len = xx or time_cost = xx
    len = suffix.split("=")[-1]
    return len

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
            computation_cost = 0
            total_timecost = 0

            common_prefix = f"[worker_{worker_id}({job_id})]: "
            worker_edge_desc_flag = common_prefix + "job edges: "
            worker_vertex_desc_flag = common_prefix + "job vertices: "
            worker_push_flag = common_prefix + "push batches: "
            worker_handle_flag = common_prefix + "handle batch "
            worker_after_fire_flag = common_prefix + "after fire operator "
            worker_operator_finished_flag = common_prefix + "operator finished "

            with open(args.input, 'r') as f:
                for line in f.readlines():
                    # initialize
                    if worker_vertex_desc_flag in line:
                        operator = extract_operator(line)
                        worker_operators[operator] = 0
                        computation_cost = 0
                    if worker_edge_desc_flag in line:
                        operator = extract_operator(line)
                        job_edge_desc[operator] = 0

                    # count local/remote push batches between operators
                    if worker_push_flag in line:
                        push_edge = extract_operator(line)
                        worker_edges[push_edge] += int(extract_val(line))

                    # count process batches for each operator
                    if worker_handle_flag in line:
                        operator_len += int(extract_val(line))
                    if worker_after_fire_flag in line:
                        operator = extract_operator(line)
                        worker_operators[operator] += operator_len
                        operator_len = 0

                    # count process time for each operator
                    if worker_operator_finished_flag in line:
                        operator = extract_operator(line)
                        time_cost = float(extract_val(line))
                        worker_operator_time_cost[operator] = time_cost
                        computation_cost += time_cost

                    job_finished = re.search(f'\[worker_{worker_id}\({job_id}\)\]: job\({job_id}\) .* finished, used (.*) ms;', line)
                    if job_finished:
                        total_timecost = job_finished.group(1)
                        print(job_finished.group())
                        percentage = computation_cost/float(total_timecost)
                        print("computation cost is {:.2f} ms, and the ratio of computation cost to total cost is {:.2f}".format(computation_cost, percentage))

            if args.show_details:
                print("=========== operator intermediate batches ===========\n", dict(worker_operators))
                print("=============== operator push batches ===============\n", dict(worker_edges))
                print("================= operator time cost ================\n", dict(worker_operator_time_cost))
            draw_time_cost_graph(f'job_{job_id}_{worker_id}_cost', worker_operator_time_cost, job_edge_desc, computation_cost)

            # accum process batches of each operator for job
            accum_job_info(worker_operators, job_operators)
            # accum push batches between operators for job; push batch including local push and remote push;
            accum_job_info(worker_edges, job_edges)

        # draw the communication cost table (of each job)
        draw_communication_cost_graph(f'job_{job_id}_comm',job_operators,job_edges)

if __name__ == '__main__':
    args = parse_args()
    main(args)

