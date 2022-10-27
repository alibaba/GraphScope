#! /usr/bin/env python3
# -*- coding: utf-8 -*-
#
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
#

import copy
import os
import queue
from enum import Enum
from typing import Sequence

from graphscope.proto import message_pb2
from graphscope.proto import op_def_pb2
from graphscope.proto import types_pb2

# defaults to 256MB
CHUNK_SIZE = (
    int(os.environ["GS_GRPC_CHUNK_SIZE"])
    if "GS_GRPC_CHUNK_SIZE" in os.environ
    else 256 * 1024 * 1024 - 1
)


class GSEngine(Enum):
    analytical_engine = 0
    interactive_engine = 1
    learning_engine = 2
    coordinator = 11


class DAGManager(object):
    _analytical_engine_split_op = [
        types_pb2.CREATE_GRAPH,  # spawn an io stream to read/write data from/to vineyard
        types_pb2.BIND_APP,  # need loaded graph to compile
        types_pb2.ADD_LABELS,  # need loaded graph
        types_pb2.RUN_APP,  # need loaded app
        types_pb2.CONTEXT_TO_NUMPY,  # need loaded graph to transform selector
        types_pb2.CONTEXT_TO_DATAFRAME,  # need loaded graph to transform selector
        types_pb2.GRAPH_TO_NUMPY,  # need loaded graph to transform selector
        types_pb2.GRAPH_TO_DATAFRAME,  # need loaded graph to transform selector
        types_pb2.TO_VINEYARD_TENSOR,  # need loaded graph to transform selector
        types_pb2.TO_VINEYARD_DATAFRAME,  # need loaded graph to transform selector
        types_pb2.OUTPUT,  # need loaded graph to transform selector
        types_pb2.PROJECT_GRAPH,  # need loaded graph to transform selector
        types_pb2.PROJECT_TO_SIMPLE,  # need loaded graph schema information
        types_pb2.ADD_COLUMN,  # need ctx result
        types_pb2.UNLOAD_GRAPH,  # need loaded graph information
        types_pb2.UNLOAD_APP,  # need loaded app information
    ]

    _interactive_engine_split_op = [
        types_pb2.SUBGRAPH,
        types_pb2.GREMLIN_QUERY,
        types_pb2.FETCH_GREMLIN_RESULT,
    ]

    _learning_engine_split_op = []

    _coordinator_split_op = [
        types_pb2.DATA_SOURCE,  # spawn an io stream to read/write data from/to vineyard
        types_pb2.DATA_SINK,  # spawn an io stream to read/write data from/to vineyard
    ]

    def __init__(self, request_iterator: Sequence[message_pb2.RunStepRequest]):
        self._dag_queue = queue.Queue()
        req_head = None
        # a list of chunks
        req_bodies = []
        for req in request_iterator:
            if req.HasField("head"):
                req_head = req
            else:
                req_bodies.append(req)
        if req_head is not None:
            # split dag
            dag = op_def_pb2.DagDef()
            dag_for = GSEngine.analytical_engine
            dag_bodies = []
            for op in req_head.head.dag_def.op:
                if self.is_splited_op(op):
                    if dag.op:
                        self._dag_queue.put((dag_for, dag, dag_bodies))
                    # init empty dag
                    dag = op_def_pb2.DagDef()
                    dag_for = self.get_op_exec_engine(op)
                    dag_bodies = []
                # select op
                dag.op.extend([copy.deepcopy(op)])
                for req_body in req_bodies:
                    # select chunks belong to this op
                    if req_body.body.op_key == op.key:
                        dag_bodies.append(req_body)
            if dag.op:
                self._dag_queue.put((dag_for, dag, dag_bodies))

    def is_splited_op(self, op):
        return op.op in (
            self._analytical_engine_split_op
            + self._interactive_engine_split_op
            + self._learning_engine_split_op
            + self._coordinator_split_op
        )

    def get_op_exec_engine(self, op):
        op_type = op.op
        if op_type in self._analytical_engine_split_op:
            return GSEngine.analytical_engine
        if op_type in self._interactive_engine_split_op:
            return GSEngine.interactive_engine
        if op_type in self._learning_engine_split_op:
            return GSEngine.learning_engine
        if op_type in self._coordinator_split_op:
            return GSEngine.coordinator
        raise RuntimeError("Op {0} get execution engine failed.".format(op_type))

    def empty(self):
        return self._dag_queue.empty()

    def next_dag(self):
        if not self._dag_queue.empty():
            return self._dag_queue.get()
        raise RuntimeError("Get element from empty queue.")


def split_op_result(op_result: op_def_pb2.OpResult):
    """Split op result into a list of chunk.

    Note that this function may modify `result` attribute of op_result.
    """
    if op_result.meta.has_large_result:
        result = op_result.result
        splited_result = [
            result[i : i + CHUNK_SIZE] for i in range(0, len(result), CHUNK_SIZE)
        ]
        # clear result
        op_result.result = b""
        return splited_result
    return []
