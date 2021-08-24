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
import queue
from enum import Enum

from graphscope.proto import op_def_pb2
from graphscope.proto import types_pb2


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
        types_pb2.PROJECT_GRAPH,  # need loaded graph to transform selector
        types_pb2.PROJECT_TO_SIMPLE,  # need loaded graph schema information
        types_pb2.ADD_COLUMN,  # need ctx result
        types_pb2.UNLOAD_GRAPH,  # need loaded graph information
        types_pb2.UNLOAD_APP,  # need loaded app information
    ]

    _interactive_engine_split_op = [
        types_pb2.CREATE_INTERACTIVE_QUERY,
        types_pb2.SUBGRAPH,
        types_pb2.GREMLIN_QUERY,
        types_pb2.FETCH_GREMLIN_RESULT,
        types_pb2.CLOSE_INTERACTIVE_QUERY,
    ]

    _learning_engine_split_op = [
        types_pb2.CREATE_LEARNING_INSTANCE,
        types_pb2.CLOSE_LEARNING_INSTANCE,
    ]

    _coordinator_split_op = [
        types_pb2.DATA_SOURCE,  # spawn an io stream to read/write data from/to vineyard
        types_pb2.OUTPUT,  # spawn an io stream to read/write data from/to vineyard
    ]

    def __init__(self, dag_def: op_def_pb2.DagDef):
        self._dag_def = dag_def
        self._split_dag_def_queue = queue.Queue()

        # split dag
        split_dag_def = op_def_pb2.DagDef()
        split_dag_def_for = GSEngine.analytical_engine
        for op in self._dag_def.op:
            if op.op in self._analytical_engine_split_op:
                if split_dag_def.op:
                    self._split_dag_def_queue.put((split_dag_def_for, split_dag_def))
                split_dag_def = op_def_pb2.DagDef()
                split_dag_def_for = GSEngine.analytical_engine
            if op.op in self._interactive_engine_split_op:
                if split_dag_def.op:
                    self._split_dag_def_queue.put((split_dag_def_for, split_dag_def))
                split_dag_def = op_def_pb2.DagDef()
                split_dag_def_for = GSEngine.interactive_engine
            if op.op in self._learning_engine_split_op:
                if split_dag_def.op:
                    self._split_dag_def_queue.put((split_dag_def_for, split_dag_def))
                split_dag_def = op_def_pb2.DagDef()
                split_dag_def_for = GSEngine.learning_engine
            if op.op in self._coordinator_split_op:
                if split_dag_def.op:
                    self._split_dag_def_queue.put((split_dag_def_for, split_dag_def))
                split_dag_def = op_def_pb2.DagDef()
                split_dag_def_for = GSEngine.coordinator
            split_dag_def.op.extend([copy.deepcopy(op)])
        if len(split_dag_def.op) > 0:
            self._split_dag_def_queue.put((split_dag_def_for, split_dag_def))

    def empty(self):
        return self._split_dag_def_queue.empty()

    def get_next_dag(self):
        if not self._split_dag_def_queue.empty():
            return self._split_dag_def_queue.get()
        return None
