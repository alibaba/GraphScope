#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
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

""" Classes and functions used to manage dags.
"""


import queue

from graphscope.framework.operation import Operation
from graphscope.proto import op_def_pb2


class Dag(object):
    """Class represented as a GraphScope dataflow dag.

    A :class:`Dag` is always belongs to a session and containes a set of
    :class:`Operation` object, which performs computations on tensors.
    """

    def __init__(self):
        # the order in which op joins the dag, starting by 1.
        self._seq = 1
        #  mapping from op's key to op
        self._ops_by_key = dict()
        self._ops_seq_by_key = dict()

    def __str__(self):
        return str(self.as_dag_def())

    def __repr__(self):
        return self.__str__()

    def exists(self, op):
        if not isinstance(op, Operation):
            raise TypeError("op must be an Operation: {0}".format(op))
        return op.key in self._ops_by_key

    def add_op(self, op):
        if not isinstance(op, Operation):
            raise TypeError("op must be an Operation: {0}".format(op))
        if not op.evaluated and op.key in self._ops_by_key:
            raise ValueError("op named {0} already exist in dag".format(op.key))
        self._ops_by_key[op.key] = op
        self._ops_seq_by_key[op.key] = self._seq
        self._seq += 1

    def as_dag_def(self):
        """Return :class:`Dag` as a :class:`DagDef` proto buffer."""
        dag_def = op_def_pb2.DagDef()
        for _, op in self._ops_by_key.items():
            dag_def.op.extend([op.as_op_def()])
        return dag_def

    def to_json(self):
        return dict({k: op.to_json() for k, op in self._ops_by_key.items()})

    def extract_subdag_for(self, ops):
        """Extract all nodes included the path that can reach the target ops."""
        out = op_def_pb2.DagDef()
        # leaf op handle
        # there are two kinds of leaf op:
        #   1) unload graph / app
        #   2) networkx releated op
        if len(ops) == 1 and ops[0].is_leaf_op():
            out.op.extend([ops[0].as_op_def()])
            return out
        op_keys = list()
        # assert op is not present in current dag
        for op in ops:
            assert op.key in self._ops_by_key, "%s is not in the dag" % op.key
            assert not self._ops_by_key[op.key].evaluated, "%s is evaluated" % op.key
            op_keys.append(op.key)
        op_keys_to_keep = self._bfs_for_reachable_ops(op_keys)
        op_keys_to_keep = sorted(op_keys_to_keep, key=lambda n: self._ops_seq_by_key[n])
        for key in op_keys_to_keep:
            op_def = self._ops_by_key[key].as_op_def()
            out.op.extend([op_def])
        return out

    def clear(self):
        self._ops_by_key.clear()
        self._ops_seq_by_key.clear()
        self._seq = 1

    def _bfs_for_reachable_ops(self, op_keys):
        """Breadth first search for reachable ops from target ops.

        Why we need bfs:
            We need to build a dependency order of ops in a DAG
        Why we need record a sequence number:
            We need to ensure the dependency order is correct when:
                - an op is depended by multiple ops
                - an op occurs multiple times in target_keys
        """
        op_keys_to_keep = set()
        next_to_visit = queue.Queue()
        for key in op_keys:
            next_to_visit.put(key)
        while not next_to_visit.empty():
            next_op = next_to_visit.get()
            if next_op in op_keys_to_keep:
                continue
            op_keys_to_keep.add(next_op)
            for parent_op in self._ops_by_key[next_op].parents:
                if not parent_op.evaluated:
                    parent_key = parent_op.key
                    next_to_visit.put(parent_key)
        return list(op_keys_to_keep)


class DAGNode(object):
    """Base class to own :class:`Operation` information which as a node in a DAG."""

    def __init__(self):
        self._op = None
        self._session = None

    @property
    def op(self):
        if self._op is None:
            raise ValueError("None value of op in dag node.")
        if not isinstance(self._op, Operation):
            raise ValueError("Type of op in dag node must be Operation")
        return self._op

    @op.setter
    def op(self, value):
        self._op = value

    @property
    def evaluated(self):
        return self._op.evaluated

    @evaluated.setter
    def evaluated(self, value):
        self._op.evaluated = bool(value)

    @property
    def session(self):
        """Get the session that the dag node belongs to."""
        assert self._session is not None
        return self._session

    @session.setter
    def session(self, value):
        self._session = value

    @property
    def session_id(self):
        """Get the session id that the dag node belongs to."""
        assert self._session is not None
        return self._session.session_id
