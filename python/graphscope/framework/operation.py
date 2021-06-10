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

""" Classes and functions used to construct dags.
"""

import hashlib
import uuid

from google.protobuf.json_format import MessageToJson

from graphscope.proto import op_def_pb2


class Operation(object):
    """Represents a dag op that performs computation on tensors.

    For example :code:`c = run_app(a, b)` creates an :code:`Operation` of type
    "RunApp" that takes operation :code:`a` and :code:`b` as input, and produces :code:`c`
    as output.

    After the dag has been launched in a session, an `Operation` can
    be executed by passing it to :code:`graphscope.Session.run`.
    """

    def __init__(
        self,
        session_id,
        op_type,
        inputs=None,
        output_types=None,
        config=None,
        query_args=None,
    ):
        """Creates an :code:`Operation`.

        Args:
            op_type: :code:`types_pb2.OperationType`
                Value for the "op" attribute of the OpDef proto.
            inputs:
                A list of `Operations` that will be the parents to self
            output_types:
                The operation's output type
            config:
                Dictionary where the key is the attribute name (a string)
                and the value is the respective "attr" attribute of the OpDef proto (an
                AttrValue).
            query_args:
                Values that used as query parameters when evaluating app.

        Raises:
            TypeError: value in inputs is not a :class:`Operation`
        """
        self._session_id = session_id
        self._op_def = op_def_pb2.OpDef(op=op_type, key=uuid.uuid4().hex)
        self._parents = list()
        if config:
            for k, v in config.items():
                self._op_def.attr[k].CopyFrom(v)
        if query_args is not None:
            self._op_def.query_args.CopyFrom(query_args)
        if inputs:
            for op in inputs:
                if not isinstance(op, Operation):
                    raise TypeError("Input op must be an Operation: {0}".format(op))
                self.add_parent(op)
        self._output_types = output_types
        self._evaluated = False
        self._leaf = False

    @property
    def key(self):
        """Unique key for each :code:`types_pb2.OpDef`"""
        return self._op_def.key

    @property
    def parents(self):
        return self._parents

    @property
    def evaluated(self):
        return self._evaluated

    @evaluated.setter
    def evaluated(self, value):
        self._evaluated = bool(value)

    @property
    def type(self):
        return self._op_def.op

    @property
    def output_types(self):
        return self._output_types

    @property
    def signature(self):
        """Signature of its parents' signatures and its own parameters.
        Used to unique identify one `Operation` with fixed configuration, if the configuration
        changed, the signature will be changed accordingly.
        """
        content = ""
        for op in self._parents:
            content += str(op.as_op_def)
        content += str(self.as_op_def())
        return hashlib.sha224(content.encode()).hexdigest()

    def is_leaf_op(self):
        return self._leaf

    def eval(self, leaf=True):
        # NB: to void cycle import
        # pylint: disable=import-outside-toplevel, cyclic-import
        from graphscope.client.session import get_session_by_id

        self._leaf = leaf
        sess = get_session_by_id(self._session_id)
        if not self._leaf:
            sess.dag.add_op(self)
        res = sess.run(self)
        return res

    def generate_new_key(self):
        self._op_def.key = uuid.uuid4().hex

    def add_parent(self, op):
        self._parents.append(op)
        self._op_def.parents.extend([op.key])

    def as_op_def(self):
        return self._op_def

    def __str__(self):
        return str(self.as_op_def())

    def __repr__(self):
        return "<grape.Operation '%s'(%s)>" % (self.type, self.key)

    def to_json(self):
        """Get json represented op."""
        return MessageToJson(self._op_def)
