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
            TypeError: if `op_def` is not a `OpDef`, or if `g` is not a `Dag`.
            ValueError: if the `op_def` name is not valid.
        """
        self._session_id = session_id
        self._op_def = op_def_pb2.OpDef(op=op_type, key=uuid.uuid4().hex)
        if config:
            for k, v in config.items():
                self._op_def.attr[k].CopyFrom(v)

        if query_args is not None:
            self._op_def.query_args.CopyFrom(query_args)

        self._output_types = output_types

        self._signature = None
        self._output = None  # hold the executed result of the DAG.

    def as_op_def(self):
        return self._op_def

    @property
    def key(self):
        return self._op_def.key

    @property
    def signature(self):
        """Signature of its parents' signatures and its own parameters.
        Used to unique identify one `Operation` with fixed configuration, if the configuration
        changed, the signature will be changed accordingly.
        """
        content = str(self.as_op_def())
        return hashlib.sha224(content.encode()).hexdigest()

    def eval(self):
        # NB: to void cycle import
        # pylint: disable=import-outside-toplevel, cyclic-import
        from graphscope.client.session import get_session_by_id

        sess = get_session_by_id(self._session_id)
        res = sess.run(self)
        return res

    def set_output(self, output):
        """Set Operation's output value.
        Args:
            output: The output after evaluated the op

        Raises:
            RuntimeError: If the output is already be set before, since one op can only be evaluated once.
        """
        if self._output is not None:
            raise RuntimeError("The executed value of a DAG node is already set")
        self._output = output

    @property
    def evaluated(self):
        return self._output is not None

    @property
    def output(self):
        """Executed result of the DAG node.
        Returns None if the DAG node hasn't been evaluated yet, otherwise a `dict` from `JSON` object.
        """
        return self._output

    @property
    def type(self):
        return self._op_def.op

    @property
    def output_types(self):
        return self._output_types

    def __str__(self):
        return str(self.as_op_def())

    def __repr__(self):
        return "<grape.Operation '%s'(%s)>" % (self.type, self.key)

    def to_json(self):
        """Get json represented op."""
        return MessageToJson(self._op_def)
