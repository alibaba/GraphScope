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

from gremlin_python import statics
from gremlin_python.driver import request
from gremlin_python.driver.client import Client
from gremlin_python.driver.serializer import GraphSONMessageSerializer
from gremlin_python.process.graph_traversal import GraphTraversal
from gremlin_python.process.traversal import Bytecode
from gremlin_python.process.traversal import Traversal


def process(cls, *args):
    cls.bytecode.add_step("process", *args)
    return cls


def scatter(cls, *args):
    cls.bytecode.add_step("scatter", *args)
    return cls


def gather(cls, *args):
    cls.bytecode.add_step("gather", *args)
    return cls


def expr_(cls, *args):
    cls.bytecode.add_step("expr", *args)
    return cls


def withProperty(cls, *args):
    cls.bytecode.add_step("withProperty", *args)
    return cls


def sample(cls, *args):
    cls.bytecode.add_step("sample", *tuple(str(arg) for arg in args))
    return cls


def toTensorFlowDataset(cls, *args):
    cls.bytecode.add_step("toTensorFlowDataset", *args)
    return cls


def toPyTorchDataset(cls, *args):
    cls.bytecode.add_step("toPyTorchDataset", *args)
    return cls


setattr(GraphTraversal, "process", process)
setattr(GraphTraversal, "scatter", scatter)
setattr(GraphTraversal, "gather", gather)
setattr(GraphTraversal, "expr", expr_)
setattr(GraphTraversal, "withProperty", withProperty)
setattr(GraphTraversal, "sample", sample)
setattr(GraphTraversal, "toTensorFlowDataset", toTensorFlowDataset)
setattr(GraphTraversal, "toPyTorchDataset", toPyTorchDataset)


def expr(*args):
    byte_code = Bytecode()
    byte_code.add_step("expr", *args)
    return byte_code


statics.add_static("expr", expr)


def patch_for_gremlin_python():  # noqa: C901
    def toList(self):
        import pickle

        import graphscope
        from graphscope.client.session import __graphscope_interactive_query__

        interactive = __graphscope_interactive_query__[0]
        bytecode_pickle = pickle.dumps(self.bytecode)
        rlt = interactive.execute(
            bytecode_pickle, request_options={"engine": "gae_traversal"}
        ).all()
        try:
            return list(rlt)
        except Exception:
            return [rlt]

    setattr(Traversal, "toList", toList)

    def toSet(self):
        import pickle

        import graphscope
        from graphscope.client.session import __graphscope_interactive_query__

        interactive = __graphscope_interactive_query__[0]
        bytecode_pickle = pickle.dumps(self.bytecode)
        rlt = interactive.execute(
            bytecode_pickle, request_options={"engine": "gae_traversal"}
        ).all()
        try:
            return set(rlt)
        except Exception:
            return set([rlt])

    setattr(Traversal, "toSet", toSet)

    def get_processor(self, processor):
        if processor == "gae":
            return getattr(self, "standard", None)
        elif processor == "gae_traversal":
            return getattr(self, "traversal", None)
        processor = getattr(self, processor, None)
        if not processor:
            raise Exception("Unknown processor")
        return processor

    setattr(GraphSONMessageSerializer, "get_processor", get_processor)

    def submitAsync(self, message, bindings=None, request_options=None):
        has_gae_step = False
        if isinstance(message, Bytecode):
            for step in message.step_instructions:
                if step[0] == "process":
                    has_gae_step = True
                    break
            message = request.RequestMessage(
                processor="traversal",
                op="bytecode",
                args={"gremlin": message, "aliases": {"g": self._traversal_source}},
            )
        elif isinstance(message, str):
            message = request.RequestMessage(
                processor="",
                op="eval",
                args={"gremlin": message, "aliases": {"g": self._traversal_source}},
            )
            if bindings:
                message.args.update({"bindings": bindings})
            if self._sessionEnabled:
                message = message._replace(processor="session")
                message.args.update({"session": self._session})
        # Determind if is a GAE style Bytecode
        if has_gae_step:
            message = message._replace(processor="gae_traversal")
        conn = self._pool.get(True)
        if request_options:
            message.args.update(request_options)
        return conn.write(message)

    setattr(Client, "submitAsync", submitAsync)


patch_for_gremlin_python()


"""
import graphscope
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python import statics

statics.load_statics(globals())

g = traversal().withRemote(DriverRemoteConnection("xxx", "g"))
g.V().process(
    V().property('$pr', expr('1.0/TOTAL_V'))
        .repeat(
            V().property('$tmp', expr('$pr/OUT_DEGREE'))
            .scatter('$tmp').by(out())
            .gather('$tmp', sum)
            .property('$new', expr('0.15/TOTAL_V+0.85*$tmp'))
            .where(expr('abs($new-$pr)>1e-10'))
            .property('$pr', expr('$new')))
        .until(count().is_(0))
    ).with_('$pr', 'pr').order().by('pr', desc).limit(10).elementMap('name', 'pr')
"""
