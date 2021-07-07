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
from gremlin_python.process.graph_traversal import GraphTraversal
from gremlin_python.process.traversal import Bytecode


def process(cls, *args):
    cls.bytecode.add_step("process", *args)
    return cls


def scatter(cls, *args):
    cls.bytecode.add_step("scatter", *args)
    return cls


def gather(cls, *args):
    cls.bytecode.add_step("gather", *args)
    return cls


setattr(GraphTraversal, "process", process)
setattr(GraphTraversal, "scatter", scatter)
setattr(GraphTraversal, "gather", gather)


def expr(*args):
    byte_code = Bytecode()
    byte_code.add_step("expr", *args)
    return byte_code


statics.add_static("expr", expr)

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
