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

from gremlin_python.process import anonymous_traversal
from gremlin_python.process import graph_traversal
from gremlin_python.process.traversal import Bytecode


class GraphScopeGraphTraversalSource(graph_traversal.GraphTraversalSource):
    def __init__(self, graph, traversal_strategies, bytecode=None):
        super(GraphScopeGraphTraversalSource, self).__init__(
            graph, traversal_strategies, bytecode
        )
        self.graph_traversal = GraphScopeGraphTraversal


class GraphScopeGraphTraversal(graph_traversal.GraphTraversal):
    def __init__(self, graph, traversal_strategies, bytecode):
        super(GraphScopeGraphTraversal, self).__init__(
            graph, traversal_strategies, bytecode
        )

    def process(self, *args):
        self.bytecode.add_step("process", *args)
        return self

    def scatter(self, *args):
        self.bytecode.add_step("scatter", *args)
        return self

    def gather(self, *args):
        self.bytecode.add_step("gather", *args)
        return self

    def with_(self, *args):
        return super().with_(*args)

    def by(self, *args):
        return super().by(*args)


def expr(*args):
    byte_code = Bytecode()
    byte_code.add_step("expr", *args)
    return byte_code


def traversal(traversal_source_class=GraphScopeGraphTraversalSource):
    return anonymous_traversal.traversal(traversal_source_class)


class __(graph_traversal.__):
    graph_traversal = GraphScopeGraphTraversal


def V(*args):
    return __.V(*args)


def addE(*args):
    return __.addE(*args)


def addV(*args):
    return __.addV(*args)


def aggregate(*args):
    return __.aggregate(*args)


def and_(*args):
    return __.and_(*args)


def as_(*args):
    return __.as_(*args)


def barrier(*args):
    return __.barrier(*args)


def both(*args):
    return __.both(*args)


def bothE(*args):
    return __.bothE(*args)


def bothV(*args):
    return __.bothV(*args)


def branch(*args):
    return __.branch(*args)


def cap(*args):
    return __.cap(*args)


def choose(*args):
    return __.choose(*args)


def coalesce(*args):
    return __.coalesce(*args)


def coin(*args):
    return __.coin(*args)


def constant(*args):
    return __.constant(*args)


def count(*args):
    return __.count(*args)


def cyclicPath(*args):
    return __.cyclicPath(*args)


def dedup(*args):
    return __.dedup(*args)


def drop(*args):
    return __.drop(*args)


def elementMap(*args):
    return __.elementMap(*args)


def emit(*args):
    return __.emit(*args)


def filter_(*args):
    return __.filter_(*args)


def flatMap(*args):
    return __.flatMap(*args)


def fold(*args):
    return __.fold(*args)


def group(*args):
    return __.group(*args)


def groupCount(*args):
    return __.groupCount(*args)


def has(*args):
    return __.has(*args)


def hasId(*args):
    return __.hasId(*args)


def hasKey(*args):
    return __.hasKey(*args)


def hasLabel(*args):
    return __.hasLabel(*args)


def hasNot(*args):
    return __.hasNot(*args)


def hasValue(*args):
    return __.hasValue(*args)


def id_(*args):
    return __.id_(*args)


def identity(*args):
    return __.identity(*args)


def inE(*args):
    return __.inE(*args)


def inV(*args):
    return __.inV(*args)


def in_(*args):
    return __.in_(*args)


def index(*args):
    return __.index(*args)


def inject(*args):
    return __.inject(*args)


def is_(*args):
    return __.is_(*args)


def key(*args):
    return __.key(*args)


def label(*args):
    return __.label(*args)


def limit(*args):
    return __.limit(*args)


def local(*args):
    return __.local(*args)


def loops(*args):
    return __.loops(*args)


def map(*args):
    return __.map(*args)


def match(*args):
    return __.match(*args)


def math(*args):
    return __.math(*args)


def max_(*args):
    return __.max_(*args)


def mean(*args):
    return __.mean(*args)


def min_(*args):
    return __.min_(*args)


def not_(*args):
    return __.not_(*args)


def optional(*args):
    return __.optional(*args)


def or_(*args):
    return __.or_(*args)


def order(*args):
    return __.order(*args)


def otherV(*args):
    return __.otherV(*args)


def out(*args):
    return __.out(*args)


def outE(*args):
    return __.outE(*args)


def outV(*args):
    return __.outV(*args)


def path(*args):
    return __.path(*args)


def project(*args):
    return __.project(*args)


def properties(*args):
    return __.properties(*args)


def property(*args):
    return __.property(*args)


def propertyMap(*args):
    return __.propertyMap(*args)


def range_(*args):
    return __.range_(*args)


def repeat(*args):
    return __.repeat(*args)


def sack(*args):
    return __.sack(*args)


def sample(*args):
    return __.sample(*args)


def select(*args):
    return __.select(*args)


def sideEffect(*args):
    return __.sideEffect(*args)


def simplePath(*args):
    return __.simplePath(*args)


def skip(*args):
    return __.skip(*args)


def store(*args):
    return __.store(*args)


def subgraph(*args):
    return __.subgraph(*args)


def sum_(*args):
    return __.sum_(*args)


def tail(*args):
    return __.tail(*args)


def timeLimit(*args):
    return __.timeLimit(*args)


def times(*args):
    return __.times(*args)


def to(*args):
    return __.to(*args)


def toE(*args):
    return __.toE(*args)


def toV(*args):
    return __.toV(*args)


def tree(*args):
    return __.tree(*args)


def unfold(*args):
    return __.unfold(*args)


def union(*args):
    return __.union(*args)


def until(*args):
    return __.until(*args)


def value(*args):
    return __.value(*args)


def valueMap(*args):
    return __.valueMap(*args)


def values(*args):
    return __.values(*args)


def where(*args):
    return __.where(*args)


# Deprecated - prefer the underscore suffixed versions e.g filter_()


def filter(*args):
    return __.filter_(*args)


def id(*args):
    return __.id_(*args)


def max(*args):
    return __.max_(*args)


def min(*args):
    return __.min_(*args)


def range(*args):
    return __.range_(*args)


def sum(*args):
    return __.sum_(*args)


"""
from gremlin_python.process.traversal import Operator
from gremlin_python.process.traversal import Order
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

from graphscope.interactive.graph_traversal import traversal
from graphscope.interactive.graph_traversal import expr
from graphscope.interactive.graph_traversal import V
from graphscope.interactive.graph_traversal import out
from graphscope.interactive.graph_traversal import until
from graphscope.interactive.graph_traversal import count


g = traversal().withRemote(DriverRemoteConnection("xxx", "g"))
g.V().process(
    V().property('$pr', expr('1.0/TOTAL_V'))
        .repeat(
            V().property('$tmp', expr('$pr/OUT_DEGREE'))
            .scatter('$tmp').by(out())
            .gather('$tmp', Operator.sum)
            .property('$new', expr('0.15/TOTAL_V+0.85*$tmp'))
            .where(expr('abs($new-$pr)>1e-10'))
            .property('$pr', expr('$new')))
        .until(count().is_(0))
    ).with_('$pr', 'pr').order().by('pr', Order.desc).limit(10).elementMap('name', 'pr')
"""
