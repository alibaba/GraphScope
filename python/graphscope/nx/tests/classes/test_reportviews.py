#
# This file is referred and derived from project NetworkX
#
# which has the following license:
#
# Copyright (C) 2004-2020, NetworkX Developers
# Aric Hagberg <hagberg@lanl.gov>
# Dan Schult <dschult@colgate.edu>
# Pieter Swart <swart@lanl.gov>
# All rights reserved.
#
# This file is part of NetworkX.
#
# NetworkX is distributed under a BSD license; see LICENSE.txt for more
# information.
#

# fmt: off
import networkx
import pytest
from networkx.classes.reportviews import NodeDataView
from networkx.classes.tests.test_reportviews import TestDegreeView as _TestDegreeView
from networkx.classes.tests.test_reportviews import \
    TestEdgeDataView as _TestEdgeDataView
from networkx.classes.tests.test_reportviews import TestEdgeView as _TestEdgeView
from networkx.classes.tests.test_reportviews import \
    TestInDegreeView as _TestInDegreeView
from networkx.classes.tests.test_reportviews import \
    TestInEdgeDataView as _TestInEdgeDataView
from networkx.classes.tests.test_reportviews import TestInEdgeView as _TestInEdgeView
from networkx.classes.tests.test_reportviews import \
    TestNodeDataView as _TestNodeDataView
from networkx.classes.tests.test_reportviews import TestNodeView as _TestNodeView
from networkx.classes.tests.test_reportviews import \
    TestNodeViewSetOps as _TestNodeViewSetOps
from networkx.classes.tests.test_reportviews import \
    TestOutDegreeView as _TestOutDegreeView
from networkx.classes.tests.test_reportviews import \
    TestOutEdgeDataView as _TestOutEdgeDataView
from networkx.classes.tests.test_reportviews import TestOutEdgeView as _TestOutEdgeView

from graphscope import nx

# fmt:on


# Nodes
@pytest.mark.usefixtures("graphscope_session")
class TestNodeView(_TestNodeView):
    def setup_class(cls):
        cls.G = nx.path_graph(9)
        cls.nv = cls.G.nodes  # NodeView(G)

    def test_pickle(self):
        pass


class TestNodeDataView(_TestNodeDataView):
    def setup_class(cls):
        cls.G = nx.path_graph(9)
        cls.nv = NodeDataView(cls.G)
        cls.ndv = cls.G.nodes.data(True)
        cls.nwv = cls.G.nodes.data("foo")

    def test_pickle(self):
        pass


class TestNodeViewSetOps(_TestNodeViewSetOps):
    @classmethod
    def setup_class(cls):
        cls.G = nx.path_graph(9)
        cls.G.nodes[3]["foo"] = "bar"
        cls.nv = cls.G.nodes


class TestNodeDataViewSetOps(TestNodeViewSetOps):
    @classmethod
    def setup_class(cls):
        cls.G = nx.path_graph(9)
        cls.G.nodes[3]["foo"] = "bar"
        cls.nv = cls.G.nodes.data("foo")
        print("nv", cls.nv)

    def n_its(self, nodes):
        return {(node, "bar" if node == 3 else None) for node in nodes}


class TestNodeDataViewDefaultSetOps(TestNodeDataViewSetOps):
    @classmethod
    def setup_class(cls):
        cls.G = nx.path_graph(9)
        cls.G.nodes[3]["foo"] = "bar"
        cls.nv = cls.G.nodes.data("foo", default=1)

    def n_its(self, nodes):
        return {(node, "bar" if node == 3 else 1) for node in nodes}


# Edges Data View
class TestEdgeDataView(_TestEdgeDataView):
    @classmethod
    def setup_class(cls):
        cls.G = nx.path_graph(9)
        cls.eview = networkx.reportviews.EdgeView

    def test_pickle(self):
        pass

    def test_str(self):
        pass

    def test_repr(self):
        pass


class TestOutEdgeDataView(_TestOutEdgeDataView):
    @classmethod
    def setup_class(cls):
        cls.G = nx.path_graph(9, create_using=nx.DiGraph())
        cls.eview = networkx.reportviews.OutEdgeView

    def test_pickle(self):
        pass

    def test_str(self):
        pass

    def test_repr(self):
        pass


class TestInEdgeDataView(_TestInEdgeDataView):
    @classmethod
    def setup_class(cls):
        cls.G = nx.path_graph(9, create_using=nx.DiGraph())
        cls.eview = networkx.reportviews.InEdgeView

    def test_pickle(self):
        pass

    def test_str(self):
        pass

    def test_repr(self):
        pass


class TestEdgeView(_TestEdgeView):
    @classmethod
    def setup_class(cls):
        cls.G = nx.path_graph(9)
        cls.eview = networkx.reportviews.EdgeView

    def test_pickle(self):
        pass


class TestOutEdgeView(_TestOutEdgeView):
    @classmethod
    def setup_class(cls):
        cls.G = nx.path_graph(9, nx.DiGraph())
        cls.eview = networkx.reportviews.OutEdgeView

    def test_pickle(self):
        pass


class TestInEdgeView(_TestInEdgeView):
    @classmethod
    def setup_class(cls):
        cls.G = nx.path_graph(9, nx.DiGraph())
        cls.eview = networkx.reportviews.InEdgeView

    def test_pickle(self):
        pass


class TestDegreeView(_TestDegreeView):
    GRAPH = nx.Graph
    dview = networkx.reportviews.DegreeView

    def test_pickle(self):
        print(type(self.G))
        pass


class TestDiDegreeView(TestDegreeView):
    GRAPH = nx.DiGraph
    dview = networkx.reportviews.DiDegreeView

    def test_repr(self):
        dv = self.G.degree()
        rep = "DiDegreeView({0: 1, 1: 3, 2: 2, 3: 3, 4: 2, 5: 1})"
        assert repr(dv) == rep


class TestOutDegreeView(_TestOutDegreeView):
    GRAPH = nx.DiGraph
    dview = networkx.reportviews.OutDegreeView

    def test_pickle(self):
        pass


class TestInDegreeView(_TestInDegreeView):
    GRAPH = nx.DiGraph
    dview = networkx.reportviews.InDegreeView

    def test_pickle(self):
        pass
