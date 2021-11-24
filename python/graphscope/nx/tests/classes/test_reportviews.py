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
import os

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

    def test_str(self):
        assert str(self.nv) in (
            "[0, 1, 2, 3, 4, 5, 6, 7, 8]",
            "[0, 2, 4, 6, 8, 1, 3, 5, 7]",
        )

    def test_repr(self):
        assert repr(self.nv) in (
            "NodeView((0, 1, 2, 3, 4, 5, 6, 7, 8))",
            "NodeView((0, 2, 4, 6, 8, 1, 3, 5, 7))",
        )

    def test_iter(self):
        nv = self.nv
        nlist = list(self.G)
        for i, n in enumerate(nv):
            assert nlist[i] == n
        inv = iter(nv)
        assert next(inv) == 0
        assert iter(nv) != nv
        assert iter(inv) == inv
        inv2 = iter(nv)
        next(inv2)
        assert list(inv) == list(inv2)
        # odd case where NodeView calls NodeDataView with data=False
        nnv = nv(data=False)
        for i, n in enumerate(nnv):
            assert nlist[i] == n

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

    def test_str(self):
        assert str(self.ndv) in (
            "[(0, {}), (1, {}), (2, {}), (3, {}), (4, {}), (5, {}), (6, {}), (7, {}), (8, {})]",
            "[(0, {}), (2, {}), (4, {}), (6, {}), (8, {}), (1, {}), (3, {}), (5, {}), (7, {})]",
        )

    def test_repr(self):
        expected = (
            "NodeDataView((0, 1, 2, 3, 4, 5, 6, 7, 8))",
            "NodeDataView((0, 2, 4, 6, 8, 1, 3, 5, 7))",
        )
        assert repr(self.nv) in expected
        expected = (
            "NodeDataView({0: {}, 1: {}, 2: {}, 3: {}, "
            + "4: {}, 5: {}, 6: {}, 7: {}, 8: {}})",
            "NodeDataView({0: {}, 2: {}, 4: {}, 6: {}, "
            + "8: {}, 1: {}, 3: {}, 5: {}, 7: {}})",
        )
        assert repr(self.ndv) in expected
        expected = (
            "NodeDataView({0: None, 1: None, 2: None, 3: None, 4: None, "
            + "5: None, 6: None, 7: None, 8: None}, data='foo')",
            "NodeDataView({0: None, 2: None, 4: None, 6: None, 8: None, "
            + "1: None, 3: None, 5: None, 7: None}, data='foo')",
        )
        assert repr(self.nwv) in expected

    def test_iter(self):
        G = self.G.copy()
        nlist = list(G)
        nv = G.nodes.data()
        ndv = G.nodes.data(True)
        nwv = G.nodes.data("foo")
        for i, (n, d) in enumerate(nv):
            assert nlist[i] == n
            assert d == {}
        inv = iter(nv)
        assert next(inv) == (0, {})
        G.nodes[3]["foo"] = "bar"
        # default
        for n, d in nv:
            if n == 3:
                assert d == {"foo": "bar"}
            else:
                assert d == {}
        # data=True
        for n, d in ndv:
            if n == 3:
                assert d == {"foo": "bar"}
            else:
                assert d == {}
        # data='foo'
        for n, d in nwv:
            if n == 3:
                assert d == "bar"
            else:
                assert d is None
        # data='foo', default=1
        for n, d in G.nodes.data("foo", default=1):
            if n == 3:
                assert d == "bar"
            else:
                assert d == 1


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

    def test_iter(self):
        evr = self.eview(self.G)
        ev = evr()
        for u, v in ev:
            pass
        iev = iter(ev)
        assert next(iev) in ((0, 1), (1, 2))
        assert iter(ev) != ev
        assert iter(iev) == iev

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

    def test_str(self):
        ev = self.eview(self.G)
        rep = (
            str([(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7), (7, 8)]),
            str([(0, 1), (2, 3), (2, 1), (4, 5), (4, 3), (6, 7), (6, 5), (8, 7)]),
        )
        assert str(ev) in rep

    def test_repr(self):
        ev = self.eview(self.G)
        rep = (
            "EdgeView([(0, 1), (1, 2), (2, 3), (3, 4), "
            + "(4, 5), (5, 6), (6, 7), (7, 8)])",
            "EdgeView([(0, 1), (2, 3), (2, 1), (4, 5), "
            + "(4, 3), (6, 7), (6, 5), (8, 7)])",
        )
        assert repr(ev) in rep

    def test_or(self):
        # print("G | H edges:", gnv | hnv)
        ev = self.eview(self.G)
        some_edges = {(0, 1), (1, 0), (0, 2)}
        result1 = {(n, n - 1) for n in (2, 4, 6)}
        result1.update({(n, n + 1) for n in (2, 4, 6)})
        result1.update({(0, 1), (8, 7)})
        result1.update(some_edges)
        result2 = {(n, n + 1) for n in range(8)}
        result2.update(some_edges)
        assert (ev | some_edges) in (result1, result2)
        assert (some_edges | ev) in (result1, result2)

    def test_xor(self):
        # print("G ^ H edges:", gnv ^ hnv)
        ev = self.eview(self.G)
        some_edges = {(0, 1), (1, 0), (0, 2)}
        if self.G.is_directed():
            result = {(n, n + 1) for n in range(1, 8)}
            result.update({(1, 0), (0, 2)})
            assert ev ^ some_edges == result
        else:
            if os.environ.get("DEPLOYMENT", None) == "standalone":
                result = {(n, n + 1) for n in range(1, 8)}
                result.update({(0, 2)})
            else:  # num_workers=2
                result = {(n, n - 1) for n in (2, 4, 6)}
                result.update({(n, n + 1) for n in (2, 4, 6)})
                result.update({(0, 2), (8, 7)})
            assert ev ^ some_edges == result
        return

    def test_pickle(self):
        pass


class TestOutEdgeView(_TestOutEdgeView):
    @classmethod
    def setup_class(cls):
        cls.G = nx.path_graph(9, nx.DiGraph())
        cls.eview = networkx.reportviews.OutEdgeView

    def test_str(self):
        ev = self.eview(self.G)
        rep = (
            str([(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7), (7, 8)]),
            str([(0, 1), (2, 3), (4, 5), (6, 7), (1, 2), (3, 4), (5, 6), (7, 8)]),
        )
        assert str(ev) in rep

    def test_repr(self):
        ev = self.eview(self.G)
        rep = (
            "OutEdgeView([(0, 1), (1, 2), (2, 3), (3, 4), "
            + "(4, 5), (5, 6), (6, 7), (7, 8)])",
            "OutEdgeView([(0, 1), (2, 3), (4, 5), (6, 7), "
            + "(1, 2), (3, 4), (5, 6), (7, 8)])",
        )
        assert repr(ev) in rep

    def test_pickle(self):
        pass


class TestInEdgeView(_TestInEdgeView):
    @classmethod
    def setup_class(cls):
        cls.G = nx.path_graph(9, nx.DiGraph())
        cls.eview = networkx.reportviews.InEdgeView

    def test_str(self):
        ev = self.eview(self.G)
        rep = (
            str([(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7), (7, 8)]),
            str([(1, 2), (3, 4), (5, 6), (7, 8), (0, 1), (2, 3), (4, 5), (6, 7)]),
        )
        assert str(ev) in rep

    def test_repr(self):
        ev = self.eview(self.G)
        # NB: only pass on num_workers=2
        rep = (
            "InEdgeView([(1, 2), (3, 4), (5, 6), (7, 8), "
            + "(0, 1), (2, 3), (4, 5), (6, 7)])"
        )

    def test_iter(self):
        ev = self.eview(self.G)
        for u, v in ev:
            pass
        iev = iter(ev)
        assert next(iev) in ((0, 1), (1, 2))
        assert iter(ev) != ev
        assert iter(iev) == iev

    def test_pickle(self):
        pass


class TestDegreeView(_TestDegreeView):
    GRAPH = nx.Graph
    dview = networkx.reportviews.DegreeView

    def test_str(self):
        dv = self.dview(self.G)
        rep = (
            str([(0, 1), (1, 3), (2, 2), (3, 3), (4, 2), (5, 1)]),
            str([(0, 1), (2, 2), (4, 2), (1, 3), (3, 3), (5, 1)]),
        )
        assert str(dv) in rep
        dv = self.G.degree()
        assert str(dv) in rep

    def test_repr(self):
        dv = self.dview(self.G)
        rep = (
            "DegreeView({0: 1, 1: 3, 2: 2, 3: 3, 4: 2, 5: 1})",
            "DegreeView({0: 1, 2: 2, 4: 2, 1: 3, 3: 3, 5: 1})",
        )
        assert repr(dv) in rep

    def test_iter(self):
        dv = self.dview(self.G)
        nlist = list(self.G)
        for n, d in dv:
            pass
        idv = iter(dv)
        assert iter(dv) != dv
        assert iter(idv) == idv
        assert next(idv) == (nlist[0], dv[nlist[0]])
        assert next(idv) == (nlist[1], dv[nlist[1]])
        # weighted
        dv = self.dview(self.G, weight="foo")
        for n, d in dv:
            pass
        idv = iter(dv)
        assert iter(dv) != dv
        assert iter(idv) == idv
        assert next(idv) == (nlist[0], dv[nlist[0]])
        assert next(idv) == (nlist[1], dv[nlist[1]])

    def test_pickle(self):
        print(type(self.G))
        pass


class TestDiDegreeView(TestDegreeView):
    GRAPH = nx.DiGraph
    dview = networkx.reportviews.DiDegreeView

    def test_repr(self):
        dv = self.G.degree()
        rep = (
            "DiDegreeView({0: 1, 1: 3, 2: 2, 3: 3, 4: 2, 5: 1})",
            "DiDegreeView({0: 1, 2: 2, 4: 2, 1: 3, 3: 3, 5: 1})",
        )
        assert repr(dv) in rep


class TestOutDegreeView(_TestOutDegreeView):
    GRAPH = nx.DiGraph
    dview = networkx.reportviews.OutDegreeView

    def test_str(self):
        dv = self.dview(self.G)
        rep = (
            str([(0, 1), (1, 2), (2, 1), (3, 1), (4, 1), (5, 0)]),
            str([(0, 1), (2, 1), (4, 1), (1, 2), (3, 1), (5, 0)]),
        )
        assert str(dv) in rep
        dv = self.G.out_degree()
        assert str(dv) in rep

    def test_repr(self):
        # NB: only pass on num_workers=2
        dv = self.dview(self.G)
        rep = (
            "OutDegreeView({0: 1, 1: 2, 2: 1, 3: 1, 4: 1, 5: 0})",
            "OutDegreeView({0: 1, 2: 1, 4: 1, 1: 2, 3: 1, 5: 0})",
        )
        assert repr(dv) in rep

    def test_iter(self):
        dv = self.dview(self.G)
        nlist = list(self.G)
        for n, d in dv:
            pass
        idv = iter(dv)
        assert iter(dv) != dv
        assert iter(idv) == idv
        assert next(idv) == (nlist[0], dv[nlist[0]])
        assert next(idv) == (nlist[1], dv[nlist[1]])
        # weighted
        dv = self.dview(self.G, weight="foo")
        for n, d in dv:
            pass
        idv = iter(dv)
        assert iter(dv) != dv
        assert iter(idv) == idv
        assert next(idv) == (nlist[0], dv[nlist[0]])
        assert next(idv) == (nlist[1], dv[nlist[1]])

    def test_pickle(self):
        pass


class TestInDegreeView(_TestInDegreeView):
    GRAPH = nx.DiGraph
    dview = networkx.reportviews.InDegreeView

    def test_str(self):
        dv = self.dview(self.G)
        rep = (
            str([(0, 0), (1, 1), (2, 1), (3, 2), (4, 1), (5, 1)]),
            str([(0, 0), (2, 1), (4, 1), (1, 1), (3, 2), (5, 1)]),
        )
        assert str(dv) in rep
        dv = self.G.in_degree()
        assert str(dv) in rep

    def test_repr(self):
        dv = self.G.in_degree()
        rep = (
            "InDegreeView({0: 0, 1: 1, 2: 1, 3: 2, 4: 1, 5: 1})",
            "InDegreeView({0: 0, 2: 1, 4: 1, 1: 1, 3: 2, 5: 1})",
        )
        assert repr(dv) in rep

    def test_iter(self):
        dv = self.dview(self.G)
        nlist = list(self.G)
        for n, d in dv:
            pass
        idv = iter(dv)
        assert iter(dv) != dv
        assert iter(idv) == idv
        assert next(idv) == (nlist[0], dv[nlist[0]])
        assert next(idv) == (nlist[1], dv[nlist[1]])
        # weighted
        dv = self.dview(self.G, weight="foo")
        for n, d in dv:
            pass
        idv = iter(dv)
        assert iter(dv) != dv
        assert iter(idv) == idv
        assert next(idv) == (nlist[0], dv[nlist[0]])
        assert next(idv) == (nlist[1], dv[nlist[1]])

    def test_pickle(self):
        pass
