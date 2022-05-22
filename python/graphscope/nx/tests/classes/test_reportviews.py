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

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
    def test_str(self):
        assert str(self.nv) == "[0, 1, 2, 3, 4, 5, 6, 7, 8]"

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
    def test_repr(self):
        assert repr(self.nv) == "NodeView((0, 1, 2, 3, 4, 5, 6, 7, 8))"

    def test_iter(self):
        nv = self.nv
        nlist = list(self.G)
        # the order of iteration is not the same every time
        assert sorted(nlist) == sorted(nv)
        # odd case where NodeView calls NodeDataView with data=False
        nnv = nv(data=False)
        assert sorted(nlist) == sorted(nnv)

    def test_pickle(self):
        pass


@pytest.mark.usefixtures("graphscope_session")
class TestNodeDataView(_TestNodeDataView):
    def setup_class(cls):
        cls.G = nx.path_graph(9)
        cls.nv = NodeDataView(cls.G)
        cls.ndv = cls.G.nodes.data(True)
        cls.nwv = cls.G.nodes.data("foo")

    def test_pickle(self):
        pass

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
    def test_str(self):
        msg = str([(n, {}) for n in range(9)])
        assert str(self.ndv) == msg

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
    def test_repr(self):
        expected = "NodeDataView((0, 1, 2, 3, 4, 5, 6, 7, 8))"
        assert repr(self.nv) == expected
        expected = (
            "NodeDataView({0: {}, 1: {}, 2: {}, 3: {}, "
            + "4: {}, 5: {}, 6: {}, 7: {}, 8: {}})"
        )
        assert repr(self.ndv) == expected
        expected = (
            "NodeDataView({0: None, 1: None, 2: None, 3: None, 4: None, "
            + "5: None, 6: None, 7: None, 8: None}, data='foo')"
        )
        assert repr(self.nwv) == expected

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
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


@pytest.mark.usefixtures("graphscope_session")
class TestNodeViewSetOps(_TestNodeViewSetOps):
    @classmethod
    def setup_class(cls):
        cls.G = nx.path_graph(9)
        cls.G.nodes[3]["foo"] = "bar"
        cls.nv = cls.G.nodes


@pytest.mark.usefixtures("graphscope_session")
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
@pytest.mark.usefixtures("graphscope_session")
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

    def test_iter(self):
        evr = self.eview(self.G)
        ev = evr()
        for u, v in ev:
            pass
        iev = iter(ev)
        # The node order of iteration is not start from 0 every time.
        # assert next(iev) == (0, 1)
        assert iter(ev) != ev
        assert iter(iev) == iev


@pytest.mark.usefixtures("graphscope_session")
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

    def test_iter(self):
        evr = self.eview(self.G)
        ev = evr()
        for u, v in ev:
            pass
        iev = iter(ev)
        # The node order of iteration is not start from 0 every time.
        # assert next(iev) == (0, 1)
        assert iter(ev) != ev
        assert iter(iev) == iev


@pytest.mark.usefixtures("graphscope_session")
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


@pytest.mark.usefixtures("graphscope_session")
class TestEdgeView(_TestEdgeView):
    @classmethod
    def setup_class(cls):
        cls.G = nx.path_graph(9)
        cls.eview = networkx.reportviews.EdgeView

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
    def test_str(self):
        ev = self.eview(self.G)
        rep = str([(n, n + 1) for n in range(8)])
        assert str(ev) in rep

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
    def test_repr(self):
        ev = self.eview(self.G)
        rep = (
            "EdgeView([(0, 1), (1, 2), (2, 3), (3, 4), "
            + "(4, 5), (5, 6), (6, 7), (7, 8)])"
        )
        assert repr(ev) in rep

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
    def test_or(self):
        # print("G | H edges:", gnv | hnv)
        ev = self.eview(self.G)
        some_edges = {(0, 1), (1, 0), (0, 2)}
        result1 = {(n, n + 1) for n in range(8)}
        result1.update(some_edges)
        result2 = {(n + 1, n) for n in range(8)}
        result2.update(some_edges)
        assert (ev | some_edges) in (result1, result2)
        assert (some_edges | ev) in (result1, result2)

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
    def test_xor(self):
        # print("G ^ H edges:", gnv ^ hnv)
        ev = self.eview(self.G)
        some_edges = {(0, 1), (1, 0), (0, 2)}
        if self.G.is_directed():
            result = {(n, n + 1) for n in range(1, 8)}
            result.update({(1, 0), (0, 2)})
            assert ev ^ some_edges == result
        else:
            result = {(n, n + 1) for n in range(1, 8)}
            result.update({(0, 2)})
            assert ev ^ some_edges == result
        return

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
    def test_iter(self):
        ev = self.eview(self.G)
        for u, v in ev:
            pass
        iev = iter(ev)
        assert next(iev) == (0, 1)
        assert iter(ev) != ev
        assert iter(iev) == iev

    def test_pickle(self):
        pass


@pytest.mark.usefixtures("graphscope_session")
class TestOutEdgeView(_TestOutEdgeView):
    @classmethod
    def setup_class(cls):
        cls.G = nx.path_graph(9, nx.DiGraph())
        cls.eview = networkx.reportviews.OutEdgeView

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
    def test_str(self):
        ev = self.eview(self.G)
        rep = str([(n, n + 1) for n in range(8)])
        assert str(ev) == rep

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
    def test_repr(self):
        ev = self.eview(self.G)
        rep = (
            "OutEdgeView([(0, 1), (1, 2), (2, 3), (3, 4), "
            + "(4, 5), (5, 6), (6, 7), (7, 8)])"
        )
        assert repr(ev) == rep

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
    def test_iter(self):
        ev = self.eview(self.G)
        for u, v in ev:
            pass
        iev = iter(ev)
        assert next(iev) == (0, 1)
        assert iter(ev) != ev
        assert iter(iev) == iev

    def test_pickle(self):
        pass


@pytest.mark.usefixtures("graphscope_session")
class TestInEdgeView(_TestInEdgeView):
    @classmethod
    def setup_class(cls):
        cls.G = nx.path_graph(9, nx.DiGraph())
        cls.eview = networkx.reportviews.InEdgeView

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
    def test_str(self):
        ev = self.eview(self.G)
        rep = str([(n, n + 1) for n in range(8)])
        assert str(ev) == rep

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
    def test_repr(self):
        ev = self.eview(self.G)
        rep = (
            "InEdgeView([(0, 1), (1, 2), (2, 3), (3, 4), "
            + "(4, 5), (5, 6), (6, 7), (7, 8)])"
        )
        assert repr(ev) == rep

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


@pytest.mark.usefixtures("graphscope_session")
class TestDegreeView(_TestDegreeView):
    GRAPH = nx.Graph
    dview = networkx.reportviews.DegreeView

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
    def test_str(self):
        dv = self.dview(self.G)
        rep = str([(0, 1), (1, 3), (2, 2), (3, 3), (4, 2), (5, 1)])
        assert str(dv) == rep
        dv = self.G.degree()
        assert str(dv) == rep

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
    def test_repr(self):
        dv = self.dview(self.G)
        rep = "DegreeView({0: 1, 1: 3, 2: 2, 3: 3, 4: 2, 5: 1})"
        assert repr(dv) == rep

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
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

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
    def test_repr(self):
        dv = self.G.degree()
        rep = "DiDegreeView({0: 1, 1: 3, 2: 2, 3: 3, 4: 2, 5: 1})"
        assert repr(dv) == rep


@pytest.mark.usefixtures("graphscope_session")
class TestOutDegreeView(_TestOutDegreeView):
    GRAPH = nx.DiGraph
    dview = networkx.reportviews.OutDegreeView

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
    def test_str(self):
        dv = self.dview(self.G)
        rep = str([(0, 1), (1, 2), (2, 1), (3, 1), (4, 1), (5, 0)])
        assert str(dv) == rep
        dv = self.G.out_degree()
        assert str(dv) == rep

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
    def test_repr(self):
        dv = self.G.out_degree()
        rep = "OutDegreeView({0: 1, 1: 2, 2: 1, 3: 1, 4: 1, 5: 0})"
        assert repr(dv) == rep

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
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


@pytest.mark.usefixtures("graphscope_session")
class TestInDegreeView(_TestInDegreeView):
    GRAPH = nx.DiGraph
    dview = networkx.reportviews.InDegreeView

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
    def test_str(self):
        dv = self.dview(self.G)
        rep = str([(0, 0), (1, 1), (2, 1), (3, 2), (4, 1), (5, 1)])
        assert str(dv) == rep
        dv = self.G.in_degree()
        assert str(dv) == rep

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
    def test_repr(self):
        dv = self.G.in_degree()
        rep = "InDegreeView({0: 0, 1: 1, 2: 1, 3: 2, 4: 1, 5: 1})"
        assert repr(dv) == rep

    @pytest.mark.skipif(
        os.environ.get("DEPLOYMENT", None) != "standalone",
        reason="Only need to test on standalone",
    )
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
