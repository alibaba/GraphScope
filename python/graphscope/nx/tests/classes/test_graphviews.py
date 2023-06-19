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

import os

import networkx
import pytest
from networkx.classes.tests import test_graphviews as test_gvs

from graphscope import nx
from graphscope.nx.utils.misc import edges_equal

# Note: SubGraph views are not tested here. They have their own testing file


@pytest.mark.usefixtures("graphscope_session")
class TestReverseView(test_gvs.TestReverseView):
    def setup_method(self):
        self.G = nx.path_graph(9, create_using=nx.DiGraph())
        self.rv = self.G.reverse(copy=False)
        # self.rv = nx.reverse_view(self.G)

    # replace nx with graphscope.nx
    def test_exceptions(self):
        nxg = networkx.graphviews
        pytest.raises(nx.NetworkXNotImplemented, nxg.reverse_view, nx.Graph())

    # replace nx with graphscope.nx
    def test_subclass(self):
        class MyGraph(nx.DiGraph):
            def my_method(self):
                return "me"

            def to_directed_class(self):
                return MyGraph()

        M = MyGraph()
        M.add_edge(1, 2)
        RM = M.reverse(copy=False)
        # RM = nx.reverse_view(M)
        print("RM class", RM.__class__)
        # RMC = RM.copy()
        RMC = RM.copy(as_view=True)
        print("RMC class", RMC.__class__)
        print(RMC.edges)
        assert RMC.has_edge(2, 1)
        assert RMC.my_method() == "me"

    @pytest.mark.skip(reason="not support pickle remote graph.")
    def test_pickle(self):
        pass


@pytest.mark.usefixtures("graphscope_session")
class TestToDirected(test_gvs.TestToDirected):
    def setup_method(self):
        self.G = nx.path_graph(9)
        self.dv = nx.to_directed(self.G)

    def test_pickle(self):
        pass

    def test_already_directed(self):
        dd = nx.to_directed(self.dv)
        assert edges_equal(dd.edges, self.dv.edges)

    def test_iter(self):
        edges = list(self.G.edges)
        revd = [tuple(reversed(e)) for e in edges]
        expected = sorted(edges + revd)
        assert sorted(self.dv.edges) == expected


@pytest.mark.usefixtures("graphscope_session")
class TestToUndirected(test_gvs.TestToUndirected):
    def setup_method(self):
        self.DG = nx.path_graph(9, create_using=nx.DiGraph())
        self.uv = nx.to_undirected(self.DG)

    @pytest.mark.skip(reason="not support pickle remote graph.")
    def test_pickle(self):
        pass

    def test_already_directed(self):
        uu = nx.to_undirected(self.uv)
        assert edges_equal(uu.edges, self.uv.edges)

    def test_iter(self):
        assert edges_equal(self.uv.edges, self.DG.edges)


@pytest.mark.usefixtures("graphscope_session")
class TestChainsOfViews(test_gvs.TestChainsOfViews):
    @classmethod
    def setup_class(cls):
        cls.G = nx.path_graph(9)
        cls.DG = nx.path_graph(9, create_using=nx.DiGraph())
        cls.Gv = nx.to_undirected(cls.DG)
        cls.DGv = nx.to_directed(cls.G)
        cls.Rv = cls.DG.reverse()
        cls.graphs = [cls.G, cls.DG, cls.Gv, cls.DGv, cls.Rv]
        for G in cls.graphs:
            print(G.edges, G.nodes, G.degree)

    def test_pickle(self):
        pass

    # subgraph is deepcopy subgraph in graphscope.nx
    def test_subgraph_of_subgraph(self):
        SGv = nx.subgraph(self.G, range(3, 7))
        SDGv = nx.subgraph(self.DG, range(3, 7))
        for G in self.graphs + [SGv, SDGv]:
            SG = G.subgraph([4, 5, 6])
            assert sorted(list(SG)) == [4, 5, 6]
            SSG = SG.subgraph([6, 7])
            assert list(SSG) == [6]

    @pytest.mark.skip(reason="not restricted view in graphscope.nx")
    def test_restricted_induced_subgraph_chains(self):
        pass

    def test_subgraph_copy(self):
        for origG in self.graphs:
            SG = origG.subgraph([4, 5, 6])
            H = SG.copy()
            assert type(origG) == type(H)

    def test_subgraph_todirected(self):
        SG = self.G.subgraph([4, 5, 6])
        SSG = SG.to_directed()
        assert sorted(SSG) == [4, 5, 6]
        assert sorted(SSG.edges) == [(4, 5), (5, 4), (5, 6), (6, 5)]

    def test_subgraph_toundirected(self):
        SG = self.G.subgraph([4, 5, 6])
        SSG = SG.to_undirected()
        assert sorted(list(SSG)) == [4, 5, 6]
        edges = sorted(SSG.edges)
        assert len(edges) == 2
        assert edges[0] in ((4, 5), (5, 4))
        assert edges[1] in ((5, 6), (6, 5))

    def test_reverse_subgraph_toundirected(self):
        # a view can not project subgraph in graphscope.nx
        G = self.DG.reverse()
        SG = G.subgraph([4, 5, 6])
        SSG = SG.to_undirected()
        assert sorted(list(SSG)) == [4, 5, 6]
        edges = sorted(SSG.edges)
        assert len(edges) == 2
        assert edges[0] in ((4, 5), (5, 4))
        assert edges[1] in ((5, 6), (6, 5))

    def test_reverse_reverse_copy(self):
        G = self.DG.reverse(copy=False)
        H = G.reverse(copy=False)
        assert H.nodes == self.DG.nodes
        assert H.edges == self.DG.edges

    def test_subgraph_edgesubgraph_toundirected(self):
        G = self.G.copy()
        SG = G.subgraph([4, 5, 6])
        SSG = SG.edge_subgraph([(4, 5), (5, 4)])
        USSG = SSG.to_undirected()
        assert sorted(list(USSG)) == [4, 5]
        assert sorted(USSG.edges) == [(5, 4)] or sorted(USSG.edges) == [(4, 5)]

    @pytest.mark.skip(reason="not support multigraph.")
    def test_copy_multidisubgraph(self):
        pass

    @pytest.mark.skip(reason="not support multigraph.")
    def test_copy_multisubgraph(self):
        pass

    @pytest.mark.skip(reason="not support order-like graph")
    def test_copy_of_view(self):
        G = nx.OrderedMultiGraph(self.MGv)
        assert G.__class__.__name__ == "OrderedMultiGraph"
        G = G.copy(as_view=True)
        assert G.__class__.__name__ == "OrderedMultiGraph"
