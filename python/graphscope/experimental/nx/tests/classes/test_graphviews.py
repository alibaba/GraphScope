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
import networkx
import pytest
from networkx.classes.tests import test_graphviews as test_gvs

from graphscope.experimental import nx
from graphscope.experimental.nx.tests.utils import assert_edges_equal
from graphscope.experimental.nx.tests.utils import assert_nodes_equal

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


@pytest.mark.usefixtures("graphscope_session")
class TestToDirected(test_gvs.TestToDirected):
    def setup_method(self):
        self.G = nx.path_graph(9)
        self.dv = nx.to_directed(self.G)

    def test_pickle(self):
        pass

    def test_already_directed(self):
        dd = nx.to_directed(self.dv)
        assert_edges_equal(dd.edges, self.dv.edges)


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
        assert_edges_equal(uu.edges, self.uv.edges)


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
            G.edges, G.nodes, G.degree

    def test_pickle(self):
        pass

    def test_subgraph_of_subgraph(self):
        SGv = nx.subgraph(self.G, range(3, 7))
        SDGv = nx.subgraph(self.DG, range(3, 7))
        for G in self.graphs + [SGv, SDGv]:
            SG = nx.induced_subgraph(G, [4, 5, 6])
            assert list(SG) == [4, 5, 6]
            SSG = SG.subgraph([6, 7])
            assert list(SSG) == [6]
            # subgraph-subgraph chain is short-cut in base class method
            assert SSG._graph is G

    def test_restricted_induced_subgraph_chains(self):
        """Test subgraph chains that both restrict and show nodes/edges.

        A restricted_view subgraph should allow induced subgraphs using
        G.subgraph that automagically without a chain (meaning the result
        is a subgraph view of the original graph not a subgraph-of-subgraph.
        """
        hide_nodes = [3, 4, 5]
        hide_edges = [(6, 7)]
        RG = nx.restricted_view(self.G, hide_nodes, hide_edges)
        nodes = [4, 5, 6, 7, 8]
        SG = nx.induced_subgraph(RG, nodes)
        SSG = RG.subgraph(nodes)
        assert RG._graph is self.G
        assert SSG._graph is self.G
        assert SG._graph is RG
        assert_edges_equal(SG.edges, SSG.edges)
        # should be same as morphing the graph
        CG = self.G.copy()
        CG.remove_nodes_from(hide_nodes)
        CG.remove_edges_from(hide_edges)
        assert_edges_equal(CG.edges(nodes), SSG.edges)
        CG.remove_nodes_from([0, 1, 2, 3])
        assert_edges_equal(CG.edges, SSG.edges)
        # switch order: subgraph first, then restricted view
        SSSG = self.G.subgraph(nodes)
        RSG = nx.restricted_view(SSSG, hide_nodes, hide_edges)
        assert RSG._graph is not self.G
        assert_edges_equal(RSG.edges, CG.edges)

    @pytest.mark.skip(reason="not support order-like graph")
    def test_subgraph_copy(self):
        for origG in self.graphs:
            G = nx.OrderedGraph(origG)
            SG = G.subgraph([4, 5, 6])
            H = SG.copy()
            assert type(G) == type(H)

    @pytest.mark.skip(reason="not support yet")
    def test_subgraph_toundirected(self):
        SG = nx.induced_subgraph(self.G, [4, 5, 6])
        # FIXME: not match like networkx.
        SSG = SG.to_undirected(as_view=True)
        assert list(SSG) == [4, 5, 6]
        assert sorted(SSG.edges) == [(4, 5), (5, 6)]

    def test_reverse_subgraph_toundirected(self):
        G = self.DG.reverse(copy=False)
        SG = G.subgraph([4, 5, 6])
        # FIXME: not match like networkx.
        SSG = SG.to_undirected(as_view=True)
        assert list(SSG) == [4, 5, 6]
        assert sorted(SSG.edges) == [(4, 5), (5, 6)]

    def test_reverse_reverse_copy(self):
        G = self.DG.reverse(copy=False)
        # FIXME: not match networkx
        # H = G.reverse(copy=True)
        H = G.reverse(copy=False)
        assert H.nodes == self.DG.nodes
        assert H.edges == self.DG.edges

    def test_subgraph_edgesubgraph_toundirected(self):
        G = self.G.copy()
        SG = G.subgraph([4, 5, 6])
        SSG = SG.edge_subgraph([(4, 5), (5, 4)])
        USSG = SSG.to_undirected(as_view=True)
        assert list(USSG) == [4, 5]
        assert sorted(USSG.edges) == [(4, 5)]

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

    @pytest.mark.skip(reason="subgraph now is fallback with networkx, not view")
    def test_subgraph_of_subgraph(self):
        pass

    @pytest.mark.skip(reason="subgraph now is fallback with networkx, not view")
    def test_restricted_induced_subgraph_chains(self):
        pass

    @pytest.mark.skip(reason="subgraph now is view, but to_directted is deepcopy")
    def test_subgraph_todirected(self):
        pass
