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
import networkx.algorithms.bipartite.tests.test_generators
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.bipartite.tests.test_generators,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.bipartite.tests.test_generators import TestGeneratorsBipartite


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestGeneratorsBipartite)
class TestGeneratorsBipartite():
    def test_complete_bipartite_graph(self):
        G = complete_bipartite_graph(0, 0, create_using=nx.Graph)
        assert nx.is_isomorphic(G, nx.null_graph())

        for i in [1, 5]:
            G = complete_bipartite_graph(i, 0, create_using=nx.Graph)
            assert nx.is_isomorphic(G, nx.empty_graph(i))
            G = complete_bipartite_graph(0, i)
            assert nx.is_isomorphic(G, nx.empty_graph(i))

        G = complete_bipartite_graph(2, 2, create_using=nx.Graph)
        assert nx.is_isomorphic(G, nx.cycle_graph(4))

        G = complete_bipartite_graph(1, 5, create_using=nx.Graph)
        assert nx.is_isomorphic(G, nx.star_graph(5))

        G = complete_bipartite_graph(5, 1, create_using=nx.Graph)
        assert nx.is_isomorphic(G, nx.star_graph(5))

        # complete_bipartite_graph(m1,m2) is a connected graph with
        # m1+m2 nodes and  m1*m2 edges
        for m1, m2 in [(5, 11), (7, 3)]:
            G = complete_bipartite_graph(m1, m2, create_using=nx.Graph)
            assert nx.number_of_nodes(G) == m1 + m2
            assert nx.number_of_edges(G) == m1 * m2

        pytest.raises(nx.NetworkXError,
                      complete_bipartite_graph,
                      7,
                      3,
                      create_using=nx.DiGraph)
        pytest.raises(nx.NetworkXError,
                      complete_bipartite_graph,
                      7,
                      3,
                      create_using=nx.DiGraph)

        mG = complete_bipartite_graph(7, 3, create_using=nx.Graph)  # default to Graph
        assert sorted(mG.edges()) == sorted(G.edges())
        assert not mG.is_multigraph()
        assert not mG.is_directed()

        # specify nodes rather than number of nodes
        G = complete_bipartite_graph([1, 2], [10, 11], create_using=nx.Graph)
        has_edges = G.has_edge(1, 10) & G.has_edge(1, 11) &\
            G.has_edge(2, 10) & G.has_edge(2, 11)
        assert has_edges
        assert G.size() == 4

    def test_configuration_model(self):
        aseq = []
        bseq = []

        G = reverse_havel_hakimi_graph(aseq, bseq, create_using=nx.Graph)
        assert not G.is_multigraph()
        assert not G.is_directed()

        pytest.raises(nx.NetworkXError,
                      configuration_model,
                      aseq,
                      bseq,
                      create_using=nx.DiGraph())
        pytest.raises(nx.NetworkXError,
                      configuration_model,
                      aseq,
                      bseq,
                      create_using=nx.DiGraph)

    def test_havel_hakimi_graph(self):
        # default=nx.MultiGraph
        aseq = []
        bseq = []

        G = reverse_havel_hakimi_graph(aseq, bseq, create_using=nx.Graph)
        assert not G.is_multigraph()
        assert not G.is_directed()

        pytest.raises(nx.NetworkXError,
                      havel_hakimi_graph,
                      aseq,
                      bseq,
                      create_using=nx.DiGraph)
        pytest.raises(nx.NetworkXError,
                      havel_hakimi_graph,
                      aseq,
                      bseq,
                      create_using=nx.DiGraph)

    def test_reverse_havel_hakimi_graph(self):
        aseq = []
        bseq = []

        G = reverse_havel_hakimi_graph(aseq, bseq, create_using=nx.Graph)
        assert not G.is_multigraph()
        assert not G.is_directed()

        pytest.raises(nx.NetworkXError,
                      reverse_havel_hakimi_graph,
                      aseq,
                      bseq,
                      create_using=nx.DiGraph)
        pytest.raises(nx.NetworkXError,
                      reverse_havel_hakimi_graph,
                      aseq,
                      bseq,
                      create_using=nx.DiGraph)

    def test_alternating_havel_hakimi_graph(self):
        aseq = []
        bseq = []

        G = reverse_havel_hakimi_graph(aseq, bseq, create_using=nx.Graph)
        assert not G.is_multigraph()
        assert not G.is_directed()

        pytest.raises(nx.NetworkXError,
                      alternating_havel_hakimi_graph,
                      aseq,
                      bseq,
                      create_using=nx.DiGraph)
        pytest.raises(nx.NetworkXError,
                      alternating_havel_hakimi_graph,
                      aseq,
                      bseq,
                      create_using=nx.DiGraph)
