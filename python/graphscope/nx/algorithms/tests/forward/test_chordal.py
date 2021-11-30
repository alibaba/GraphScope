import pytest
from networkx.algorithms.tests.test_chordal import TestMCS

import graphscope.nx as nx
from graphscope.nx.utils.compat import with_graphscope_nx_context


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestMCS)
class TestMCS:
    # NB: graphscope.nx does not support grid_graph, remove grid_graph
    def test_complete_to_chordal_graph(self):
        fgrg = nx.fast_gnp_random_graph
        test_graphs = [
            nx.barbell_graph(6, 2),
            nx.cycle_graph(15),
            nx.wheel_graph(20),
            nx.ladder_graph(15),
            nx.star_graph(5),
            nx.bull_graph(),
            fgrg(20, 0.3, seed=1),
        ]
        for G in test_graphs:
            H, a = nx.complete_to_chordal_graph(G)
            assert nx.is_chordal(H)
            assert len(a) == H.number_of_nodes()
            if nx.is_chordal(G):
                assert G.number_of_edges() == H.number_of_edges()
                assert set(a.values()) == {0}
            else:
                assert len(set(a.values())) == H.number_of_nodes()
