import pytest

from graphscope import nx

"""
    Unit tests for voterank.
"""


@pytest.mark.usefixtures("graphscope_session")
class TestVoteRank(object):
    def test_direct_graph(self):
        G = nx.DiGraph()
        edges = [(1, 3), (1, 4), (2, 4), (2, 5), (3, 5), (1, 2)]
        G.add_edges_from(edges)
        p = nx.builtin.voterank(G, 3)
        assert p == [1, 2, 3]
        p = nx.builtin.voterank(G, 2)
        assert p == [1, 2]
        p = nx.builtin.voterank(G, 4)
        assert p == [1, 2, 3]
        p = nx.builtin.voterank(G)
        assert p == [1, 2, 3]

        G = nx.DiGraph()
        G.add_edge(1, 2, weight=1)
        G.add_edge(1, 3, weight=1)
        G.add_edge(3, 1, weight=1)
        p = nx.builtin.voterank(G, 4)
        assert p == [1]

        G = nx.DiGraph()
        edges = [(21, 1), (89, 1), (91, 1), (92, 1), (20, 1)]
        G.add_edges_from(edges)
        p = nx.builtin.voterank(G, 1)
        assert p == [21]

    def test_undirect_graph(self):
        G = nx.Graph()
        edges = [(1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7)]
        G.add_edges_from(edges)
        p = nx.builtin.voterank(G, 4)
        assert p == [2, 5, 6, 3]
        edges = [
            (1, 2),
            (1, 3),
            (1, 4),
            (1, 5),
            (1, 6),
            (1, 7),
            (1, 8),
            (1, 9),
        ]
        G = nx.Graph()
        G.add_edges_from(edges)
        p = nx.builtin.voterank(G)
        assert p == [1]
        G.add_edge(8, 9)
        p = nx.builtin.voterank(G)
        assert p == [1, 8]

    def test_single_node(self):
        G = nx.trivial_graph()
        p = nx.builtin.voterank(G)
        assert p == []

    def test_complete_graph(self):
        G = nx.complete_graph(4)
        p = nx.builtin.voterank(G)
        assert p == [0, 1, 2]
        G = nx.complete_graph(5)
        p = nx.builtin.voterank(G)
        assert p == [0, 1, 2, 3]
