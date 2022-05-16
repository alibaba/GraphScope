import pytest
from networkx.testing import almost_equal

from graphscope import nx
from graphscope.nx.utils.misc import replace_with_inf


@pytest.mark.usefixtures("graphscope_session")
class TestRunGenericPath:
    def setup_class(cls):
        cls.edges = [(0, 1), (0, 2), (1, 2), (2, 3), (1, 4)]
        G = nx.Graph()
        G.add_edges_from(cls.edges, weight=1)
        DG = nx.DiGraph()
        DG.add_edges_from(cls.edges, weight=1)
        cls.G = G
        cls.DG = DG

    def test_run_shortest_path(self):
        nx.builtin.shortest_path(self.G, source=0, weight="weight")

    def test_run_shortest_path_length(self):
        nx.builtin.single_source_dijkstra_path_length(self.G, source=0, weight="weight")

    def test_run_average_shortest_path_length(self):
        nx.builtin.average_shortest_path_length(self.G, weight="weight")

    def test_run_has_path(self):
        assert nx.builtin.has_path(self.G, source=0, target=3)

    def test_shortest_path_length_on_reverse_view(self):
        ret1 = nx.builtin.single_source_dijkstra_path_length(
            self.DG, source=2, weight="weight"
        )
        assert replace_with_inf(ret1) == {
            0.0: float("inf"),
            1.0: float("inf"),
            2.0: 0.0,
            3.0: 1.0,
            4.0: float("inf"),
        }
        RDG = self.DG.reverse(copy=True)
        ret2 = nx.builtin.single_source_dijkstra_path_length(
            RDG, source=2, weight="weight"
        )
        assert replace_with_inf(ret2) == {
            0.0: 1.0,
            1.0: 1.0,
            2.0: 0.0,
            3.0: float("inf"),
            4.0: float("inf"),
        }

    def test_shortest_path_length_on_directed_view(self):
        ret1 = nx.builtin.single_source_dijkstra_path_length(
            self.G, source=2, weight="weight"
        )
        assert ret1 == {0.0: 1.0, 1.0: 1.0, 2.0: 0.0, 3.0: 1.0, 4.0: 2.0}
        DG = self.G.to_directed(as_view=True)
        ret2 = nx.builtin.single_source_dijkstra_path_length(
            DG, source=2, weight="weight"
        )
        assert ret2 == {0.0: 1.0, 1.0: 1.0, 2.0: 0.0, 3.0: 1.0, 4.0: 2.0}

    @pytest.mark.skip(reason="DynamicFragment duplicated mode not ready.")
    def test_all_pairs_shortest_path_length(self):
        cycle = nx.cycle_graph(7)
        pl = nx.builtin.all_pairs_shortest_path_length(cycle)
        assert pl[0] == {0: 0, 1: 1, 2: 2, 3: 3, 4: 3, 5: 2, 6: 1}
        assert pl[1] == {0: 1, 1: 0, 2: 1, 3: 2, 4: 3, 5: 3, 6: 2}

        for e in cycle.edges:
            cycle.edges[e]["weight"] = 1
        cycle[1][2]["weight"] = 10
        pl = nx.builtin.all_pairs_shortest_path_length(cycle, weight="weight")
        assert pl[0] == {0: 0, 1: 1, 2: 5, 3: 4, 4: 3, 5: 2, 6: 1}
        assert pl[1] == {0: 1, 1: 0, 2: 6, 3: 5, 4: 4, 5: 3, 6: 2}


@pytest.mark.usefixtures("graphscope_session")
class TestGenericPath:
    @classmethod
    def setup_class(cls):
        from networkx import convert_node_labels_to_integers as cnlti
        from networkx import grid_2d_graph

        grid = cnlti(grid_2d_graph(4, 4), first_label=1, ordering="sorted")
        cls.grid = nx.Graph(grid)
        cls.cycle = nx.cycle_graph(7)
        cls.directed_cycle = nx.cycle_graph(7, create_using=nx.DiGraph())
        cls.neg_weights = nx.DiGraph()
        cls.neg_weights.add_edge(0, 1, weight=1)
        cls.neg_weights.add_edge(0, 2, weight=3)
        cls.neg_weights.add_edge(1, 3, weight=1)
        cls.neg_weights.add_edge(2, 3, weight=-2)

    def test_has_path(self):
        G = nx.Graph()
        nx.add_path(G, range(3))
        nx.add_path(G, range(3, 5))
        assert nx.builtin.has_path(G, 0, 2)
        assert not nx.builtin.has_path(G, 0, 4)


@pytest.mark.usefixtures("graphscope_session")
class TestAverageShortestPathLength:
    def test_cycle_graph(self):
        ans = nx.average_shortest_path_length(nx.cycle_graph(7))
        assert almost_equal(ans, 2)

    def test_path_graph(self):
        ans = nx.average_shortest_path_length(nx.path_graph(5))
        assert almost_equal(ans, 2)

    def test_weighted(self):
        G = nx.Graph()
        nx.add_cycle(G, range(7), weight=2)
        ans = nx.average_shortest_path_length(G, weight="weight")
        assert almost_equal(ans, 4)
        G = nx.Graph()
        nx.add_path(G, range(5), weight=2)
        ans = nx.average_shortest_path_length(G, weight="weight")
        assert almost_equal(ans, 4)

    @pytest.mark.skip(reason="not support specify method.")
    def test_specified_methods(self):
        G = nx.Graph()
        nx.add_cycle(G, range(7), weight=2)
        ans = nx.average_shortest_path_length(G, weight="weight", method="dijkstra")
        assert almost_equal(ans, 4)
        ans = nx.average_shortest_path_length(G, weight="weight", method="bellman-ford")
        assert almost_equal(ans, 4)
        ans = nx.average_shortest_path_length(
            G, weight="weight", method="floyd-warshall"
        )
        assert almost_equal(ans, 4)

        G = nx.Graph()
        nx.add_path(G, range(5), weight=2)
        ans = nx.average_shortest_path_length(G, weight="weight", method="dijkstra")
        assert almost_equal(ans, 4)
        ans = nx.average_shortest_path_length(G, weight="weight", method="bellman-ford")
        assert almost_equal(ans, 4)
        ans = nx.average_shortest_path_length(
            G, weight="weight", method="floyd-warshall"
        )
        assert almost_equal(ans, 4)

    @pytest.mark.skip(
        reason="TODO(@weibin): raise disconnected error when result is inf."
    )
    def test_disconnected(self):
        g = nx.Graph()
        g.add_nodes_from(range(3))
        g.add_edge(0, 1)
        pytest.raises(nx.NetworkXError, nx.average_shortest_path_length, g)
        g = g.to_directed()
        pytest.raises(nx.NetworkXError, nx.average_shortest_path_length, g)

    def test_trivial_graph(self):
        """Tests that the trivial graph has average path length zero,
        since there is exactly one path of length zero in the trivial
        graph.

        For more information, see issue #1960.

        """
        G = nx.trivial_graph()
        assert nx.average_shortest_path_length(G) == 0

    def test_null_graph(self):
        with pytest.raises(nx.NetworkXPointlessConcept):
            nx.average_shortest_path_length(nx.null_graph())

    @pytest.mark.skip(reason="not support specify method.")
    def test_bad_method(self):
        with pytest.raises(ValueError):
            G = nx.path_graph(2)
            nx.average_shortest_path_length(G, weight="weight", method="SPAM")
