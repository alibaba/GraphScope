import pytest

from graphscope import nx
from graphscope.nx.tests.utils import replace_with_inf


class TestRunGenericPath:
    def setup_method(self):
        self.edges = [(0, 1), (0, 2), (1, 2), (2, 3), (1, 4)]
        G = nx.Graph()
        G.add_edges_from(self.edges, weight=1)
        DG = nx.DiGraph()
        DG.add_edges_from(self.edges, weight=1)
        self.G = G
        self.DG = DG

    def teardown_method(self):
        del self.G
        del self.edges

    def test_run_shortest_path(self):
        nx.builtin.shortest_path(self.G, source=0, weight="weight")

    def test_run_shortest_path_length(self):
        nx.builtin.single_source_dijkstra_path_length(self.G, source=0, weight="weight")

    def test_run_average_shortest_path_length(self):
        nx.builtin.average_shortest_path_length(self.G, weight="weight")

    def test_run_has_path(self):
        nx.builtin.has_path(self.G, source=0, target=3)

    def test_shortest_path_length_on_reverse_view(self):
        ret1 = nx.builtin.single_source_dijkstra_path_length(
            self.DG, source=2, weight="weight"
        )
        assert replace_with_inf(dict(ret1.values)) == {
            0.0: float("inf"),
            1.0: float("inf"),
            2.0: 0.0,
            3.0: 1.0,
            4.0: float("inf"),
        }
        RDG = self.DG.reverse(copy=False)
        ret2 = nx.builtin.single_source_dijkstra_path_length(
            RDG, source=2, weight="weight"
        )
        assert replace_with_inf(dict(ret2.values)) == {
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
        assert dict(ret1.values) == {0.0: 1.0, 1.0: 1.0, 2.0: 0.0, 3.0: 1.0, 4.0: 2.0}
        DG = self.G.to_directed(as_view=True)
        ret2 = nx.builtin.single_source_dijkstra_path_length(
            DG, source=2, weight="weight"
        )
        assert dict(ret2.values) == {0.0: 1.0, 1.0: 1.0, 2.0: 0.0, 3.0: 1.0, 4.0: 2.0}
