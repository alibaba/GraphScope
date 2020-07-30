import pytest

from graphscope.experimental import nx


class TestRunGenericPath:
    def setup_method(self):
        self.edges = [(0, 1), (0, 2), (1, 2), (2, 3), (1, 4)]
        G = nx.Graph()
        G.add_edges_from(self.edges, weight=2)
        self.G = G

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
