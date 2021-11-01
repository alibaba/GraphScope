import pytest

from graphscope import nx
from graphscope.nx.tests.utils import replace_with_inf


@pytest.mark.usefixtures("graphscope_session")
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
        RDG = self.DG.reverse(copy=False)
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
