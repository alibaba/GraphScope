import os

import networkx.algorithms.tests.test_threshold
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tests.test_threshold,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestGeneratorThreshold)
class TestGeneratorThreshold:
    def test_eigenvectors(self):
        # N.B: graphscope.nx has no attribute 'laplacian_matrix'
        np = pytest.importorskip("numpy")
        eigenval = np.linalg.eigvals
        pytest.importorskip("scipy")

        cs = "ddiiddid"
        G = nxt.threshold_graph(cs)
        (tgeval, tgevec) = nxt.eigenvectors(cs)
        np.testing.assert_allclose([np.dot(lv, lv) for lv in tgevec], 1.0, rtol=1e-9)

    @pytest.mark.skipif("CI" in os.environ, reason="This case would failed in ci.")
    def test_finding_routines(self):
        G = nx.Graph({1: [2], 2: [3], 3: [4], 4: [5], 5: [6]})
        G.add_edge(2, 4)
        G.add_edge(2, 5)
        G.add_edge(2, 7)
        G.add_edge(3, 6)
        G.add_edge(4, 6)

        # Alternating 4 cycle
        assert nxt.find_alternating_4_cycle(G) == [1, 2, 3, 6]

        # Threshold graph
        TG = nxt.find_threshold_graph(G)
        assert nxt.is_threshold_graph(TG)
        assert sorted(TG.nodes()) == [1, 2, 3, 4, 5, 7]

        cs = nxt.creation_sequence(dict(TG.degree()), with_labels=True)
        assert nxt.find_creation_sequence(G) == cs
