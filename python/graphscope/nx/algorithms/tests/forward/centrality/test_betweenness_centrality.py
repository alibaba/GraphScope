import networkx.algorithms.centrality.tests.test_betweenness_centrality
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(
    networkx.algorithms.centrality.tests.test_betweenness_centrality,
    decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.centrality.tests.test_betweenness_centrality import \
    TestBetweennessCentrality


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestBetweennessCentrality)
class TestBetweennessCentrality:
    @pytest.mark.skip(reason="not support sampling")
    def test_sample_from_P3(self):
        G = nx.path_graph(3)
        b_answer = {0: 0.0, 1: 1.0, 2: 0.0}
        b = nx.betweenness_centrality(G, k=3, weight=None, normalized=False, seed=1)
        for n in sorted(G):
            assert b[n] == pytest.approx(b_answer[n], abs=1e-7)
        b = nx.betweenness_centrality(G, k=2, weight=None, normalized=False, seed=1)
        # python versions give different results with same seed
        b_approx1 = {0: 0.0, 1: 1.5, 2: 0.0}
        b_approx2 = {0: 0.0, 1: 0.75, 2: 0.0}
        for n in sorted(G):
            assert b[n] in (b_approx1[n], b_approx2[n])
