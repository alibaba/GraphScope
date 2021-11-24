import networkx.algorithms.tests.test_threshold
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tests.test_threshold,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.tests.test_threshold import TestGeneratorThreshold


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestGeneratorThreshold)
class TestGeneratorThreshold:
    def test_eigenvectors(self):
        np = pytest.importorskip('numpy')
        eigenval = np.linalg.eigvals
        scipy = pytest.importorskip('scipy')

        cs = 'ddiiddid'
        G = nxt.threshold_graph(cs)
        (tgeval, tgevec) = nxt.eigenvectors(cs)
        dot = np.dot
        assert [abs(dot(lv, lv) - 1.0) < 1e-9 for lv in tgevec] == [True] * 8

    def test_create_using(self):
        cs = 'ddiiddid'
        G = nxt.threshold_graph(cs)
        assert pytest.raises(nx.exception.NetworkXError,
                             nxt.threshold_graph,
                             cs,
                             create_using=nx.DiGraph())
