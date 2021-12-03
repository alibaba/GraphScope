import networkx.algorithms.shortest_paths.tests.test_dense_numpy
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.shortest_paths.tests.test_dense_numpy,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.shortest_paths.tests.test_dense_numpy import TestFloydNumpy


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestFloydNumpy)
class TestFloydNumpy():
    def test_zero_weight(self):
        G = nx.DiGraph()
        edges = [(1, 2, -2), (2, 3, -4), (1, 5, 1), (5, 4, 0), (4, 3, -5), (2, 5, -7)]
        G.add_weighted_edges_from(edges)
        dist = nx.floyd_warshall_numpy(G)
        assert int(numpy.min(dist)) == -14
