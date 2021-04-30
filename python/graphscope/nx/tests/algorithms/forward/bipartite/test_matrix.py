import networkx.algorithms.bipartite.tests.test_matrix
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.bipartite.tests.test_matrix,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.bipartite.tests.test_matrix import TestBiadjacencyMatrix


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestBiadjacencyMatrix)
class TestBiadjacencyMatrix():
    @pytest.mark.skip(reason="not support multigraph")
    def test_from_biadjacency_multigraph(self):
        pass
