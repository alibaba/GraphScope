import networkx.algorithms.centrality.tests.test_eigenvector_centrality
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(
    networkx.algorithms.centrality.tests.test_eigenvector_centrality,
    decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.centrality.tests.test_eigenvector_centrality import \
    TestEigenvectorCentralityExceptions


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestEigenvectorCentrality)
class TestEigenvectorCentralityExceptions():
    @pytest.mark.skip(reason="not support multigraph")
    def test_multigraph(self):
        pass

    @pytest.mark.skip(reason="not support multigraph")
    def test_multigraph_numpy(self):
        pass
