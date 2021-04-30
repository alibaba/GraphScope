import networkx.algorithms.tests.test_voronoi
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tests.test_voronoi,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.tests.test_voronoi import TestVoronoiCells


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestVoronoiCells)
class TestVoronoiCells:
    @pytest.mark.skip(reason="not support multigraph")
    def test_multigraph_unweighted(self):
        pass

    @pytest.mark.skip(reason="not support multigraph")
    def test_multidigraph_unweighted(self):
        pass

    @pytest.mark.skip(reason="not support multigraph")
    def test_multigraph_weighted(self):
        pass

    @pytest.mark.skip(reason="not support multigraph")
    def test_multidigraph_weighted(self):
        pass
