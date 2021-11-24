import networkx.algorithms.tests.test_boundary
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tests.test_boundary,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.tests.test_boundary import TestEdgeBoundary
from networkx.algorithms.tests.test_boundary import TestNodeBoundary


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestNodeBoundary)
class TestNodeBoundary:
    @pytest.mark.skip(reason="not support multigraph")
    def test_multigraph(self):
        pass

    @pytest.mark.skip(reason="not support multigraph")
    def test_multidigraph(self):
        pass


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestEdgeBoundary)
class TestEdgeBoundary:
    @pytest.mark.skip(reason="not support multigraph")
    def test_multigraph(self):
        pass

    @pytest.mark.skip(reason="not support multigraph")
    def test_multidigraph(self):
        pass
