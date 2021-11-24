import networkx.algorithms.tests.test_similarity
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.tests.test_similarity,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.tests.test_similarity import TestSimilarity


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestSimilarity)
class TestSimilarity:
    @pytest.mark.skip(reason="not support multigraph")
    def test_multigraph(self):
        pass

    @pytest.mark.skip(reason="not support multigraph")
    def test_multidigraph(self):
        pass
