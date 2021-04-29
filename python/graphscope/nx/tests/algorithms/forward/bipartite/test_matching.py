import networkx.algorithms.bipartite.tests.test_matching
import pytest

from graphscope.nx.utils.compat import import_as_graphscope_nx
from graphscope.nx.utils.compat import with_graphscope_nx_context

import_as_graphscope_nx(networkx.algorithms.bipartite.tests.test_matching,
                        decorators=pytest.mark.usefixtures("graphscope_session"))

from networkx.algorithms.bipartite.tests.test_matching import TestMatching


@pytest.mark.usefixtures("graphscope_session")
@with_graphscope_nx_context(TestMatching)
class TestMatching():
    @pytest.mark.skip(reason="not support tuple object as node")
    def test_eppstein_matching_disconnected(self):
        pass

    @pytest.mark.skip(reason="not support tuple object as node")
    def test_hopcroft_karp_matching_disconnected(self):
        pass

    @pytest.mark.skip(reason="not support class object as node")
    def test_unorderable_nodes(self):
        pass

    @pytest.mark.skip(reason="not support class object as node")
    def test_issue_2127(self):
        pass
